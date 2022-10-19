from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.discourse.discourse_spider import DiscourseSummarySpider, DiscourseTopicSpider, generateCrawlSummary
from scrapy.crawler import CrawlerProcess
import os
import pathlib

import cProfile

class DiscourseScraper(Scraper):
    profiler_enabled = False

    def scrape(self) -> RawDataset:
        if self.profiler_enabled:
            profiler = cProfile.Profile()
            profiler.enable()

        pathlib.Path(self.target_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(self.target_dir)
        crawlsettings = {
            "SCHEDULER_PRIORITY_QUEUE": 'scrapy.pqueues.DownloaderAwarePriorityQueue',
            "CONCURRENT_REQUESTS": 1000,
            "LOG_LEVEL": "WARN",
            "DOWNLOAD_DELAY": .6,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": False,
            "AUTOTHROTTLE_DEBUG": True,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": .5,
            "REACTOR_THREADPOOL_MAXSIZE": 100,
            #"JOBDIR": "scrapy-job",
        }

        # TODO - crawl type should be an argument we can pass in
        crawltype = 'topics'

        if crawltype == 'stats':
            # use DiscourseSummarySpider to generate crawl summary by grabbing the index from every site
            crawlsettings['RETRY_ENABLED'] = False
            process = CrawlerProcess(crawlsettings)
            process.crawl(DiscourseSummarySpider)
            process.start()

            # Generate a summary of all the sites that were crawled
            generateCrawlSummary()
        elif crawltype == 'topics':
            # use DiscourseTopicSpider to perform a full crawl
            process = CrawlerProcess(crawlsettings)
            process.crawl(DiscourseTopicSpider)
            process.start()
        elif crawltype == 'summary':
            # Generate a summary of all the sites that were crawled
            generateCrawlSummary()

        if self.profiler_enabled:
            profiler.disable()
            print("write profile to profile.prof")
            #with open('profile.txt', 'w') as fd:
            #    pstat_profile = pstats.Stats(profiler, stream=fd)
            #    pstat_profile.print_stats()
            profiler.dump_stats("profile.prof")

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata='')


class DiscourseCodeProcessor(Processor):
    def process(self, raw_data: RawDataset, *args, **kwargs):
        # TODO - transform raw JSON data into whatever format we need for training
        return

class DiscourseDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = DiscourseScraper(tempdir, target_dir)
        #self.processor = DiscourseCodeProcessor()
    def download(self):
        self.scraper.download()

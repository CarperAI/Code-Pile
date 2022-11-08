from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.discourse.discourse_spider import DiscourseSummarySpider, DiscourseTopicSpider, generateCrawlSummary, verify_site
from codepile.discourse.discourse_processor import DiscourseProcessor
from scrapy.crawler import CrawlerProcess
import os, sys
import pathlib

import cProfile

# CLI usage example at bottom

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
        crawltype = 'summary'

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
        elif crawltype == 'verify':
            # Generate a summary of all the sites that were crawled
            verify_site('forum.arduino.cc')

        if self.profiler_enabled:
            profiler.disable()
            print("write profile to profile.prof")
            #with open('profile.txt', 'w') as fd:
            #    pstat_profile = pstats.Stats(profiler, stream=fd)
            #    pstat_profile.print_stats()
            profiler.dump_stats("profile.prof")

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata='')


class DiscourseDataset(Dataset):
    def __init__(self, site, data_dir, temp_dir):
        self.scraper = DiscourseScraper(temp_dir, data_dir)
        self.processor = DiscourseProcessor(site, data_dir, temp_dir)
    def download(self):
        self.scraper.download()
    def compress(self):
        # TODO - bundle up each site from the crawl as a .tar.gz file
        pass

if __name__=="__main__":
    if not os.path.exists("data/"):
            os.makedirs("data/")
    # TODO - site should be an optional parameter, so you can choose whether to work on individual sites or the whole dataset
    if len(sys.argv) > 4:
        action = sys.argv[1]
        site = sys.argv[2]
        datadir = sys.argv[3]
        tmpdir = sys.argv[4]

        discourse_dataset = DiscourseDataset(site, datadir, tmpdir)
        if action == 'download':
            print(discourse_dataset.download())
        elif action == 'compress':
            print(discourse_dataset.compress())
        elif action == 'process':
            print(discourse_dataset.process())
        elif action == 'analyze':
            print(discourse_dataset.processor.analyze())
    else:
        print('Usage: %s <action> <site> <datadir> <tmpdir>' % (sys.argv[0]))


# Example usage:
#
# - Start new crawl:
#        python3 -m codepile.discourse.discourse download all ~/data/my-discourse-crawl /tmp
#
# - Compress each site into its own <site>.tar.gz file
#        python3 -m codepile.discourse.discourse compress <site> ~/data/my-discourse-crawl /tmp
#
# - Process collected site data into lm_dataset format:
#        python3 -m codepile.discourse.discourse process <site> ~/data/my-discourse-crawl /tmp
#
# - Collect stats about the sites included in this crawl:
#        python3 -m codepile.discourse.discourse analyze <site> ~/data/my-discourse-crawl /tmp

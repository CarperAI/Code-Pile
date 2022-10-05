from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.discourse.discourse_spider import DiscourseSummarySpider, DiscourseTopicSpider, generateCrawlSummary
from scrapy.crawler import CrawlerProcess
import os

class DiscourseScraper(Scraper):
    def scrape(self) -> RawDataset:
        os.chdir(self.target_dir)
        process = CrawlerProcess(settings={
            "SCHEDULER_PRIORITY_QUEUE": 'scrapy.pqueues.DownloaderAwarePriorityQueue',
            "CONCURRENT_REQUESTS": 1000,
            "LOG_LEVEL": "WARNING",
            "RETRY_ENABLED": False # TODO - this should depend on crawl type
        })

        # TODO - crawl type should be an argument we can pass in
        crawltype = 'topics'

        if crawltype == 'summary':
            # use DiscourseSummarySpider to generate crawl summary by grabbing the index from every site
            process.crawl(DiscourseSummarySpider)
            process.start()

            # Generate a summary of all the sites that were crawled
            generateCrawlSummary()
        elif crawltype == 'topics':
            # use DiscourseTopicSpider to perform a full crawl
            process.crawl(DiscourseTopicSpider)
            process.start()

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

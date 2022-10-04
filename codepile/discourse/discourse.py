from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.discourse.discourse_spider import DiscourseTopicSpider 
from scrapy.crawler import CrawlerProcess


class DiscourseScraper(Scraper):
    def scrape(self) -> RawDataset:
        process = CrawlerProcess()
        process.crawl(DiscourseTopicSpider)
        process.start()

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata=str(metadata))


class DiscourseDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = DiscourseScraper(tempdir, target_dir)
    def download(self):
        self.scraper.download()

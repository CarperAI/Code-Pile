from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.tutorialspoint.tutorialspoint_spider import TutorialspointArticleSpider
from scrapy.crawler import CrawlerProcess
import os, sys
import pathlib
from datetime import datetime

class TutorialspointScraper(Scraper):
    def __init__(self, tmppath, target_dir):
        self.target_dir = target_dir
        self.info = DatasetInfo(
            id="Tutorialspoint Dataset",
            description="Articles about various computing topics from TutorialsPoint.com",
            size=3,
            source_uri="https://tutorialspoint.com",
            dataset_pros="",
            dataset_cons="",
            languages=["english"],
            coding_languages=[],
            modalities=[],
            source_license="",
            source_citation="TutorialsPoint.com",
            data_owner="James Baicoianu",
            contributers=["James Baicoianu"],
            data_end=datetime(2022, 11, 3)
       )

    def download(self) -> RawDataset:

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
            'ITEM_PIPELINES': {
                'codepile.tutorialspoint.tutorialspoint_spider.JsonS3WriterPipeline': 300,
            }
            #"JOBDIR": "scrapy-job",
        }

        # TODO - crawl type should be an argument we can pass in
        crawltype = 'articles'

        if crawltype == 'articles':
            process = CrawlerProcess(crawlsettings)
            process.crawl(TutorialspointArticleSpider)
            process.start()

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata='')


#class TutorialspointProcessor(Processor):
#    def process(self, raw_data: RawDataset, *args, **kwargs):
#        # TODO - transform raw JSON data into whatever format we need for training
#        return

class TutorialspointDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = TutorialspointScraper(tempdir, target_dir)
        #self.processor = DiscourseCodeProcessor()
    def info(self):
        return self.info
    
    def id(self):
        return self.info.id
    def download(self):
        self.scraper.download()

if __name__=="__main__":
    if not os.path.exists("data/"):
            os.makedirs("data/")
    tutorialspoint_dataset = TutorialspointDataset('/tmp', sys.argv[1])
    print(tutorialspoint_dataset.download())

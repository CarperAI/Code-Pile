from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.discourse.discourse_spider import DiscourseSummarySpider, DiscourseTopicSpider, DiscourseAdditionalPostsSpider, generateCrawlSummary, verify_site
from codepile.discourse.discourse_processor import DiscourseProcessor
from scrapy.crawler import CrawlerProcess
import os, sys
import pathlib

USAGE_EXAMPLES = """
Usage: python3 -m codepile.discourse.discourse <action> <site> <datadir> <tmpdir>

    action : [index|download|compress|process|analyze]
    site   : [all|<site>[,<site2>,...]]


Place discourse/index.json inside of <datadir> before running


Example usage:

 - fetch the site index from each site in our index.json file

        python3 -m codepile.discourse.discourse index all ~/data/my-discourse-crawl /tmp


 - crawl topics from all indexed sites:

        python3 -m codepile.discourse.discourse download all ~/data/my-discourse-crawl /tmp


 - Compress each site into its own <site>.tar.gz file

        python3 -m codepile.discourse.discourse compress [all|<site>] ~/data/my-discourse-crawl /tmp


 - Process collected site data into lm_dataset format:

        python3 -m codepile.discourse.discourse process [all|<site>] ~/data/my-discourse-crawl /tmp


 - Collect stats about the sites included in this crawl:

        python3 -m codepile.discourse.discourse analyze [all|<site>] ~/data/my-discourse-crawl /tmp


 - Sync processed jsonl files to s3
        python3 -m codepile.discourse.discourse sync [all|<site>] ~/data/my-discourse-crawl /tmp
"""

class DiscourseScraper(Scraper):
    def __init__(self, tempdir, target_dir, *args, **kwargs):
        super().__init__(tempdir, target_dir, args, kwargs)
        pathlib.Path(self.target_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(self.target_dir)

    def get_crawl_settings(self):
        return {
            "SCHEDULER_PRIORITY_QUEUE": 'scrapy.pqueues.DownloaderAwarePriorityQueue',
            "CONCURRENT_REQUESTS": 1000,
            "LOG_LEVEL": "WARN",
            "DOWNLOAD_DELAY": .6,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": False,
            "AUTOTHROTTLE_DEBUG": True,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": .5,
            "REACTOR_THREADPOOL_MAXSIZE": 100,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": '2.7',
            "SCHEDULER_DEBUG": True,
            "TELNETCONSOLE_ENABLED": False,
            #"JOBDIR": "scrapy-job",
        }

    def scrape(self) -> RawDataset:
        pathlib.Path(self.target_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(self.target_dir)
        crawlsettings = self.get_crawl_settings()

        crawltype = 'summary'

        # use DiscourseTopicSpider to perform a full crawl
        process = CrawlerProcess(crawlsettings)
        process.crawl(DiscourseTopicSpider)
        process.start()

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata='')

    def index(self, update_indexes=True):
        if update_indexes:
            crawlsettings = self.get_crawl_settings()
            crawlsettings['RETRY_ENABLED'] = False
            process = CrawlerProcess(crawlsettings)
            process.crawl(DiscourseSummarySpider)
            process.start()

        # Generate a summary of all the sites that were crawled
        generateCrawlSummary()

    def verify(self, site):
        # verify the completeness of this dataset by comparing saved files with the index
        verify_site(site)
    def get_additional_posts(self, site="all"):
        os.chdir(self.target_dir)
        crawlsettings = self.get_crawl_settings()

        # use DiscourseTopicSpider to perform a full crawl
        process = CrawlerProcess(crawlsettings)
        process.crawl(DiscourseAdditionalPostsSpider, site=site)
        process.start()


class DiscourseDataset(Dataset):
    def __init__(self, tempdir, data_dir):
        self.tempdir = tempdir
        self.data_dir = data_dir
        self.scraper = DiscourseScraper(tempdir, data_dir)
        self.processor = DiscourseProcessor(tempdir, data_dir)
    def index(self):
        self.scraper.index()
    def download(self):
        self.scraper.scrape()
    def compress(self, site):
        self.processor.compress(site) 
    def process(self, site):
        self.processor.process(site) 
    def analyze(self, site):
        #self.processor.analyze(site)
        self.scraper.index(False)
    def sync(self, site):
        self.processor.sync(site) 
    def fix(self, site):
        # Fetch posts that were missing from the initial crawl (eg, additional replies to long topics)
        self.scraper.get_additional_posts(site)



if __name__=="__main__":
    if not os.path.exists("data/"):
        os.makedirs("data/")

    # TODO - site should be an optional parameter, so you can choose whether to work on individual sites or the whole dataset
    if len(sys.argv) > 4:
        action = sys.argv[1]
        site = sys.argv[2]
        datadir = sys.argv[3]
        tmpdir = sys.argv[4]

        discourse_dataset = DiscourseDataset(tmpdir, datadir)
        if action == 'index':
            discourse_dataset.index()
        elif action == 'download':
            discourse_dataset.download()
        elif action == 'compress':
            discourse_dataset.compress(site)
        elif action == 'process':
            discourse_dataset.process(site)
        elif action == 'analyze':
            discourse_dataset.analyze(site)
        elif action == 'sync':
            discourse_dataset.sync(site)
        elif action == 'fix':
            discourse_dataset.fix(site)
    else:
        print(USAGE_EXAMPLES)

from codepile.dataset import DatasetInfo, RawDataset, Scraper, Dataset

from datetime import datetime
import os
import boto3

from .processor import LeetCodeProcessor

LEETCODE_S3_BUCKET = "s-eai-neox"
LEETCODE_PARQUET_S3_PATH = "data/codepile/leetcode/leetcode_topics_with_questions.parquet.gzip"
LEETCODE_PARQUET_ZIP_NAME = "leetcode_topics_with_questions.parquet.gzip"

'''
# example
LeetCodeInfo = DatasetInfo(
        id="LeetCodeDataset",
        description="Competitive Programming Dataset taken from LeetCode",
        size=1,
        source_uri="https://leetcode.com/",
        dataset_pros="Programming Problem with Solutions and Solution proposals bring explicit context for language model",
        dataset_cons="Domain related to Competitive Programming Only",
        languages=["english"],
        coding_languages=["python", "c++", "java"],
        modalities=["source_code"],
        source_license="MIT",
        source_citation="LeetCode",
        data_owner="Cagatay Calli",
        contributers=["Cagatay Calli"],
        data_end=datetime(2022, 9, 29),
       )
'''
class LeetCodeScraper(Scraper):
    def scrape(self, metadata) -> RawDataset:
        if not os.path.exists(os.path.join(self.config.raw_data_dir, LEETCODE_PARQUET_ZIP_NAME)):
            s3 = boto3.client('s3')
            s3.download_file(LEETCODE_S3_BUCKET, LEETCODE_PARQUET_S3_PATH, os.path.join(self.config.raw_data_dir, LEETCODE_PARQUET_ZIP_NAME))
        return RawDataset(storage_uris=['file:///{self.config.raw_data_dir}'],
                metadata=str(metadata))


class LeetCodeDataset(Dataset):
    def __init__(self, config):
        self.config = config

        self.info = DatasetInfo(
            id="LeetCodeDataset",
            description="Competitive Programming Dataset taken from LeetCode",
            size=1,
            source_uri="https://leetcode.com/",
            dataset_pros="Programming Problem with Solutions and Solution proposals bring explicit context for language model",
            dataset_cons="Domain related to Competitive Programming Only",
            languages=["english"],
            coding_languages=["python", "c++", "java"],
            modalities=["source_code"],
            source_license="MIT",
            source_citation="LeetCode",
            data_owner="Cagatay Calli",
            contributers=["Cagatay Calli"],
            data_end=datetime(2022, 9, 29)
        )

        self.scraper = LeetCodeScraper(config, self.info.id)
        self.processor = LeetCodeProcessor(config)
    
    def info(self):
        return self.info
    
    def id(self):
        return self.info.id

    def download(self):
        if not os.path.exists(os.path.join(self.config.raw_data_dir, LEETCODE_PARQUET_ZIP_NAME)):
            self.scraper.scrape(metadata=self.info)

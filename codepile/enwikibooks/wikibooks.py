import os
import json
import pandas as pd
from codepile.dataset import RawDataset, Scraper, Dataset, DatasetInfo
from codepile.codepile import Config
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

BOOKS_S3_BUCKET = "s-eai-neox"

class WikiBookDataset(Dataset):

    def __init__(self, config):
        self.config = config
        self.info = DatasetInfo(
            id="WikiBook Dataset",
            description="The books deal with computing: usually defined as the activity of using and developing computer technology, computer hardware, and software get from Wikibook",
            size=3,
            source_uri="https://en.wikibooks.org/wiki/Department:Computing",
            dataset_pros="Books guarantee lience problems",
            dataset_cons="Small size",
            languages=["english"],
            coding_languages=["python, c++, java"],
            modalities=["code_review"],
            source_license="Free",
            source_citation="Wikibook",
            data_owner="Duy Phung",
            contributers=["Duy Phung"],
            data_end=datetime(2022, 10, 12)
       )
    
    def info(self):
        return self.info
    
    def id(self):
        return self.info.id

    def make_format(self, sample):
        title = sample['title']
        text = sample['text']
        return f"Book title: {title}\nBook Content: \n{text}"

    def fetch_raw(self, return_df=False):
        if not os.path.exists(self.config.raw_data_dir):
            os.makedirs(self.config.raw_data_dir)

        if not os.path.exists(os.path.join(self.config.raw_data_dir, 'computing_wikibooks.parquet')):
            s3 = boto3.client('s3')
            s3.download_file(BOOKS_S3_BUCKET, "data/codepile/books/computing_wikibooks.parquet", os.path.join(self.config.raw_data_dir, 'computing_wikibooks.parquet'))

        if return_df:
            return pd.read_parquet(os.path.join(self.config.raw_data_dir, 'computing_wikibooks.parquet'))

    def download(self, return_df=False):
        df = self.fetch_raw(return_df)
        return df
    

if __name__=="__main__":
    if not os.path.exists("data/"):
            os.makedirs("data/")
    config = Config(
        raw_data_dir="data/",
        output_data_dir="data/",
        tmpdir="/tmp"
    )
    book_dataset = WikiBookDataset(config)
    print(book_dataset.download(True))
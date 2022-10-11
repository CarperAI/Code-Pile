from genericpath import exists
import os
import json
import pandas as pd
from codepile.dataset import RawDataset, Scraper, Dataset
import boto3
import requests
from bs4 import BeautifulSoup

BOOKS_S3_BUCKET = "s-eai-neox"

class WikiBookDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = WikiBookScraper(tempdir, target_dir)

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

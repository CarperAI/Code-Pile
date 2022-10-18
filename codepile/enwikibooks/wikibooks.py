import os
import json
import pandas as pd
from codepile.dataset import RawDataset, Scraper, Dataset, DatasetInfo
from codepile.codepile import Config
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from codepile.tools.filtering import fitering_pipeline, fixes_text, uniform_whitespace, document_normalization
from codepile.tools.near_deduplication.minhash_deduplication import deduplicate_dataset
from datasets import Dataset


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
    
    def process(self):
        raw_df = pd.read_parquet(os.path.join(self.config.raw_data_dir, 'computing_wikibooks.parquet'))
        raw_df = raw_df.reset_index(drop=True)
        if 'id' not in raw_df.columns.values: # create id 
            raw_df['id'] = raw_df.index
        raw_df['content'] = raw_df.apply(lambda row: self.make_format(row), axis = 1) # content function will contain text/code to convert into lm_dataformat
        normalize_funcs = [fixes_text, uniform_whitespace] # normalize text
        raw_df['content'] = raw_df['content'].apply(lambda x: document_normalization(x, normalize_funcs))
        hf_dataset = Dataset.from_pandas(raw_df) 
        hf_dataset = hf_dataset.filter(lambda sample: fitering_pipeline(sample['content']) == False) # hf filtering 
        hf_dataset, duplicate_clusters = deduplicate_dataset(hf_dataset) # hf deduplication


if __name__=="__main__":
    if not os.path.exists("data/"):
            os.makedirs("data/")
    config = Config(
        raw_data_dir="data/",
        output_data_dir="data/",
        tmpdir="/tmp"
    )
    book_dataset = WikiBookDataset(config)
    book_dataset.download(False)
    book_dataset.process()
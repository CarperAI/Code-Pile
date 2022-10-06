import os
import gdown
import pandas as pd
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.codepile import Config
from datetime import datetime
import boto3


AI4CODE_KAGGLE_BUCKET = "s-eai-neox"

class AI4Code_Kaggle(Dataset):
    def __init__(self, config):
        self.config = config
        self.info = DatasetInfo(
            id="AI4Code Dataset",
            description="The dataset comprises about 160,000 Jupyter notebooks published by the Kaggle community.",
            size=3,
            source_uri="https://www.kaggle.com/competitions/AI4Code/data",
            dataset_pros="Notebook almost have description for code this will help for text to code",
            dataset_cons="Python language almost",
            languages=["english"],
            coding_languages=["python"],
            modalities=["source_code"],
            source_license="MIT",
            source_citation="AI4Code",
            data_owner="Duy Phung",
            contributers=["Duy Phung"],
            data_end=datetime(2022, 10, 6)
       )
    
    def info(self):
        return self.info
    
    def id(self):
        return self.info.id

    def fetch_raw(self, return_df=False):
        if not os.path.exists(self.config.raw_data_dir):
            os.makedirs(self.config.raw_data_dir)

        if not os.path.exists(os.path.join(self.config.raw_data_dir, 'AI4Code_Kaggle.parquet')):
            s3 = boto3.client('s3')
            s3.download_file(AI4CODE_KAGGLE_BUCKET, "data/codepile/AI4Code_Kaggle.parquet", os.path.join(self.config.raw_data_dir, 'AI4Code_Kaggle.parquet'))

        if return_df:
            return pd.read_parquet(os.path.join(self.config.raw_data_dir, 'AI4Code_Kaggle.parquet'))

    def download(self, return_df=False):
        df = self.fetch_raw(return_df)
        return df


if __name__=="__main__":
    config = Config(
        raw_data_dir="data/",
        output_data_dir="data/",
        tmpdir="/tmp"
    )
    kg_dataset = AI4Code_Kaggle(config)
    kg_dataset.download()
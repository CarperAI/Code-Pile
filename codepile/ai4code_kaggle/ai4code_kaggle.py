import os
import pandas as pd
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
from codepile.codepile import Config
from datetime import datetime
import boto3
import datasets
from codepile.tools.filtering import fitering_pipeline
from codepile.tools.near_deduplication.minhash_deduplication import deduplicate_dataset
from codepile.tools.bigscience_pii_detect_redact import run_pii_batch
from functools import partial
from lm_dataformat import Archive, Reader
from tqdm import tqdm


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

    def process(self):
        raw_df = pd.read_parquet(os.path.join(self.config.raw_data_dir, 'AI4Code_Kaggle.parquet'))
        raw_df = raw_df.reset_index(drop=True)
        raw_df.columns = ['id', 'content']
        if 'id' not in raw_df.columns.values: # create id 
            raw_df['id'] = raw_df.index
        hf_dataset = datasets.Dataset.from_pandas(raw_df)
        """
        # hf dataset filtering 
        print("Run filtering")
        hf_dataset = hf_dataset.filter(lambda sample: fitering_pipeline(sample['content']) == False) 
        # run PII
        print("Run PII")
        hf_dataset = hf_dataset.map(
            partial(run_pii_batch),
            batched=True,
            batch_size=16,
            num_proc=16
        )
        # hf near-deduplication
        print("Run deduplication")
        hf_dataset, duplicate_clusters = deduplicate_dataset(hf_dataset)
        #import ipdb; ipdb.set_trace()
        # convert to lmdata_format
        """
        ar = Archive(str(self.config.output_data_dir))
        for content in tqdm(hf_dataset['content']):
            ar.add_data(content, meta={"source": self.__class__.__name__,
                "fields": list(hf_dataset.features.keys())})
        ar.commit(self.__class__.__name__)


if __name__=="__main__":
    if not os.path.exists("data/"):
            os.makedirs("data/")
    if not os.path.exists("data_lm/"):
            os.makedirs("data_lm/")
    config = Config(
        raw_data_dir="data/",
        output_data_dir="data_lm/",
        tmpdir="/tmp"
    )
    kg_dataset = AI4Code_Kaggle(config)
    kg_dataset.download(False)
    kg_dataset.process()
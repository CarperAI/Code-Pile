import os
import gdown
import pandas as pd
from codepile.dataset import Dataset

AI4CODE_KAGGLE_URL = "https://drive.google.com/file/d/1RgiY7_zvLPdMPOXaOL4aE-Or5KuZXQ02/view?usp=sharing"

class AI4Code_Kaggle(Dataset):
    def __init__(self, tempdir, target_dir):
        self.tempdir = tempdir
        self.target_dir = target_dir

    def fetch_raw(self, return_df=True):

        if not os.path.exists(os.path.join(self.target_dir, 'AI4Code_Kaggle.parquet')):
            gdown.download(url=AI4CODE_KAGGLE_URL, output=os.path.join(self.target_dir, 'AI4Code_Kaggle.parquet'), quiet=False, fuzzy=True)

        if return_df:
            return pd.read_parquet(os.path.join(self.target_dir, 'AI4Code_Kaggle.parquet'))

    def download(self, return_df=False):
        df = self.fetch_raw(return_df)
        return df


if __name__=="__main__":

    kg_dataset = AI4Code_Kaggle('data/', 'data/')
    df = kg_dataset.download(True)
    print(df.head())
    df.head().to_parquet("data/ai4code_kaggle_dummy.parquet")
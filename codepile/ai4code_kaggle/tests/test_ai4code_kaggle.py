## Test Topcoder Crawling
import os
import pandas as pd
from codepile.ai4code_kaggle.ai4code_kaggle import AI4Code_Kaggle
from codepile.codepile import Config

import pytest

class TestAI4CodeDataset:
    @pytest.mark.s3_download
    def test_same_cols(self):
        if not os.path.exists("data/"):
                os.makedirs("data/")
        config = Config(
            raw_data_dir="data/",
            output_data_dir="data/",
            tmpdir="/tmp"
        )
        ai4code_dataset = AI4Code_Kaggle(config)
        ai4code_dataset.download()

        self.dummy = pd.read_parquet("test/ai4code_kaggle_dummy.parquet")
        self.df = pd.read_parquet("data/AI4Code_Kaggle.parquet")

        self.assertEqual(self.dummy.columns.tolist(), self.df.columns.tolist())

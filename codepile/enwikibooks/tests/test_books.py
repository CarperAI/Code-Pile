import os
import pandas as pd
from codepile.codepile import Config
from codepile.enwikibooks.wikibooks import WikiBookDataset

import pytest

class TestWikiBookDataset:
    @pytest.mark.s3_download
    def test_same_cols(self):
        if not os.path.exists("data/"):
            os.makedirs("data/")
        config = Config(
            raw_data_dir="data/",
            output_data_dir="data/",
            tmpdir="/tmp"
        )
        book_dataset = WikiBookDataset(config)
        book_dataset.download()
        self.dummy = pd.read_parquet("test/computing_wikibook_dummy.parquet")
        self.df = pd.read_parquet("data/computing_wikibooks.parquet")
        self.assertEqual(self.dummy.columns.tolist(), self.df.columns.tolist())

import os
import pandas as pd
from wikibooks import WikiBookDataset

from unittest import TestCase
class TestWikiBookDataset(TestCase):
    
    def setUp(self):
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

    def test_same_cols(self):
        self.setUp()
        self.assertEqual(self.dummy.columns.tolist(), self.df.columns.tolist())

if __name__ == '__main__':
    TestWikiBookDataset().test_same_cols()
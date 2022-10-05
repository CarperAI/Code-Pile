import os
import pandas as pd
from wikibooks import WikiBookDataset

from unittest import TestCase
class TestWikiBookDataset(TestCase):
    def setUp(self):
        if not os.path.exists('data3'):
            os.mkdir('data3')
        self.wikibook_comp = WikiBookDataset("data3/", "data3/")
        self.wikibook_comp.download()
        self.dummy = pd.read_parquet("test/computing_wikibook_dummy.parquet")
        self.df = pd.read_parquet("data3/computing_wikibooks.parquet.gzip")
    def test_same_cols(self):
        self.setUp()
        self.assertEqual(self.dummy.columns.tolist(), self.df.columns.tolist())

if __name__ == '__main__':
    TestWikiBookDataset().test_same_cols()
import os
import pandas as pd
from codepile.competitive_programming.competitive_programming import CPDataset
from codepile.codepile import Config

import pytest

class TestCPDataset:
    @pytest.mark.s3_download
    def test_same_cols(self):
        if not os.path.exists('data/'):
            os.mkdir('data/')

        config = Config(
            raw_data_dir="data/",
            output_data_dir="data/",
            tmpdir="/tmp"
        )
        cp_dataset = CPDataset(config)
        cp_dataset.download()

        self.cc_dummy = pd.read_pickle("test/CodeContest_dummy.pickle")
        self.tc_dummy = pd.read_pickle("test/TopCoder_dummy.pickle")

        self.cc_df = pd.read_pickle("data/CodeContest_raw.pickle")
        self.tc_df = pd.read_pickle("data/TopCoder_raw.pickle")

        self.assertEqual(self.cc_dummy.columns.tolist(), self.cc_df.columns.tolist())
        self.assertEqual(self.tc_dummy.columns.tolist(), self.tc_df.columns.tolist())
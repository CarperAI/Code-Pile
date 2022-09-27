## Test Topcoder Crawling
import os
import pandas as pd

# make directory if not exists
if not os.path.exists('data/'):
    os.mkdir('data/')

from topcoder import TopCoderDataset


tc_dataset = TopCoderDataset("data/", "data/")
tc_dataset.download()

dummy = pd.read_pickle("test/TopCoder_dummy.pickle")
tc_df = pd.read_pickle("data/TopCoder_raw.pickle")

assert dummy.columns.tolist() == tc_df.columns.tolist()
## Test commpetitive dataset

from competitive_programming import CPDataset

cp_dataset = CPDataset("data/", "data/")
cp_dataset.download()

cc_dummy = pd.read_pickle("test/CodeContest_dummy.pickle")
tc_dummy = pd.read_pickle("test/TopCoder_dummy.pickle")

cc_df = pd.read_pickle("data/CodeContest_raw.pickle")
tc_df = pd.read_pickle("data/TopCoder_raw.pickle")

assert cc_dummy.columns.tolist() == cc_df.columns.tolist()
assert tc_dummy.columns.tolist() == tc_df.columns.tolist()
print("All tests passed!")

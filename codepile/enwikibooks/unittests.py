import os
import pandas as pd
from wikibooks import WikiBookDataset

# make directory if not exists
if not os.path.exists('data'):
    os.mkdir('data')


wikibook_comp = WikiBookDataset("data/", "data/")
wikibook_comp.download()

dummy = pd.read_parquet("test/computing_wikibook_dummy.parquet")
df = pd.read_parquet("data/computing_wikibooks.parquet.gzip")

assert dummy.columns.tolist() == df.columns.tolist()

print("Passed test!")
## Test Topcoder Crawling
import os
import pandas as pd
from ai4code_kaggle import AI4Code_Kaggle

# make directory if not exists
if not os.path.exists('data'):
    os.mkdir('data')


ai4code_dataset = AI4Code_Kaggle("data/", "data/")
ai4code_dataset.download()

dummy = pd.read_parquet("test/ai4code_kaggle_dummy.parquet")
df = pd.read_parquet("data/AI4Code_Kaggle.parquet")

assert dummy.columns.tolist() == df.columns.tolist()

print("Passed test!")
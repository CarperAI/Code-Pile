import enum
import os
import json
import pickle
import pandas as pd

def get_all_wikibooks(root_path):
    wikibooks = []
    for path, currentDirectory, files in os.walk(root_path):
        for file in files:
            wikibooks.append(os.path.join(path, file))

    doc_wikibooks = []

    for wikibook in wikibooks:
        with open(wikibook, 'r') as json_file:
            json_list = list(json_file)
        for json_str in json_list:
            result = json.loads(json_str)
            doc_wikibooks.append(result)
    df = pd.DataFrame(doc_wikibooks)
    return df

def get_computing_wikibooks(df, total_titles):
    df_titles = ['_'.join(title.split()) for title in df.title.values]
    idx_list = []
    for i, title in enumerate(df_titles):
        if title in total_titles:
            idx_list.append(i)
    df = df.iloc[idx_list]
    return df

if __name__=="__main__":
    df = get_all_wikibooks("data/enwikibooks_json/")
    df.to_parquet('data/all_wikibooks.parquet.gzip', compression='gzip')
    total_titles = pickle.load(open("data/wikibooks_computing_titles.pkl", "rb"))
    df = get_computing_wikibooks(df, total_titles)
    df.to_parquet('data/computing_wikibooks.parquet.gzip', compression='gzip')
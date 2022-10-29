from cProfile import run
import sys
from codepile.tools.filtering import fitering_pipeline, clean_html_tags
from codepile.tools.near_deduplication.minhash_deduplication import deduplicate_dataset
from codepile.tools.bigscience_pii_detect_redact import run_pii_batch, run_pii
from functools import partial
from lm_dataformat import Archive, Reader
from tqdm import tqdm
import datasets
import pandas as pd 
import json
import re
import argparse
import numpy as np
import os
from multiprocessing import Pool
from datasets import load_dataset

args = argparse.ArgumentParser()
args.add_argument('--clean-html-body', action='store_true', default=True)
args.add_argument('--filter', action='store_true', default=True)
args = args.parse_args()

def tryjsonload(x):
    try:
        return json.loads(x)
    except:
        return None


def read_file(file):
    df = pd.read_parquet(file)
    df['answers'] = df.answers.apply(lambda x: tryjsonload(x))
    df['comments'] = df.comments.apply(lambda x: tryjsonload(x))
    df['AcceptedAnswerId'] = df['AcceptedAnswerId'].fillna(-111)
    return df

def make_format(sample):
    prompt = ""
    if sample['Title']:
        prompt = prompt + "Title: " + sample['Title'] + "\n"
    if sample['Tags']:
        tags = ';'.join(sample['Tags'].split('><')).replace('<', '').replace('>', '')
        prompt = prompt + "Tags: " + tags + "\n"
    if sample['Body'] and args.clean_html_body:
        prompt = prompt + "Question: " + clean_html_tags(sample['Body']) + "\n"
    comments = []
    if sample['comments']:
        for comment in sample['comments']:
            if comment['Text'] and args.clean_html_body:
                comments.append("Comment: " + clean_html_tags(comment['Text']) + "\n")
    prompt = prompt + "\n".join(comments)
    accepted = []
    others = []
    if sample['answers']:
        for answer in sample['answers']:
            if str(answer['Id']) == str(int(sample['AcceptedAnswerId'])):
                    if answer['Body'] and args.clean_html_body:
                        answer_txt = "Here is the accepted answer: " + clean_html_tags(answer['Body']) + "\n"
                        if answer['comments']:
                            comments = json.loads(answer['comments'])
                            for comment in comments:
                                if comment['Text'] and args.clean_html_body:
                                    answer_txt = answer_txt + "Comment for this answer: " + clean_html_tags(comment['Text']) + "\n"
                        accepted.append(answer_txt)
            else:
                if answer['Body'] and args.clean_html_body:
                        answer_txt = "Here is another answer: " + clean_html_tags(answer['Body']) + "\n"
                        if answer['comments']:
                            comments = json.loads(answer['comments'])
                            for comment in comments:
                                if comment['Text'] and args.clean_html_body:
                                    answer_txt = answer_txt + "Comment for this answer: " + clean_html_tags(comment['Text']) + "\n"
                        others.append(answer_txt)
    prompt = prompt + "\n" + "\n".join(accepted) + "\n" + "\n".join(others)
    return prompt

def process_file(file):
    df = read_file(file)
    if df.shape[0] == 0:
        return None
    df['content'] = df.apply(lambda x: make_format(x), axis=1)
    df = df[['Id', 'content']]
    df.to_parquet(os.path.join('data_content', file.split('/')[-1]))

uni_index = 0
from p_tqdm import p_map


def custom_pii(x):
    return run_pii(x)[0]
    
def filter_file(file):
    global uni_index
    df = pd.read_parquet(file)
    ids = []
    for i in range(df.shape[0]):
        ids.append(uni_index)
        uni_index += 1
    df['id'] = ids



    # df['content'] = p_map(custom_pii, df.content.tolist(), num_cpus=32)
    df.to_parquet(os.path.join('data_content_filtered', file.split('/')[-1]))

    # hf_dataset = datasets.Dataset.from_pandas(df) 
    #     # hf dataset filtering 
    # hf_dataset = hf_dataset.filter(lambda sample: fitering_pipeline(sample['content']) == False)
    
    # hf_dataset = hf_dataset.map(
    #         partial(run_pii_batch),
    #        batched=True,
    #        batch_size=1000,
    #         num_proc=32
    # )
    # new_df = hf_dataset.to_pandas()
    # new_df.to_parquet(os.path.join('data_content_filtered', file.split('/')[-1]))
    # print("Number of samples before filtering: ", len(df))
    # print("Number of samples after filtering: ", len(hf_dataset))
    # print("=====================================")
    
def dedup():
    files = [os.path.join('data_content_filtered', x) for x in os.listidr("data_content_filtered")]
    ds = load_dataset('parquet', data_files=files)
    

if __name__=="__main__":
    if args.filter:
        # if not os.path.exists('data_content_filtered'):
        #     os.mkdir('data_content_filtered')
        lst_parquet_files = [os.path.join('data_content', x) for x in os.listdir('data_content')]
            # pool = Pool(4)
    # for _ in tqdm(pool.imap_unordered(process_file, lst_parquet_files), total=len(lst_parquet_files)):
    #     pass
        from lm_dataformat import Archive, Reader
        ar = Archive('data_lm/')
        for file in tqdm(lst_parquet_files):
            df = pd.read_parquet(file)
            for content in df['content']:
                ar.add_data(content, meta={"source": "StackExchangeDataset",
                    "fields": list(df.columns.values)})
        ar.commit("StackExchangeDataset")
            
        exit()
    
    lst_parquet_files = []
    for root, dirs, files in os.walk("./data/"):
        for file in files:
            if file.endswith(".parquet"):
                lst_parquet_files.append(os.path.join(root, file))
    # pool = Pool(4)
    # for _ in tqdm(pool.imap_unordered(process_file, lst_parquet_files), total=len(lst_parquet_files)):
    #     pass
        
    for file in tqdm(lst_parquet_files):
        process_file(file)

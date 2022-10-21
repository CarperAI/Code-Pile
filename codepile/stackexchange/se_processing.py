from codepile.tools.filtering import fitering_pipeline, clean_html_tags
from codepile.tools.near_deduplication.minhash_deduplication import deduplicate_dataset
from codepile.tools.bigscience_pii_detect_redact import run_pii_batch
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

args = argparse.ArgumentParser()
args.add_argument('--clean-html-body', action='store_true', default=True)
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
        prompt = prompt + "Tags: " + sample['Tags'] + "\n"
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
                        accepted.append("Here is the accepted answer: " + clean_html_tags(answer['Body']) + "\n")
            else:
                if answer['Body'] and args.clean_html_body:
                        others.append("Here is another answer: " + clean_html_tags(answer['Body']) + "\n")
    prompt = prompt + "\n" + "\n".join(accepted) + "\n" + "\n".join(others)
    return prompt

def process_file(file):
    df = read_file(file)
    if df.shape[0] == 0:
        return None
    df['content'] = df.apply(lambda x: make_format(x), axis=1)
    df = df[['Id', 'content']]
    df.to_parquet(os.path.join('data_content', file.split('/')[-1]))
        
if __name__=="__main__":
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

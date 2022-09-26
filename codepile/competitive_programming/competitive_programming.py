import os
import json
import requests
import gdown
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset

LIST_DATASET = ['CodeContest', 'TopCoder']
CODE_CONTEST_URL_RAW = "https://drive.google.com/file/d/1MOdiZ6sCgJiUcrRNUMJTN06RWhei2q90/view?usp=sharing"
TOPCODER_URL_RAW = "https://drive.google.com/file/d/1Mvvsm_a70gNTWKkPEcjX2AR29XkP2sws/view?usp=sharing"

class CPDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.tempdir = tempdir
        self.target_dir = target_dir

    def make_format_code_contest(self, sample):
        title = sample['name']
        description = sample['description']
        difficulty = sample['difficulty']
        tags = sample['cf_tags']
        source = sample['source']
        time_limit = sample['time_limit']
        memory_limit = sample['memory_limit_bytes']
        solutions = sample['solutions']
        incorrect_solutions = sample['incorrect_solutions']
        hint = sample['hint_string']
        title = "<title> " + title + " </title>"
        problem = "<problem "  + "source=" + source +  " tags=" + ','.join(tags) \
            + " time_limit=" + time_limit + " memory_limit=" \
            + memory_limit + " difficulty=" + difficulty + ">\n"
        problem = problem + description + "\n</problem>"
        lst = []
        for sol in solutions:
            lst.append("<code language=" + sol['language'] + ">\n" + sol['solution'] + "\n</code>")
        solutions = "\n".join(lst)
        lst = []
        for sol in incorrect_solutions:
            lst.append("<code language=" + sol['language'] + ">\n" + sol['solution'] + "\n</code>")
        incorrect_solutions = "\n".join(lst)
        text = title + "\n" + problem + "\n" + "<hint> " + hint + "\n</hint>" \
            + "\n" +  "<correct_solutions>\n" + solutions + "\n</correct_solutions>" \
            + "\n" +  "<incorrect_solutions>\n" + incorrect_solutions + "\n</incorrect_solutions>"
        return text
    
    def make_format_topcoder(self, sample):
        name = sample['name']
        description = sample['description']
        solutions = sample['solutions']

        title = "<title> " + name + " </title>"
        problem = "<problem>\n" + description + "\n</problem>"
        lst = []
        for sol in solutions:
            lst.append("<code>\n" + sol.strip() + "\n</code>")
        solutions = "\n".join(lst)
        text = title + "\n" + problem + "\n" + "<correct_solutions>\n" + solutions + "\n</correct_solutions>"
        return text

    def make_format(self, sample, source):
        if source == 'CodeContest':
            return self.make_format_code_contest(sample)
        elif source == 'TopCoder':
            return self.make_format_topcoder(sample)
        else:
            raise ValueError('Unknown source')

    def fetch_raw(self, return_df=True):

        if not os.path.exists(os.path.join(self.target_dir, 'CodeContest_raw.pickle')):
            gdown.download(CODE_CONTEST_URL_RAW, os.path.join(self.target_dir, 'CodeContest_raw.pickle'), quiet=False)
        
        if not os.path.exists(os.path.join(self.target_dir, 'TopCoder_raw.pickle')):
            gdown.download(TOPCODER_URL_RAW, os.path.join(self.target_dir, 'TopCoder_raw.pickle'), quiet=False)

        if return_df:
            return {'CodeContest': pd.read_pickle(os.path.join(self.target_dir, 'CodeContest_raw.pickle')),
                    'TopCoder': pd.read_pickle(os.path.join(self.target_dir, 'TopCoder_raw.pickle'))}
    
    def download(self):
        self.fetch_raw(return_df=False)


if __name__=="__main__":

    cp_dataset = CPDataset('data/', 'data/')
    cp_dataset.download()
from functools import cache
import os
import json
import requests
import gdown
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset

LIST_DATASET = ['CodeContest', 'TopCoder']
CODE_CONTEST_URL_RAW = ""
TOPCODER_URL_RAW = ""

class CPScraper(Scraper):


    def fetch_raw(self):

        if not os.path.exists(os.path.join(self.tempdir, 'CodeContest_raw.pickle')):
            gdown.download(CODE_CONTEST_URL_RAW, os.path.join(self.tempdir, 'CodeContest_raw.pickle'), quiet=False)
        
        if not os.path.exists(os.path.join(self.tempdir, 'TopCoder_raw.pickle')):
            gdown.download(TOPCODER_URL_RAW, os.path.join(self.tempdir, 'TopCoder_raw.pickle'), quiet=False)

        cc_df = pd.read_pickle(os.path.join(self.tempdir, 'CodeContest_raw.pickle'))
        tc_df = pd.read_pickle(os.path.join(self.tempdir, 'TopCoder_raw.pickle'))

        return {"CodeContest": cc_df, "TopCoder": tc_df}

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
            + memory_limit + " difficulty=" + difficulty + "> "
        problem = problem + description + " </problem>"
        lst = []
        for sol in solutions:
            lst.append("<code language=" + sol['language'] + ">" + sol['solution'] + "</code>")
        solutions = "\n".join(lst)
        lst = []
        for sol in incorrect_solutions:
            lst.append("<code language=" + sol['language'] + "> " + sol['solution'] + " </code>")
        incorrect_solutions = "\n".join(lst)
        text = title + "\n" + problem + "\n" + "<hint> " + hint + " </hint>" \
            + "\n" +  "<correct_solutions>\n" + solutions + "\n</correct_solutions>" \
            + "\n" +  "<incorrect_solutions>\n" + incorrect_solutions + "</incorrect_solutions>"
        return text
    
    def make_format_topcoder(self, sample):
        name = sample['name']
        description = sample['description']
        solutions = sample['solutions']

        title = "<title> " + name + " </title>"
        problem = "<problem> " + description + " </problem>"
        lst = []
        for sol in solutions:
            lst.append("<code>" + sol.strip() + "</code>")
        solutions = "\n".join(lst)
        text = title + "\n" + problem + "\n" + "<correct_solutions>\n" + solutions + "\n</correct_solutions>"
        return text

    def make_format(self):
        dct = self.fetch_raw()
        cc_df = dct['CodeContest']
        tc_df = dct['TopCoder']
        all_text = []
        all_name = []
        all_source = []
        for _, row in tqdm(cc_df.iterrows(), total=len(cc_df)):
            text = self.make_format_code_contest(row)
            all_text.append(text)
            all_name.append(row['name'])
            all_source.append('CodeContest')

        for _, row in tqdm(tc_df.iterrows(), total=len(tc_df)):
            text = self.make_format_topcoder(row)
            all_text.append(text)
            all_name.append(row['name'])
            text = self.make_format_topcoder(row)
            all_text.append(text)
            all_name.append(row['name'])
            all_source.append('TopCoder')
            
        return pd.DataFrame.from_dict({'name': all_name, 'text': all_text, 'source': all_source})



    def scrape(self) -> RawDataset:

        if os.path.exists(os.path.join(self.target_dir, "CompetitiveProgramming.pickle")):
            return RawDataset(storage_uris=['file:///{self.target_dir}'])
        
        df = self.make_format()
        df.to_pickle(os.path.join(self.target_dir, "CompetitiveProgramming.pickle"))
        return RawDataset(storage_uris=['file:///{self.target_dir}'])



class CPDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = CPScraper(tempdir, target_dir)
    def download(self):
        self.scraper.scrape()



if __name__=="__main__":

    cp_dataset = CPDataset('data/', 'data/')
    cp_dataset.download()
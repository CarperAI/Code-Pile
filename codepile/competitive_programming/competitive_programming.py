from functools import cache
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset


class CPDataset(Scraper):


    def scrape(self) -> RawDataset:
        return RawDataset(storage_uris=['file:///{self.target_dir}'])



class TopCoderDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = TopCoder(tempdir, target_dir)
    def download(self):
        self.scraper.scrape()



if __name__=="__main__":

    cp_dataset = CPDataset('data/', 'data/')
    cp_dataset.download()
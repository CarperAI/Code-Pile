import requests
from bs4 import BeautifulSoup

import os
import json
import pickle
import pandas as pd


class WikiBookDataset():

    def __init__(self, wikiextractor_dir=None):
        self.wikiextractor_dir = wikiextractor_dir # directory output json format from wikiextractor
        self.df_books = self.get_all_wikibooks()

    def _scrape_titles(self, category_url):
        # get all books title from category_url

        html = requests.get(category_url).text
        soup = BeautifulSoup(html, "html.parser")

        titles = []
        urls = []
        main_div = soup.find('div', {'id': 'mw-content-text'})
        for a in main_div.find_all('a', href=True):
            if a['href'].startswith('/wiki/'):
                titles.append(a['href'][6:])
                urls.append("https://en.wikibooks.org/" + a['href'])
        return titles, urls

    def get_title_by_category(self, category):
        # get all books title from category name
        if category == "Computing" or  category == "computing":
            category_url = "https://en.wikibooks.org/wiki/Department:Computing"
        depth = 2 # depth of recursion to get all titles
        total_titles = []
        total_urls = []
        while depth:
            titles, urls = self._scrape_titles(category_url)
            total_titles.extend(titles)
            total_urls.extend(urls)
            for url in urls:
                titles, urls = self._scrape_titles(url)
                total_titles.extend(titles)
                total_urls.extend(urls)
            depth -= 1
        return total_titles, total_urls


    def get_all_wikibooks(self):
        wikibooks = []
        for path, currentDirectory, files in os.walk(self.wikiextractor_dir):
            for file in files:
                wikibooks.append(os.path.join(path, file))

        doc_wikibooks = []

        for wikibook in wikibooks:
            with open(wikibook, 'r') as json_file:
                json_list = list(json_file)
            for json_str in json_list:
                result = json.loads(json_str)
                doc_wikibooks.append(result)
        return pd.DataFrame(doc_wikibooks)
        

    def get_wikibooks_by_category(self, category='Computing'):

        cat_titles, cat_urls = self.get_title_by_category(category)
        df_titles = ['_'.join(title.split()) for title in self.df_books.title.values] # convert title to match with wikibooks
        idx_list = []
        for i, title in enumerate(df_titles):
            if title in cat_titles:
                idx_list.append(i)
        return self.df_books.iloc[idx_list]


    

if __name__=="__main__":
    data = WikiBookDataset(wikiextractor_dir="data/enwikibooks_json_keep_block/")
    df = data.df_books
    df_computing = data.get_wikibooks_by_category(category='Computing')
    df.to_parquet("data/all_wikibooks.parquet.gzip", compression='gzip')
    df_computing.to_parquet("data/computing_wikibooks.parquet.gzip", compression='gzip')

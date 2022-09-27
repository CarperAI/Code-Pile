from genericpath import exists
import os
import json
import pandas as pd
from codepile.dataset import RawDataset, Scraper, Dataset
import gdown

import requests
from bs4 import BeautifulSoup

WIKIBOOK_DUMPS_URL = "https://dumps.wikimedia.org/enwikibooks/20220901/enwikibooks-20220901-pages-meta-current.xml.bz2"
COMPUTING_WIKIBOOKS_URL = "https://drive.google.com/file/d/1RqG63qdxIrdGVscRQnECx2tO2csyaFX0/view?usp=sharing"
ALL_WIKIBOOKS_URL = "https://drive.google.com/file/d/1GbL4MiIwMnzeIIp4HEieMopkYNFvjp-H/view?usp=sharing"

class WikiBookScraper(Scraper):
    # tempdir: wikiextractor output directory
    # target dir: parquest file directory

    def download_wiki_parser(self):
        # download wikidump
        if os.path.exists(self.tempdir, exists=True):
            os.mkdir(self.tempdir)
        os.system(f"wget {WIKIBOOK_DUMPS_URL} -O {self.tempdir}/enwikibooks-20220820-pages-meta-current.xml.bz2")
        os.system(f"bzip2 -d {self.tempdir}/enwikibooks-20220820-pages-meta-current.xml.bz2")
    
        # wikiextractor process
        os.system(f"python wikiextractor_keep_block/WikiExtractor.py  --json {self.tempdir}/enwikibooks-20220820-pages-meta-current.xml -o {self.tempdir}/enwikibooks_json_keep_block")


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
        for path, currentDirectory, files in os.walk(os.path.join(self.tempdir, "enwikibooks_json_keep_block")):
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
        

    def get_wikibooks_by_category(self):
        category='Computing'
        self.df_books = self.get_all_wikibooks()
        cat_titles, cat_urls = self.get_title_by_category(category)
        df_titles = ['_'.join(title.split()) for title in self.df_books.title.values] # convert title to match with wikibooks
        idx_list = []
        for i, title in enumerate(df_titles):
            if title in cat_titles:
                idx_list.append(i)
        return self.df_books, self.df_books.iloc[idx_list]

    def scrape(self):
        self.download_wiki_parser()
        df_all, df_computing = self.get_wikibooks_by_category()
        df_all.to_parquet(f"{self.target_dir}/all_wikibooks.parquet.gzip", compression='gzip')
        df_computing.to_parquet(f"{self.target_dir}/computing_wikibooks.parquet.gzip", compression='gzip')
        return RawDataset(storage_uris=['file:///{self.target_dir}'])


class WikiBookDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = WikiBookScraper(tempdir, target_dir)

    def download(self, return_df=False):
        if not os.path.exists(os.path.join(self.scraper.target_dir, 'computing_wikibooks.parquet.gzip')):
            try:
                gdown.download(COMPUTING_WIKIBOOKS_URL, os.path.join(self.scraper.target_dir, 'computing_wikibooks.parquet.gzip'), quiet=False, fuzzy=True)
                gdown.download(ALL_WIKIBOOKS_URL, os.path.join(self.scraper.target_dir, 'all_wikibooks.parquet.gzip'), quiet=False, fuzzy=True)
            except:
                self.scraper.scrape()
        if return_df:
            return pd.read_parquet(os.path.join(self.scraper.target_dir, 'computing_wikibooks.parquet.gzip'))


if __name__=="__main__":
    data = WikiBookDataset(tempdir="data/", target_dir="data/")
    df = data.download(return_df=True)
    print(df.head())
    df.head().to_parquet("test/computing_wikibook_dummy.parquet")
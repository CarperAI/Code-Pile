from functools import cache
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset


TOPCODER_ARCHIVE_URL = "https://www.topcoder.com/tc?module=ProblemArchive&sr=0&er=10000&sc=&sd=&class=&cat=&div1l=&div2l=&mind1s=&mind2s=&maxd1s=&maxd2s=&wr="
COOKIES_PATH = 'topcoder_cookies.json'
HEADERS_PATH = 'topcoder_headers.json'
# Get cookies from network in browers https://community.topcoder.com/stat?c=problem_solution&cr=14970299&rd=19420&pm=17819

COOKIES = json.load(open(COOKIES_PATH, 'r'))
HEADERS = json.load(open(HEADERS_PATH, 'r'))

class TopCoder(Scraper):

    def get_all_problems(self):

        html = requests.get(TOPCODER_ARCHIVE_URL).text
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table', {'class': 'paddingTable2'})
        table = None
        for tb in tables:
            if "SRM" in str(tb):
                table = tb
                break
        # find all tr contain tag <a>
        all_tr = table.find_all('tr')
        all_tr_a = []
        for tr in all_tr:
            if tr.find('a'):
                all_tr_a.append(tr)
        all_tr_a = all_tr_a[2:] # skip two first decorator <tr>
        
        lst_url = []
        lst_name = []

        for tr  in all_tr_a:
            url = tr.find_all('a')[0].get('href')
            if 'problem_statement' in url:
                url = url.replace('amp;', '')
                url = 'https://community.topcoder.com/' + url
                name = tr.find_all('a')[0].text.strip()
                lst_url.append(url)
                lst_name.append(name)
        return lst_url, lst_name


    def get_problem_solution(self, url):
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        problem_statement = soup.find('td', {'class': 'problemText'}).text.strip()
        solution_url = 'https://community.topcoder.com/' + soup.find_all('td', {'class':"statText"})[-1].find('a').get('href')
        html = requests.get(solution_url).text
        soup = BeautifulSoup(html, 'html.parser')
        sol_urls = []
        for td in soup.find_all('td', {'class':"statText"}):
            if td.find('a'):
                url = 'https://community.topcoder.com/' + td.find('a').get('href')
                if 'stat?c=problem_solution' in url:
                    sol_urls.append(url)
        solutions = []
        for url in sol_urls: # to crawl solution require login need to get cookies from browser
        
            params = {
                'c': 'problem_solution'
            }
            elements = url.split('&')[1:]
            for e in elements:
                k, v = e.split('=')
                params[k] = v
            html = requests.get(url, params=params, headers=HEADERS, cookies=COOKIES).text
            solutions.append(html)
        return problem_statement, solutions


    def scrape(self) -> RawDataset:
        total_url, total_name = self.get_all_problems()
        all_problem = []
        all_solution = []
        all_url = []
        all_name = []
        failed_url = []
        failed_name = []
        if os.path.exists(os.path.join(self.target_dir, "topcoder.parquet")):
            df_cache = pd.read_parquet(os.path.join(self.target_dir, "topcoder.parquet"))
            cache_url = set(df_cache['url'].tolist())
        else:
            cache_url = []

        for (i, (url, name)) in tqdm(enumerate(zip(total_url, total_name)), total=len(total_url)):
            if url in cache_url:
                continue
            try:
                problem_statement, solutions = self.get_problem_solution(url)
                if len(solutions) == 0:
                    failed_name.append(name)
                    failed_url.append(url)
                    print(f"Zero solution: {url}")
            except Exception as error:
                failed_url.append(url)
                failed_name.append(name)
                print(f"Failed to crawl: {url}")
                print(f"Error: {error}")
                continue
            all_problem.append(problem_statement)
            all_solution.append(solutions)
            all_url.append(url)
            all_name.append(name)
            if (i + 1) % 100 == 0:
                df = pd.DataFrame({'problem': all_problem, 'solution': all_solution, 'url': all_url, 'name': all_name})
                df.to_pickle(os.path.join(self.target_dir, 'topcoder_success.parquet'))


        df_failed = pd.DataFrame({'url': failed_url, 'name': failed_name})
        df = pd.DataFrame({'problem': all_problem, 'solution': all_solution, 'url': all_url, 'name': all_name})

        def preprocess_solution(text):
            s = text.find('class="alignMiddle" ALIGN="left">') + len('class="alignMiddle" ALIGN="left">')
            e = s + text[s:].find('</TD>')
            return text[s:e].replace("&#160;", " ").replace("<BR>", '\n')
        
        clean_sols = []
        for lst in df.solution:
            clean = []
            for sol in lst:
                temp = preprocess_solution(sol)
                if temp != "":
                    clean.append(temp)
            clean_sols.append(clean)
        df['clean_solution'] = clean_sols
        df.to_pickle(os.path.join(self.target_dir, 'topcoder_success.parquet'))
        df_failed.to_pickle(os.path.join(self.target_dir, 'topcoder_failed.parquet'))
        df[['problem', 'clean_solution', 'url', 'name']].to_pickle(os.path.join(self.target_dir, "topcoder_cleaned.pkl"))
        return RawDataset(storage_uris=['file:///{self.target_dir}'])



class TopCoderDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = TopCoder(tempdir, target_dir)
    def download(self):
        self.scraper.scrape()



if __name__=="__main__":

    topcoder = TopCoderDataset('data/', 'data/')
    topcoder.download()

import re
import gzip
from tqdm import tqdm 
import os



def get_repo(url):
    try:
        repo = url.replace("https://api.github.com/repos/", "").split("/")
        repo = "https://api.github.com/repos/" + repo[0] + "/" + repo[1]
        return repo
    except Exception as error:
        print("error logs: ", error)
        return ""


def main():

    regex_list = ['((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)',
                '((http?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)']

    file_mode = 'text'

    files = [os.path.join("ghtorrent_data", x) 
            for x in os.listdir("ghtorrent_data/") if x.endswith(".gz")]

    total_urls = []
    for file_name in files:
        urls = []
        if file_mode == 'text':
            with open(file_name, "r", encoding="ISO-8859-1") as file:
                for line in tqdm(file):
                    for link_regex in regex_list:
                        links = re.findall(link_regex, line)
                        urls += [x[0] for x in links]
        else:
            with gzip.open(file_name,'r') as file:
                for line in tqdm(file):
                    line = line.decode('ISO-8859-1')
                    for link_regex in regex_list:
                        links = re.findall(link_regex, line)
                        urls += [x[0] for x in links]
        
        urls = [x for x in urls if 'https://api.github.com/repos/' in x]
        total_urls += urls

    github_repos = []
    for url in total_urls:
        repo = get_repo(url)
        if repo != "":
            github_repos.append(repo)
    github_repos = set(github_repos)

    with open("GHTorrent_github.txt", "w") as fp:
        for url in github_repos:
            fp.write(url + '\n')

if __name__=="__main__":
    main()
import requests
from bs4 import BeautifulSoup

def get_computing_titles(root_url):
    html = requests.get(root_url).text
    soup = BeautifulSoup(html, "html.parser")

    titles = []
    urls = []
    main_div = soup.find('div', {'id': 'mw-content-text'})
    for a in main_div.find_all('a', href=True):
        if a['href'].startswith('/wiki/'):
            titles.append(a['href'][6:])
            urls.append("https://en.wikibooks.org/" + a['href'])
    return titles, urls

def get_computing_titles_recursive(root_url, depth=2):
    total_titles = []
    total_urls = []
    while depth:
        titles, urls = get_computing_titles(root_url)
        total_titles.extend(titles)
        total_urls.extend(urls)
        for url in urls:
            titles, urls = get_computing_titles(url)
            total_titles.extend(titles)
            total_urls.extend(urls)
        depth -= 1
    return total_titles, total_urls


if __name__=="__main__":
    total_titles = []
    total_urls = []
    total_titles, total_urls = get_computing_titles_recursive("https://en.wikibooks.org/wiki/Department:Computing", 2)
    total_titles = list(set(total_titles))
    total_urls = list(set(total_urls))
    print(total_titles)
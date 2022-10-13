from lib2to3.pgen2 import driver
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
from tqdm import tqdm
import os
import json


def create_driver(default_dir="./books/"):
    chrome_options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": default_dir}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://vi.singlelogin.app/")
    return driver


def login(driver, email="daicait23@gmail.com", password=None):
    em = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="email"]')))
    pas = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]')))
    em.send_keys(email)
    pas.send_keys(password)
    pas.send_keys("\n")
    print("Login successfully")
    return driver


def search(driver, search_term="programming"):
    search_box = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#searchFieldx")))
    search_box.clear()
    search_box.send_keys(search_term+"\n")
    
    def get_all_url_book(driver, search_term):
    # Find all book urls
        current_root_url = "/".join(driver.current_url.split("/")[:3])
        book_urls = []
        for page in range(1, 1000):
            print("Page: {} | Length: {}".format(page, len(book_urls)))
            url = current_root_url + "/s/{}?&page={}".format(search_term.replace(" ", "%20"), page)
            driver.get(url)

            # Create a BeautifulSoup object
            soup = BeautifulSoup(driver.page_source, "html.parser")
            all_url = soup.find_all("tr", class_="bookRow")
            if len(all_url) == 0:
                break
            for a in all_url:
                book_urls.append(current_root_url+a.find("a")["href"])

        return book_urls

    book_urls = get_all_url_book(driver, search_term)
    return driver, book_urls
    

def main(keyword="programming"):
    driver = create_driver()
    driver = login(driver)

    driver, book_urls = search(driver, keyword)
    download(driver, book_urls)


def download(driver, book_urls):
    bar = tqdm(enumerate(book_urls), desc="Downloading", total=len(book_urls))
    for i,book_url in bar:
        driver.get(book_url)
        # click download button
        try:
            download = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body > table > tbody > tr:nth-child(2) > td > div > div > div > div:nth-child(3) > div:nth-child(2) > div.details-buttons-container.pull-left > div:nth-child(1) > div > a")))
            download.click()
        except:
            # sleep 5min and skip this book
            print("Skip this book! Sleep 5min")
            time.sleep(300)
        time.sleep(5)
        # update progress bar
        bar.set_description("Downloading | page: {}".format(i))

    time.sleep(10000)

def count_finished_books(default_dir="./books"):
    while True:
        os.system("clear")
        print("Finished books: ")
        os.system("ls -a {}/*.pdf | wc -l".format(default_dir))
        print("Downloading books....: ")
        os.system("ls -a {}/*.crdownload | wc -l".format(default_dir))
        time.sleep(1)

############################ PARSER PDF ############################
from tika import parser
from PyPDF2 import PdfReader
import ftfy
from tqdm import tqdm

def parser_pdf(pdf_path, type="tika"):
    if type == "tika":
        parsed_pdf = parser.from_file(pdf_path)
        data = parsed_pdf['content'] 
        return ftfy.fix_text(data)
    elif type == "PyPDF2":
        reader = PdfReader(pdf_path)
        num_pages = reader.numPages
        text = ""
        for page in tqdm(range(num_pages)):
            text += ftfy.fix_text(reader.pages[page].extract_text())
        return text

#####################################################################


def get_links_book(url_list):
    driver = create_driver()
    driver.get(url_list)    
    bar = tqdm(range(50))
    for i in bar:
        html = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "html")))
        html.send_keys(Keys.END)
        time.sleep(2)
        bar.set_description("Scrolling page: {}".format(i))

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links = ["https://vi.book4you.org"+x.find("a")['href'] for x in soup.find_all("div", class_="title")]

    driver.get("https://vi.singlelogin.app/")
    driver = login(driver)
    return driver, links

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", type=str, default="programming")
    parser.add_argument("--count", action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--url", type=str, default="")
    args = parser.parse_args()
    if args.download:
        if args.url:
            driver, links = get_links_book(args.url)
            download(driver, links)
        else:
            main(args.keyword)
    else:
        count_finished_books()
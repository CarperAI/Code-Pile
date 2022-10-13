
import requests
import json

from bs4 import BeautifulSoup

url = "https://ebookfoundation.github.io/free-programming-books/books/free-programming-books-langs.html"
response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")
books = soup.find_all("a")
new_books = []
for book in books:
    if book.get("href") is not None and book.get("href").endswith("pdf"):
        new_books.append(
            {
                "title": book.text,
                "url": book["href"]
            }
        )


def download_pdf_from_url(url, path, chunk_size=1024):
    """Download pdf from url and save it in path"""
    response = requests.get(url)
    with open(path, 'wb') as f:
        f.write(response.content)


import os

books_dir = "books"
if not os.path.exists("books"):
    os.mkdir(books_dir)

from tqdm import tqdm
for book in tqdm(new_books):
    # os.system(f"wget -P {books_dir} {book['url']}")
    book_name = book["title"].replace("/", "_")
    if not book_name.endswith(".pdf"):
        book_name += ".pdf"
    try:
        download_pdf_from_url(book["url"], os.path.join(books_dir, book_name))
    except:
        pass
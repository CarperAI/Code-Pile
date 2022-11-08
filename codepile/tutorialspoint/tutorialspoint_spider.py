import scrapy
import json
import pathlib
import re
import os
import sys
import html2text
import zstd
import boto3
import botocore.exceptions
from datetime import datetime

S3_BUCKET = "s-eai-neox"
S3_BUCKET_PATH = "data/codepile/tutorialsites/tutorialspoint/"

s3client = boto3.client('s3')

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class TutorialspointArticleSpider(scrapy.Spider):
    name = "tutorialspoint"
    headers = { }
    user_agent = 'Mozilla/5.0 (compatible; Carper-GooseBot/8000; +https://carper.ai/)'

    # Keep a list of crawled URLs which we can persist across runs
    crawled = set()

    def start_requests(self):
        sitemaps = [
                'https://www.tutorialspoint.com/tp.xml',
                'https://www.tutorialspoint.com/tp1.xml',
        ]

        print('Loading crawled sites list...', flush=True)
        if not os.path.isfile('crawled.txt'):
            # Crawled URLs list not found, see if there's on in s3
            try:
                s3client.download_file(S3_BUCKET, S3_BUCKET_PATH + 'crawled.txt', 'crawled.txt')
                print('got it', flush=True)
            except:
                print('couldnt fetch crawled.txt', flush=True)

        # Load the list of crawled sites into a dict
        try:
            with open('crawled.txt', 'r') as crawled:
                for url in crawled:
                    #self.crawled[url.strip()] = True
                    self.crawled.add(url.strip())
        except:
            print('couldnt open crawled.txt', flush=True)
            pass


        print('Fetching sitemaps...', flush=True);
        for url in sitemaps:
            yield scrapy.Request(url=url, callback=self.parse_sitemap)

        # Uncomment to test parsing of a single article
        #yield scrapy.Request(url="https://www.tutorialspoint.com/program-to-count-number-of-walls-required-to-partition-top-left-and-bottom-right-cells-in-python")

    def parse_sitemap(self, response):
        print('Processing sitemap:', response.url, flush=True)
        response.selector.remove_namespaces()
        print('running xpath selector', flush=True)
        selectors = response.xpath('//loc//text()')
        print('iterating', flush=True)
        added = 0
        for selector in selectors:
            url = selector.get();
            if url in self.crawled:
                print('[' + bcolors.OKBLUE + ' skip ' + bcolors.ENDC + ']', url, flush=True)
            else:
                #print("YIELD", url, flush=True)
                added += 1
                yield scrapy.Request(url=url, callback=self.parse)
        print('Added %d urls of %d total' % (added, len(selectors)), flush=True)

    def parse(self, response):
        res = response.css('.tutorial-content>*')
        htmlsrc = ''
        numclears = 0
        for node in res:
            block = node.get()
            #print('check node', block, node, flush=True)
            if block == '<div class="clear"></div>':
                numclears += 1
            elif numclears == 1:
                # Extract the HTML between the first and second clear divs
                htmlsrc += node.extract()
        data = {
          "text": html2text.html2text(htmlsrc).replace(u"\u00a0", " "),
          "meta": json.dumps({
              "source": "tutorialspoint",
              "url": response.url,
              "title": response.css('.qa_title::text').get(),
              "author": response.css('.qa_author span::text').get(),
              "author_link": response.css('.author-caret a').attrib['href'],
              "categories": response.css('.qa_category>a>span::text').getall(),
              "html": htmlsrc,
              "updated_time": response.css('.qa_answer_dtm::text').get().strip().replace('Updated on ', ''),
              "crawled_time": datetime.now().strftime('%d-%b-%Y %H:%M:%S'),
            })
        }
        yield data
        print('[' + bcolors.OKGREEN + ' save ' + bcolors.ENDC + ']', response.url, flush=True)

        with open('crawled.txt', 'a+') as crawled:
            crawled.write('%s\n' % (response.url))

        links = [link.attrib['href'] for link in response.css('.toc.chapters a')]
        for url in links:
            if url.find('https://') == 0:
                #print('[' + bcolors.OKCYAN + ' link ' + bcolors.ENDC + ']', url, flush=True)
                yield scrapy.Request(url=url)

    def process_item(self, item, spider):
        pass

class JsonS3WriterPipeline:
    def open_spider(self, spider):
        self.current_filename = None
        self.current_file = None

    def close_spider(self, spider):
        self.close_current_file()

    def get_current_filename(self):
        # New file name every hour, for checkpointing
        fname = datetime.now().strftime('crawled-items-%Y-%m-%d_%H.jsonl')
        return fname

    def close_current_file(self):
        if self.current_file:
            print('[' + bcolors.HEADER + 'close ' + bcolors.ENDC + ']', self.current_filename, flush=True)
            old_filename = self.current_filename
            self.current_file.close()
            self.current_file = None
            self.current_filename = None

            zip_filename = old_filename + '.zstd'
            try:
                with open(old_filename, 'rb') as f_in, open(zip_filename, 'wb') as f_out:
                    f_out.write(zstd.ZSTD_compress(f_in.read()))

                os.remove(old_filename)

                s3client.upload_file(zip_filename, S3_BUCKET, S3_BUCKET_PATH + zip_filename)
                s3client.upload_file('crawled.txt', S3_BUCKET, S3_BUCKET_PATH + 'crawled.txt')
                print('[' + bcolors.HEADER + ' sync ' + bcolors.ENDC + '] uploaded s3://%s/%s%s' % (S3_BUCKET, S3_BUCKET_PATH, zip_filename), flush=True)
            except IOError:
                print('[' + bcolors.FAIL  + ' fail ' + bcolors.ENDC + '] writing file failed', self.current_filename, flush=True)
            except botocore.exceptions.NoCredentialsError:
                print('[' + bcolors.FAIL + ' fail ' + bcolors.ENDC + '] syncing file failed: s3://%s%s%s' % (S3_BUCKET, S3_BUCKET_PATH, zip_filename), flush=True)


    def get_file(self):
        current_filename = self.get_current_filename()

        if self.current_file == None or current_filename != self.current_filename:
            self.close_current_file()
            self.current_filename = current_filename
            self.current_file = open(current_filename, 'a+')
            print('[' + bcolors.HEADER + ' open ' + bcolors.ENDC + ']', self.current_filename, flush=True)

        return self.current_file

    def process_item(self, item, spider):
        file = self.get_file()
        line = json.dumps(item) + "\n"
        file.write(line)
        file.flush()
        return item



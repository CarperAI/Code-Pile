import scrapy
import json
import pathlib
import re
import os
import sys
import random

from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError

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

class DiscourseSpider(scrapy.Spider):
    name = "discourse"
    headers = {
            #'Content-Type': 'application/json',
            'Accept': 'application/json'
            }
    user_agent = 'Mozilla/5.0 (compatible; Carper-GooseBot/8000; +https://carper.ai/)'
    download_delay = 1

    scheduler_priority_queue = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
    concurrent_requests_per_domain = 4
    concurrent_requests = 1000


    scrapelatest = False
    scrapetop = False
    scrapecategories = False
    scrapetopics = False
    scrapeindex = True

    failures = {}

    def start_requests(self):
        with open('discourse/index.json', 'r') as indexfd:
            urls = json.loads(indexfd.read())
            random.shuffle(urls)
            for url in urls:
                if self.scrapelatest:
                    yield self.create_request(url, 10)
                if self.scrapetop:
                    yield self.create_request(url + 'top', 10)
                if self.scrapecategories:
                    yield self.create_request(url + 'categories', 10)
                if self.scrapeindex:
                    yield self.create_request(url + 'site', 20)
        
    def create_request(self, url, priority=0):
        return scrapy.Request(url=url, callback=self.parse, headers=self.headers, errback=self.handle_errback, priority=priority + random.randint(0, 10))

    def parse(self, response):
        try:
            jsondata = json.loads(response.body)
        except ValueError as e:
            # log failure
            print("[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tfailed to parse JSON at %s" % (response.url))
            self.write_failure(response.url, 'json_error')
            return

        #print(jsondata)
        #print(response.request.headers)
        m = re.match(r"^(https?://)([^/]+)(/.*?)?(/[^/]+(:?\.json)?)?$", response.url)

        if not m:
            print("Warning: couldn't understand URL", response.url)
            return

        protocol = m.group(1)
        domain = m.group(2)
        urlpath = m.group(3) or ''
        filename = m.group(4) or urlpath

        baseurl = protocol + domain + urlpath

        datapath = 'discourse/%s/%s/' % (domain, urlpath)

        if 'category_list' in jsondata:
            #print ('domain: %s\turlpath: %s\tfilename: %s' % (domain, urlpath, filename))
            self.write_file(datapath, 'categories', response.text)
            for category in jsondata['category_list']['categories']:
                if self.scrapetopics:
                    if 'topic_url' in category and category['topic_url'] is not None:
                        topicurl = '%s%s%s' % (protocol, domain, category['topic_url'])
                        yield self.create_request(topicurl)
                    categoryurl = '%s%s/c/%s/%d' % (protocol, domain, category['slug'], category['id'])
                    yield self.create_request(categoryurl, 10)
                if self.scrapecategories and 'subcategory_ids' in category:
                    for categoryid in category['subcategory_ids']:
                        subcategoryurl = '%s%s/c/%d' % (protocol, domain, categoryid)
                        #print('add subcategory', subcategoryurl)
                        yield self.create_request(subcategoryurl, 10)
                    

        if 'topic_list' in jsondata:
            #print(filename, response.url)
            self.write_file(datapath, filename, response.text)
            if 'more_topics_url' in jsondata['topic_list']:
                nexturl = protocol + domain + jsondata['topic_list']['more_topics_url']
                #print('Add next page URL', nexturl, self.headers)
                yield self.create_request(nexturl, 5)

            if self.scrapetopics:
                topics = jsondata['topic_list']['topics']
                for topic in topics:
                    crawlfname = datapath + '/t/%d.json' % topic['id']
                    if not os.path.isfile(crawlfname): 
                        # TODO - to facilitate continuous crawling, we probably want to check the last crawled time, and refresh if our list data indicates new posts
                        # As implemented, this is just a one-shot crawl that can be resumed. It'll grab new topics, but not refresh any changed ones
                        topicurl = protocol + domain + '/t/%d' % topic['id']
                        #print('New topicurl: ' + topicurl)
                        yield self.create_request(topicurl)
                    #else:
                    #    print('Skipping topic %s, already exists' % topic['slug'])
        if 'post_stream' in jsondata:
            print('[' + bcolors.OKGREEN + ' Saved ' + bcolors.ENDC + ']\t%-40s %-60s' % (domain, jsondata['fancy_title']))
            #crawlfname = datapath + filename + '.json'
            #pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)
            #with open(crawlfname, 'w') as fd:
            #    fd.write(response.text)
            self.write_file(datapath, filename, response.text)
        if 'categories' in jsondata:
            #print('got full list of categories, probably the site index', jsondata)
            self.write_file(datapath, filename, response.text)
    def write_file(self, datapath, filename, contents):
            crawlfname = datapath + filename + '.json'

            pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)

            with open(crawlfname, 'w') as fd:
                fd.write(contents)
    def handle_errback(self, failure):
        if failure.check(HttpError):
            print("["  + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tfailed to fetch URL %s" % (failure.value.response.url))
            self.write_failure(failure.value.response.url, 'http_error')
        elif failure.check(DNSLookupError):
            print("[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tDNS failure resolving %s" % (failure.request.url))
            self.write_failure(failure.request.url, 'dns_error')
        elif failure.check(TimeoutError):
            print("[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tTimed out fetching %s" % (failure.request.url))
            self.write_failure(failure.request.url, 'timeout_error')
    def write_failure(self, url, reason):
        self.failures[url] = reason
        #print("[FAILURE] %s (%s)" % (url, reason))
        self.write_file('discourse/', 'failures', json.dumps(self.failures, indent=2))



class DiscourseSummarySpider(DiscourseSpider):
    scrapelatest = False
    scrapetop = False
    scrapecategories = False
    scrapetopics = False
    scrapeindex = True

class DiscourseTopicSpider(DiscourseSpider):
    scrapelatest = True
    scrapetop = True
    scrapecategories = True
    scrapetopics = True
    scrapeindex = False


def generateCrawlSummary():
    with open('discourse/index.json') as index:
        sites = json.loads(index.read())
        crawlsummary = {
            '_totals': {
                'sites': 0,
                'sites_valid': 0,
                'topic_count': 0,
                'post_count': 0,
                'category_count': 0,
                }
            }
        print('Collecting crawl stats...')
        for site in sites:
            m = re.match(r"^(https?://)([^/]+)(/.*?)$", site)
            if m:
                protocol = m.group(1)
                domain = m.group(2)
                crawlsummary[domain] = {}
                fname = 'discourse/%s/site/site.json' % domain
                crawlsummary['_totals']['sites'] += 1
                crawlsummary[domain]['topic_count'] = 0
                crawlsummary[domain]['post_count'] = 0
                crawlsummary[domain]['category_count'] = 0
                crawlsummary[domain]['categories'] = {}
                if os.path.isfile(fname): 
                    #print('open file', fname)
                    with open(fname, 'r') as sitefd:
                        crawlsummary['_totals']['sites_valid'] += 1
                        sitejson = json.loads(sitefd.read())
                        for category in sitejson['categories']:
                            crawlsummary[domain]['categories'][category['slug']] = category
                            crawlsummary[domain]['topic_count'] = crawlsummary[domain]['topic_count'] + category['topic_count']
                            crawlsummary[domain]['post_count'] = crawlsummary[domain]['post_count'] + category['post_count']
                            crawlsummary[domain]['category_count'] += 1
                crawlsummary['_totals']['topic_count'] += crawlsummary[domain]['topic_count']
                crawlsummary['_totals']['post_count'] += crawlsummary[domain]['post_count']
                crawlsummary['_totals']['category_count'] += crawlsummary[domain]['category_count']
        
        print('Writing...')
        with open('discourse/crawlsummary.json', 'w') as crawlsummaryfd:
            crawlsummaryfd.write(json.dumps(crawlsummary, indent=2))
        print('Done.  Crawl summary written to discourse/crawlsummary.json')


if __name__ == "__main__":
    process = CrawlerProcess()
    if len(sys.argv) == 1:
        print("Usage: %s [index|topics] <sourcefile>" % (sys.argv[0]))
        exit(0)
    crawltype = sys.argv[1]


    if crawltype == 'index':
        process.crawl(DiscourseSpider)
        process.start()
        generateIndexStats()
    elif crawltype == 'topics':
        process.crawl(DiscourseTopicSpider)
        process.start()


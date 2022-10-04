import scrapy
import json
import pathlib
import re
import os
import sys

from scrapy.crawler import CrawlerProcess


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
    concurrent_requests = 100


    scrapelatest = False
    scrapetop = False
    scrapecategories = False
    scrapetopics = False
    scrapeindex = True

    def start_requests(self):
        with open('data/index.json', 'r') as indexfd:
            urls = json.loads(indexfd.read())
            for url in urls:
                if self.scrapelatest:
                    yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)
                if self.scrapetop:
                    yield scrapy.Request(url=url + 'top', callback=self.parse, headers=self.headers)
                if self.scrapecategories:
                    yield scrapy.Request(url=url + 'categories', callback=self.parse, headers=self.headers)
                if self.scrapeindex:
                    yield scrapy.Request(url=url + 'site', callback=self.parse, headers=self.headers)
        

    def parse(self, response):
        jsondata = json.loads(response.body)

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

        datapath = 'data/%s/%s/' % (domain, urlpath)

        if 'category_list' in jsondata:
            print ('domain: %s\turlpath: %s\tfilename: %s' % (domain, urlpath, filename))
            self.writeFile(datapath, 'categories', response.text)
            for category in jsondata['category_list']['categories']:
                if self.scrapetopics:
                    if 'topic_url' in category and category['topic_url'] is not None:
                        topicurl = '%s%s%s' % (protocol, domain, category['topic_url'])
                        yield scrapy.Request(url=topicurl, headers=self.headers, callback=self.parse)
                    categoryurl = '%s%s/c/%s/%d' % (protocol, domain, category['slug'], category['id'])
                    yield scrapy.Request(url=categoryurl, headers=self.headers, callback=self.parse)
                if self.scrapecategories:
                    for categoryid in category['subcategory_ids']:
                        subcategoryurl = '%s%s/c/%d' % (protocol, domain, categoryid)
                        print('add subcategory', subcategoryurl)
                        yield scrapy.Request(url=subcategoryurl, headers=self.headers, callback=self.parse)
                    

        if 'topic_list' in jsondata:
            print(filename, response.url)
            self.writeFile(datapath, filename, response.text)
            if 'more_topics_url' in jsondata['topic_list']:
                nexturl = protocol + domain + jsondata['topic_list']['more_topics_url']
                #print('Add next page URL', nexturl, self.headers)
                yield scrapy.Request(url=nexturl, headers=self.headers, callback=self.parse)

            if self.scrapetopics:
                topics = jsondata['topic_list']['topics']
                for topic in topics:
                    crawlfname = datapath + '/t/%d.json' % topic['id']
                    if not os.path.isfile(crawlfname): 
                        # TODO - to facilitate continuous crawling, we probably want to check the last crawled time, and refresh if our list data indicates new posts
                        # As implemented, this is just a one-shot crawl that can be resumed. It'll grab new topics, but not refresh any changed ones
                        topicurl = protocol + domain + '/t/%d' % topic['id']
                        #print('New topicurl: ' + topicurl)
                        yield scrapy.Request(url=topicurl, headers=self.headers)
                    #else:
                    #    print('Skipping topic %s, already exists' % topic['slug'])
        if 'post_stream' in jsondata:
            print('[Saved] %-40s %-60s' % (domain, jsondata['fancy_title']))
            #crawlfname = datapath + filename + '.json'
            #pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)
            #with open(crawlfname, 'w') as fd:
            #    fd.write(response.text)
            self.writeFile(datapath, filename, response.text)
        if 'categories' in jsondata:
            #print('got full list of categories, probably the site index', jsondata)
            self.writeFile(datapath, filename, response.text)
    def writeFile(self, datapath, filename, contents):
            crawlfname = datapath + filename + '.json'

            pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)

            with open(crawlfname, 'w') as fd:
                fd.write(contents)

class DiscourseTopicSpider(DiscourseSpider):
    scrapelatest = True
    scrapetop = True
    scrapecategories = True
    scrapetopics = True

def generateIndexStats():
    with open('data/index.json') as index:
        sites = json.loads(index.read())
        sitecategories = {}
        for site in sites:
            m = re.match(r"^(https?://)([^/]+)(/.*?)$", site)
            if m:
                protocol = m.group(1)
                domain = m.group(2)
                sitecategories[domain] = {}
                fname = 'data/%s/site/site.json' % domain
                sitecategories[domain]['topic_count'] = 0
                sitecategories[domain]['post_count'] = 0
                sitecategories[domain]['categories'] = {}
                if os.path.isfile(fname): 
                    print('open file', fname)
                    with open(fname, 'r') as sitefd:
                        sitejson = json.loads(sitefd.read())
                        for category in sitejson['categories']:
                            sitecategories[domain]['categories'][category['slug']] = category
                            sitecategories[domain]['topic_count'] = sitecategories[domain]['topic_count'] + category['topic_count']
                            sitecategories[domain]['post_count'] = sitecategories[domain]['post_count'] + category['post_count']
        
        print('Collected category stats, writing...', sitecategories)
        with open('data/sitecategories.json', 'w') as sitecategoriesfd:
            sitecategoriesfd.write(json.dumps(sitecategories))



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


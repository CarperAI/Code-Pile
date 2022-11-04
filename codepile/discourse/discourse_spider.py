import scrapy
import json
import pathlib
import re
import os
import sys
import time
import random
import tarfile, io

from urllib.parse import urlparse

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
    #download_delay = 0

    scheduler_priority_queue = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
    #concurrent_requests_per_domain = 2
    #concurrent_requests = 10


    scrapelatest = False
    scrapetop = False
    scrapecategories = False
    scrapetopics = False
    scrapeindex = True

    usetarballs = False

    failures = {}

    def start_requests(self):
        pathlib.Path('discourse').mkdir(parents=True, exist_ok=True)
        # TODO - need some way of specifying which crawl index file to use
        with open('discourse/index.json', 'r') as indexfd:
            urls = json.loads(indexfd.read())
            random.shuffle(urls)
            for url in urls:
                if self.scrapelatest:
                    yield self.create_request(url, '', 0)
                if self.scrapetop:
                    yield self.create_request(url, 'top', 0)
                if self.scrapecategories:
                    yield self.create_request(url, 'categories', 0)
                if self.scrapeindex:
                    yield self.create_request(url, 'site', 75)
        
    def create_request(self, site, path, priority=0):
        #url = site + path[1:] if site[-1] == '/' and path[0] == '/' else site + path
        url = site + path
        if len(path) > 0 and path[0] == '/':
            siteurl = urlparse(site)
            url = '%s://%s%s' % (siteurl.scheme, siteurl.netloc, path)
        return scrapy.Request(url=url, callback=self.bind_parse_func(site), headers=self.headers, errback=self.handle_errback, priority=priority + random.randint(0, 50))

    def bind_parse_func(self, site):
        def callparse(response):
            return self.parse(site, response)
        return callparse
    def parse(self, site, response):
        try:
            jsondata = json.loads(response.body)
        except ValueError as e:
            # log failure
            print(("%.4f\t[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tfailed to parse JSON at %s") % (time.time(), response.url))
            self.write_failure(response.url, 'json_error')
            return

        #print(jsondata)
        #print(response.request.headers)
        #m = re.match(r"^(https?://)([^/]+)(/.*?)?$", response.url)

        #if not m:
        #    print("Warning: couldn't understand URL", response.url)
        #    return

        #protocol = m.group(1)
        #domain = m.group(2)
        #urlpath = m.group(3)
        #filename = m.group(4) or urlpath

        sitepath = re.sub(r"^https?://(.*)/$", r'\1', site)
        siteroot = sitepath.split('/')[0]

        url = urlparse(response.url)
        protocol = url.scheme + '://'
        domain = url.netloc
        urlpath = url.path or '/'
        if url.query:
            urlpath += '?' + url.query

        if urlpath[-1] == '/':
            urlpath += 'index'

        datapath = 'discourse/%s/%s/' % (domain, urlpath)

        if 'category_list' in jsondata:
            #print ('domain: %s\turlpath: %s\tfilename: %s' % (domain, urlpath, filename))
            self.write_file(domain, urlpath, response.text)
            for category in jsondata['category_list']['categories']:
                if self.scrapetopics:
                    if 'topic_url' in category and category['topic_url'] is not None:
                        topicurl = category['topic_url']
                        crawlroot = siteroot if topicurl[0] == '/' else sitepath
                        crawlfname = 'discourse/%s%s' % (crawlroot, topicurl)
                        #print("check crawlfname", crawlfname)
                        if not os.path.isfile(crawlfname):
                            yield self.create_request(site, topicurl)
                    if len(category['slug']) > 0:
                        categoryurl = '/c/%s' % (category['slug'])
                        # check for cached category page
                        crawlfname = 'discourse/%s%s' % (sitepath, categoryurl)
                        #print("check category cache", crawlfname)
                        if os.path.isdir(crawlfname):
                            # cached file found, parse the on-disk representation to repopulate the queue
                            categoryjson = False
                            categoryslug = category['slug']
                            if len(categoryslug) == 0:
                                categoryslug = "%d-category" % category['id']
                            categoryurl = 'c/%s' % (categoryslug)
                            # check for cached category page
                            crawlroot = siteroot if categoryurl[0] == '/' else sitepath
                            crawlfname = 'discourse/%s/%s/%d' % (crawlroot, categoryurl, category['id'])
                            with open(crawlfname, 'r') as fd:
                                categorycontents = fd.read()
                            #print("cached!'", categorycontents)
                            if categorycontents:
                                fakeresponse = {
                                    'body': categorycontents,
                                    'text': categorycontents,
                                    'url': site + categoryurl[1:]
                                }
                                self.parse(site, fakeresponse)
                        elif os.path.isfile(crawlfname):
                            # cached file found, parse the on-disk representation to repopulate the queue
                            categoryjson = False
                            with open(crawlfname, 'r') as fd:
                                categorycontents = fd.read()
                            if categoryjson:
                                fakeresponse = {
                                    body: categorycontents,
                                    text: categorycontents,
                                    url: site + categoryurl[1:]
                                }
                                print(fakeresponse)
                                #self.parse(site, fakeresponse)

                        else:
                            # not found crawl it
                            yield self.create_request(site, categoryurl, 0)

                if self.scrapecategories and 'subcategory_ids' in category:
                    for categoryid in category['subcategory_ids']:
                        categoryslug = category['slug']
                        if len(categoryslug) == 0:
                            categoryslug = "%d-category" % category['id']
                        subcategoryurl = 'c/%s' % (categoryslug)
                        #print('add subcategory', subcategoryurl)
                        yield self.create_request(site, subcategoryurl, 0)
                    

        if 'topic_list' in jsondata:
            self.write_file(domain, urlpath, response.text)
            if 'more_topics_url' in jsondata['topic_list']:
                nexturl = jsondata['topic_list']['more_topics_url']
                #print('Add next page URL', nexturl, self.headers)
                yield self.create_request(site, nexturl, 0)

            if self.scrapetopics:
                topics = jsondata['topic_list']['topics']
                skips = 0
                for topic in topics:
                    #crawlfname = datapath + '/t/%s/%d' % (topic['slug'], topic['id'])
                    topicurl = 't/%s/%d' % (topic['slug'], topic['id'])
                    crawlfname = 'discourse/%s/%s' % (sitepath, topicurl)
                    #print('check datapath', crawlfname, response.url, site)
                    if not os.path.isfile(crawlfname):
                        # TODO - to facilitate continuous crawling, we probably want to check the last crawled time, and refresh if our list data indicates new posts
                        # As implemented, this is just a one-shot crawl that can be resumed. It'll grab new topics, but not refresh any changed ones
                        #print('New topicurl: ' + topicurl)
                        yield self.create_request(site, topicurl, priority=50)
                    else:
                        skips += 1
                if skips > 0:
                    print(('%.4f\t[' + bcolors.OKBLUE + 'Skipped' + bcolors.ENDC + ']\t%-40s ...skipped %d topics...') % (time.time(), domain, skips))
        if 'post_stream' in jsondata:
            print(('%.4f\t[' + bcolors.OKGREEN + ' Saved ' + bcolors.ENDC + ']\t%-40s %-60s') % (time.time(), domain, jsondata['fancy_title']))
            #crawlfname = datapath + filename + '.json'
            #pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)
            #with open(crawlfname, 'w') as fd:
            #    fd.write(response.text)
            self.write_file(domain, urlpath, response.text)
        if 'categories' in jsondata:
            #print('got full list of categories, probably the site index', jsondata)
            self.write_file(domain, urlpath, response.text)
    def write_file(self, domain, filepath, contents):
        if domain == '' or not self.usetarballs:
            crawlfname = 'discourse/%s%s' % (domain, filepath)
            datapath = os.path.dirname(crawlfname)

            pathlib.Path(datapath).mkdir(parents=True, exist_ok=True)

            with open(crawlfname, 'w') as fd:
                #print("write file", crawlfname)
                fd.write(contents)
        elif False:
            tarballname = 'discourse/%s.tar' % domain
            if len(contents) > 0:
                tarinfo = tarfile.TarInfo(filepath)
                encoded = contents.encode()
                tarinfo.size = len(encoded)
                file = io.BytesIO(encoded)
                try:
                    tar = tarfile.open(tarballname, 'a')
                    tar.addfile(tarinfo, file)
                    tar.close()
                    print("write tarball", tarballname)
                except tarfile.ReadError:
                    print("oh no", filepath)
                    pass

            else:
                print('wtf why', tarballname, filepath)
    def handle_errback(self, failure):
        if failure.check(HttpError):
            print(("%.4f\t["  + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tfailed to fetch URL %s") % (time.time(), failure.value.response.url))
            self.write_failure(failure.value.response.url, 'http_error')
        elif failure.check(DNSLookupError):
            print(("%.4f\t[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tDNS failure resolving %s") % (time.time(), failure.request.url))
            self.write_failure(failure.request.url, 'dns_error')
        elif failure.check(TimeoutError):
            print(("%.4f\t[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tTimed out fetching %s") % (time.time(), failure.request.url))
            self.write_failure(failure.request.url, 'timeout_error')
    def write_failure(self, url, reason):
        self.failures[url] = reason
        #print("[FAILURE] %s (%s)" % (url, reason))
        self.write_file('', 'failures.json', json.dumps(self.failures, indent=2))



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
    try :
        with open('discourse/failures.json') as failurefd:
            failures = json.loads(failurefd.read())
        with open('discourse/index.json') as index:
            sites = json.loads(index.read())
    except FileNotFoundError:
        print(("%.4f\t[ " + bcolors.FAIL + 'ERROR ' + bcolors.ENDC + '] couldn\'t read index or failure files' % (time.time())))
        return
    faildomains = {}
    for failurl in failures:
            m = re.match(r"^(https?://)([^/]+)(/.*?)$", failurl)
            if m:
                protocol = m.group(1)
                domain = m.group(2)
                faildomains[domain] = failures[failurl]

    if failures and sites:
        crawlsummary = {
            '_totals': {
                'sites': 0,
                'sites_valid': 0,
                'topic_count': 0,
                'topic_crawl_count': 0,
                'post_count': 0,
                'category_count': 0,
                }
            }
        print('Collecting crawl stats...')
        for site in sites:
            url = urlparse(site)
            protocol = url.scheme + '://'
            domain = url.netloc
            urlpath = url.path
            if url.query:
                urlpath += '?' + url.query

            crawlsummary[domain] = {}
            tarballname = 'discourse/%s.tar' % domain
            fname = 'discourse/%s%s/site' % (domain, urlpath)
            crawlsummary['_totals']['sites'] += 1
            crawlsummary[domain]['topic_count'] = 0
            crawlsummary[domain]['topic_crawl_count'] = 0
            crawlsummary[domain]['post_count'] = 0
            crawlsummary[domain]['category_count'] = 0
            crawlsummary[domain]['categories'] = {}

            if domain in faildomains:
                crawlsummary[domain]['failure'] = faildomains[domain]

            if os.path.isfile(fname):
                #print('open file', fname)
                with open(fname, 'r') as sitefd:
                    crawlsummary['_totals']['sites_valid'] += 1
                    sitejson = json.loads(sitefd.read())
                    for category in sitejson['categories']:
                        crawlsummary[domain]['categories'][category['slug']] = category
                        crawlsummary[domain]['topic_count'] += category['topic_count']
                        if 'topic_url' in category and category['topic_url'] is not None and category['topic_url'] != '/t/':
                            crawlsummary[domain]['topic_count'] += 1 # include the topic that describes this category
                        crawlsummary[domain]['post_count'] += category['post_count']
                        crawlsummary[domain]['category_count'] += 1
                    topicdir = 'discourse/%s%s/t' % (domain, urlpath)
                    if os.path.isdir(topicdir):
                        entries = os.listdir(topicdir)
                        for entry in entries:
                            topicfname = topicdir + '/' + entry
                            if os.path.isdir(topicfname):
                                crawlsummary[domain]['topic_crawl_count'] += 1
                    crawlsummary['_totals']['topic_crawl_count'] += crawlsummary[domain]['topic_crawl_count']


            elif os.path.isfile(tarballname):
                print("open tarball", tarballname)
                try:
                    tar = tarfile.open(tarballname, 'r')
                    extractfile = (urlpath or '/') + 'site'
                    try:
                        tarreader = tar.extractfile(extractfile)
                        sitejson = json.loads(tarreader.read(6553600))
                        for category in sitejson['categories']:
                            crawlsummary[domain]['categories'][category['slug']] = category
                            crawlsummary[domain]['topic_count'] = crawlsummary[domain]['topic_count'] + category['topic_count']
                            crawlsummary[domain]['post_count'] = crawlsummary[domain]['post_count'] + category['post_count']
                            crawlsummary[domain]['category_count'] += 1
                    except KeyError:
                        pass
                    tar.close()
                except tarfile.ReadError:
                    print("oh no", filepath)
                    pass
            crawlsummary['_totals']['topic_count'] += crawlsummary[domain]['topic_count']
            crawlsummary['_totals']['post_count'] += crawlsummary[domain]['post_count']
            crawlsummary['_totals']['category_count'] += crawlsummary[domain]['category_count']
        
        print('Writing...')
        with open('discourse/crawlsummary.json', 'w') as crawlsummaryfd:
            crawlsummaryfd.write(json.dumps(crawlsummary, indent=2))
        print('Done.  Crawl summary written to discourse/crawlsummary.json')
        print(crawlsummary['_totals'])

def verify_site(site):
    # load site map

    try:
        siteroot = 'discourse/%s' % (site)
        print("try to open site", site, siteroot)
        with open(siteroot + '/site', 'r') as sitefd:
            print("opened")
            sitedata = json.loads(sitefd.read())
            #print(sitedata)
            found = {
                "categories": [],
                "topics": []
            }
            missing = {
                "categories": [],
                "topics": []
            }
            if 'categories' in sitedata and len(sitedata['categories']) > 0:
                # walk through all categories with pagination, and extract all topic urls
                for category in sitedata['categories']:
                    # check if category file exists in cache
                    try:
                        #print(category)
                        categorypath = siteroot + '/c/%s' % (category['slug'])
                        #print("check file?", categorypath)
                        if os.path.isdir(categorypath):
                            categorypath = '%s/%d' % (categorypath, category['id'])

                        with open(categorypath, 'r') as categoryfd:
                            categorydata = json.loads(categoryfd.read())
                            print('found category', category['slug'])
                            found['categories'].append(category['name'])
                            #nextpageurl = categorydata['more_iotems']
                    except FileNotFoundError:
                        #print("no nope", category['slug'], category['name'])
                        missing['categories'].append(category['name'])


                    # compare known topic urls with saved topic urls

                    # output missing urls

            print('FOUND', found)
            print('MISSING', missing)

    except KeyError:
        print("site not found", site)


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


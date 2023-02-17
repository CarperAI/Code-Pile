import scrapy
import json
import pathlib
import re
import os
import sys
import time
import random
import tarfile, io
import multiprocessing as mp

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
    usejsonl = True

    failures = {}

    jsonl_files = {}
    crawled_urls = {}

    def __init__(self, sitelist='all'):
        self.sitelist = sitelist

    def start_requests(self):
        pathlib.Path('discourse').mkdir(parents=True, exist_ok=True)
        # TODO - need some way of specifying which crawl index file to use

        urls = []
        if self.sitelist == 'all':
            with open('discourse/index.json', 'r') as indexfd:
                urls = json.loads(indexfd.read())
        else:
            urls = self.sitelist.split(',')

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
                        #if not os.path.isfile(crawlfname):
                        if not self.has_crawled_topic(siteroot, topicurl):
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
                    #if not os.path.isfile(crawlfname):
                    if not self.has_crawled_topic(siteroot, topicurl):
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
            #self.write_file(domain, urlpath, response.text)
            self.write_topic_jsonl(domain, urlpath, response.text)
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
    def write_topic_jsonl(self, domain, filepath, contents):
        #print('write the topic file!', domain, filepath, contents)
        self.set_topic_crawled(domain, filepath)
        if not domain in self.jsonl_files:
            jsonlpath = 'discourse/%s/%s-topics-raw.jsonl' % (domain, domain)
            self.jsonl_files[domain] = open(jsonlpath, 'a')
        fulljson = '{"domain": "%s", "path": "%s", "contents": %s}\n' % (domain, filepath, contents)
        #print(fulljson)
        jsonl = self.jsonl_files[domain]
        jsonl.write(fulljson)
        jsonl.flush()

    def set_topic_crawled(self, site, topicpath):
        #url = self.get_topic_url(site, topicpath)
        if not site in self.crawled_urls:
            self.crawled_urls[site] = set()
        if topicpath[0] != '/':
            topicpath = '/' + topicpath
        self.crawled_urls[site].add(topicpath)


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

    def get_index(self):
        if not self.index:
            with open('discourse/index.json') as fd:
                sites = json.loads(fd.read())
                self.index = sites

        return self.index

    def get_site_path(self, site, temp=False, fullpath=True):
        sites = self.get_index()
        siteroot = site
        if fullpath:
            for indexedsite in sites:
                if site in indexedsite:
                    idx = indexedsite.find('://')
                    siteroot = indexedsite[idx+3:]

        if temp:
            #return ('%s/%s' % (self.temp_dir, siteroot)).replace('//', '/')
            return os.path.join(self.temp_dir, siteroot)
        return os.path.join('discourse', siteroot)
        #return 'discourse/%s' % (siteroot)
    def get_topic_url(self, site, topicurl):
        url = site + topicurl
        if len(topicurl) > 0 and topicurl[0] == '/':
            siteurl = urlparse(site)
            url = '%s://%s%s' % (siteurl.scheme, siteurl.netloc, topicurl)
        return url
    def get_crawled_topics(self, site):
        if not site in self.crawled_urls:
            print('Loading crawled urls for site: %s' % site)
            self.crawled_urls[site] = set()
            if self.usejsonl:
                jsonlpath = 'discourse/%s/%s-topics-raw.jsonl' % (site, site)
                pathre = re.compile(r'"path": "([^"]+)",')
                try:
                    with open(jsonlpath, 'r') as jsf:
                        for line in jsf:
                            # Yeah I'm parsing json with a regex, what are you gonna do about it? :D
                            # (it's an order of magnitude faster than blowing up the whole json just to extract this one value here)
                            m = pathre.search(line)
                            if m:
                                self.crawled_urls[site].add(m[1])
                except:
                    # no crawled urls found
                    pass
        return self.crawled_urls[site]

    def has_crawled_topic(self, site, topicurl):
        if self.usejsonl:
            #url = self.get_topic_url(site, topicurl)
            if topicurl[0] != '/':
                topicurl = '/' + topicurl
            crawled_urls = self.get_crawled_topics(site)
            return topicurl in crawled_urls
            return False
        else:
            sitepath = re.sub(r"^https?://(.*)/$", r'\1', site)
            siteroot = sitepath.split('/')[0]
            crawlroot = siteroot if topicurl[0] == '/' else sitepath
            crawlfname = 'discourse/%s%s' % (crawlroot, topicurl)
            return os.path.isfile(crawlfname)




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

def get_domain_from_site(site):
    url = urlparse(site)
    return url.netloc

def get_additional_posts(site, sitepath):
    additionalposts = {}
    #domain = get_domain_from_site(site)
    #additionalpath = os.path.join(sitepath, site + '-additional-posts.jsonl')
    additionalpath = os.path.join('discourse', site, site + '-additional-posts.jsonl')
    #print('load additional posts file', additionalpath)
    if os.path.isfile(additionalpath):
        with open(additionalpath, 'r') as apfd:
            for line in apfd:
                posts = json.loads(line)
                if not posts['topicid'] in additionalposts:
                    additionalposts[posts['topicid']] = []
                additionalposts[posts['topicid']].extend(posts['posts'])
    return additionalposts

def get_missing_posts(site, sitepath):
    #print('Spawn process', site, sitepath, flush=True)

    domain = get_domain_from_site(site)

    separator = 'post_ids[]='
    topicroot = os.path.join(sitepath, 't')
    jsonlpath = os.path.join(sitepath, domain + '-topics-raw.jsonl')
    topicfiles = []
    newurls = []

    if os.path.isfile(jsonlpath):
        additionalposts = get_additional_posts(site, sitepath)
        with open(jsonlpath, 'r') as jsonlfd:
            for line in jsonlfd:
                rawdata = json.loads(line)
                data = rawdata['contents']
                if data['posts_count'] > len(data['post_stream']['posts']):
                    topicurl = site + 't/%d/posts?' % (data['id'])
                    additional = [] 
                    if data['id'] in additionalposts:
                        xdata = json.loads(additionalposts[data['id']])
                        additional = [post['id'] for post in xdata]

                    posts = [post['id'] for post in data['post_stream']['posts']]
                    missing = []
                    if 'stream' in data['post_stream']:
                        missing = [x for x in data['post_stream']['stream'] if x not in posts and x not in additional]
                    #print(topicurl)
                    #print('  - ', len(missing), missing)
                    #print('  - ', len(data['post_stream']['stream']), data['post_stream']['stream'])
                    #print('  - ', len(posts), posts)
                    step = 100
                    for i in range(0, len(missing), step):
                        url = topicurl + separator + ('&' + separator).join(map(str, missing[i:i+step]))
                        #print('get the url', url)
                        topicpath = 't/%d/' % (data['id'])
                        if 'slug' in data and data['slug'] != None:
                            topicpath = 't/%s/' % (data['slug'])
                        #yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site, topicpath), headers=self.headers, errback=self.handle_errback)
                        newurls.append(url)
                        #print('new url: ' + url)
    else:
        # old file-based approach
        if os.path.isdir(topicroot):
            # iterate all topics per site
            for topicdir in os.listdir(topicroot):
                #print('e', topicdir)

                topicfilepath = os.path.join(topicroot, topicdir)
                if os.path.isdir(topicfilepath):
                    for topicfile in os.listdir(topicfilepath):
                        if topicfile == 'additional_posts.json':
                            continue
                        topicpath = os.path.join(topicfilepath, topicfile)
                        topicfiles.append(topicpath)
                elif os.path.isfile(topicfilepath):
                    topicfiles.append(topicfilepath)
        #print('read topic files...')
        for topicfile in topicfiles:
            print('.', end='', flush=True)
            with open(topicfile) as tf:
                try:
                    data = json.loads(tf.read())
                except:
                    print('error parsing JSON', topicfile)
                    continue

                if data['posts_count'] > len(data['post_stream']['posts']):
                    topicurl = site + 't/%d/posts?' % (data['id'])
                    additionalpostspath = sitepath + 't/%s/additional_posts.json' % (data['slug'] or str(data['id']))
                    additional = []
                    if os.path.isfile(additionalpostspath):
                        with open(additionalpostspath, 'r') as apfd:
                            xdata = json.loads(apfd.read())
                            additional = [post['id'] for post in xdata]

                    #print('my additionals', data['slug'], data['id'], additional)
                    posts = [post['id'] for post in data['post_stream']['posts']]
                    missing = []
                    if 'stream' in data['post_stream']:
                        missing = [x for x in data['post_stream']['stream'] if x not in posts and x not in additional]
                    #print(topicurl)
                    #print('  - ', len(missing), missing)
                    #print('  - ', len(data['post_stream']['stream']), data['post_stream']['stream'])
                    #print('  - ', len(posts), posts)
                    step = 100
                    for i in range(0, len(missing), step):
                        url = topicurl + separator + ('&' + separator).join(map(str, missing[i:i+step]))
                        #print('get the url', url)
                        topicpath = 't/%d/' % (data['id'])
                        if 'slug' in data and data['slug'] != None:
                            topicpath = 't/%s/' % (data['slug'])
                        #yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site, topicpath), headers=self.headers, errback=self.handle_errback)
                        newurls.append(url)
                # read topic json
                # for any topics with numposts > the number of posts we have, make a list of missing ones
                # fetch posts for each topic, in batches of 100
                # save to additional_posts.json
    #print('ok now yield them', newurls)
    return newurls


class DiscourseAdditionalPostsSpider(DiscourseSpider):
    scrapelatest = False
    scrapetop = False
    scrapecategories = False
    scrapetopics = False
    scrapeindex = False
    missingpostsfile = 'missing-posts.txt'

    index = False
    sitetopicpaths = {}

    def __init__(self, site='all', **kwargs):
        super().__init__(**kwargs)  # python3
        self.site = site

    def start_requests(self):
        #for s in self.settings:
        #    print(' - ', s, self.settings[s])
        #return
        # iterate all sites
        if not os.path.isfile(self.missingpostsfile):
            print('Missing posts file hasn\'t been built yet, building...')
            self.gather_missing_posts(self.site)

        #blah = self.load_site_topic_paths('discourse.threejs.org')
        #print('cool', blah)

        #try:
        if True:
            print('Load missing posts file')
            additionalposts = {}
            lastsite = None
            lastcount = 0
            allurls = []
            with open(self.missingpostsfile, 'r') as mpfd:
                for url in mpfd:
                    if self.site != 'all' and self.site not in url:
                        continue
                    urlparts = url.split('/')
                    site = urlparts[2]
                    topicid = 0
                    for i in range(2, len(urlparts)):
                        if urlparts[i] == 't':
                            topicid = int(urlparts[i+1])
                            break
                    if site != lastsite:
                        print('%d new urls (%d total)\nnew site: %-70s' % (len(allurls) - lastcount, len(allurls), site), end='', flush=True)
                        lastsite = site
                        lastcount = len(allurls)
                        sitepath = self.get_site_path(site)
                        additionalposts = get_additional_posts(site, sitepath)

                    #print(site, topicid)
                    #topicpath = self.get_site_topic_path(site, topicid)
                    #print(' - ', url, topicpath)
                    #print('site has additional posts?', site, additionalposts[site])
                    if not topicid in additionalposts:
                        #yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site, topicid), headers=self.headers, errback=self.handle_errback, priority=random.randint(1, 10000))
                        allurls.insert(random.randint(0, len(allurls)), (url, site, topicid))
                    #else:
                    #    print('skip a topic, already got it: %s %d', site, topicid)
                    #yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site))

            print('Loaded %d total sites, start crawling...' % (len(allurls)))
            for url,site,topicid in allurls:
                yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site, topicid), headers=self.headers, errback=self.handle_errback, priority=random.randint(1, 10000))
        #except:
        #    print('oh no')

    def gather_missing_posts(self, site):
        if site == 'all':
            sites = self.get_index()
        else:
            sites = site.split(',')
        #print('Iterate sites', sites)
        pool = mp.Pool(processes=32)

        jobs = []
        for site in sites:
            sitepath = self.get_site_path(site)
            job = pool.apply_async(get_missing_posts, args=(site, sitepath, ), callback=self.update_missing_posts)
            jobs.append(job)
            #print('add site', site, job)

        print('Waiting for jobs...')
        pool.close()
        pool.join()
        for job in jobs:
            job.get()
        print('done');
        #yield scrapy.Request(url='https://www.google.com')

    def bind_parse_additional_posts_func(self, site, topicid):
        def callparse(response):
            return self.parse_additional_posts(site, topicid, response)
        return callparse

    def parse_additional_posts(self, site, topicid, response):
        try:
            jsondata = json.loads(response.body)
        except ValueError as e:
            # log failure
            print(("%.4f\t[" + bcolors.WARNING + 'WARNING' + bcolors.ENDC + "]\tfailed to parse JSON at %s") % (time.time(), response.url))
            self.write_failure(response.url, 'json_error')
            return

        sitepath = self.get_site_path(site)
        #topicpath = self.get_topic_path(site, topicid)
        newposts = jsondata['post_stream']['posts']
        #print('parse it', site, topicpath, sitepath, jsondata)
        use_jsonl = True

        if use_jsonl:
            additionalpath = os.path.join('discourse', site, site + '-additional-posts.jsonl')
            print('write to file', sitepath, site, additionalpath, len(newposts))
            try:
                with open(additionalpath, 'a') as apfd:
                    data = {
                        'topicid': topicid,
                        'posts': newposts
                        }
                    apfd.write(json.dumps(data) + '\n')
            except FileNotFoundError:
                print('Couldn\'t write to additional posts file: %s' % (additionalpath))
        else:
            topicpath = self.get_site_topic_path(site, topicid)
            try:
                with open(topicpath + 'additional_posts.json', 'r+') as tfd:
                    currentdata = json.loads(tfd.read())
                    newposts.extend(currentdata)
                    print('append', site, topicpath)
                    tfd.seek(0)
                    tfd.write(json.dumps(newposts))
                    tfd.truncate()
            except FileNotFoundError:
                with open(topicpath + 'additional_posts.json', 'w') as tfd:
                    print('write new', site, topicpath)
                    tfd.write(json.dumps(newposts))
    def update_missing_posts(self, newurls):
        #print('UPDATE URLS', newurls)

        with open(self.missingpostsfile, 'a+') as fd:
            for url in newurls:
                #yield scrapy.Request(url=url, callback=self.bind_parse_additional_posts_func(site, topicpath), headers=self.headers, errback=self.handle_errback)
                fd.write(url + '\n')

    def get_site_topic_path(self, site, topicid):
        if not site in self.sitetopicpaths:
            self.sitetopicpaths[site] = self.load_site_topic_paths(site)
        #print(self.sitetopicpaths)
        if site in self.sitetopicpaths and topicid in self.sitetopicpaths[site]:
            return self.sitetopicpaths[site][topicid]
        return None

    def load_site_topic_paths(self, site):
        sitetopicpaths = {}
        sitepath = self.get_site_path(site)
        topicroot = sitepath + 't/'
        print('loading topics for site', site)
        for fname in os.listdir(topicroot):
            topicpath = topicroot + fname
            #topicfilepath = '%s/%d' % (topicpath, topicid)

            if os.path.isdir(topicpath):
                for fname2 in os.listdir(topicpath):
                    if fname2.isnumeric():
                        topicpath += '/'
                        sitetopicpaths[int(fname2)] = topicpath
                        #print(' - %s => %s' % (fname2, topicpath))
            else:
                sitetopicpaths[int(fname)] = topicpath
                #print(' - %s => %s' % (fname, topicpath))
        return sitetopicpaths

def generateCrawlSummary():
    try:
        with open('discourse/index.json') as index:
            sites = json.loads(index.read())
    except FileNotFoundError:
        print(("%.4f\t[ " + bcolors.FAIL + 'ERROR ' + bcolors.ENDC + '] couldn\'t read index file' % (time.time())))
        return

    sitelicenses = {}
    try:
        with open('discourse/licenses.tsv') as licensesfd:
            for line in licensesfd:
                site, sitelicense = line.strip().split('\t')
                sitelicenses[site] = sitelicense
        print(("%.4f\t[ " + bcolors.OKGREEN + 'SUCCESS' + bcolors.ENDC + '] loaded licenses file') % (time.time()))
    except FileNotFoundError:
        print(("%.4f\t[ " + bcolors.WARNING + 'WARNING' + bcolors.ENDC + '] couldn\'t read licenses file') % (time.time()))
        return


    failures = []
    try:
        with open('discourse/failures.json') as failurefd:
            failures = json.loads(failurefd.read())
    except FileNotFoundError:
        print(("%.4f\t[ " + bcolors.WARNING + 'WARNING' + bcolors.ENDC + '] couldn\'t read failure file') % (time.time()))
        failures = []

    faildomains = {}
    for failurl in failures:
            m = re.match(r"^(https?://)([^/]+)(/.*?)$", failurl)
            if m:
                protocol = m.group(1)
                domain = m.group(2)
                faildomains[domain] = failures[failurl]

    if sites:
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
        print('Collecting crawl stats', end='', flush=True)
        for site in sites:
            print('.', end='', flush=True)
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
            if site in sitelicenses:
                crawlsummary[domain]['license'] = sitelicenses[site]
            else:
                crawlsummary[domain]['license'] = 'UNKNOWN'

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
                    jsonlpath = 'discourse/%s/%s-topics-raw.jsonl' % (domain, domain)
                    if os.path.isdir(topicdir):
                        entries = os.listdir(topicdir)
                        for entry in entries:
                            topicfname = topicdir + '/' + entry
                            if os.path.isdir(topicfname):
                                crawlsummary[domain]['topic_crawl_count'] += 1
                    elif os.path.isfile(jsonlpath):
                        #print('Read jsonl file: %s' % jsonlpath)
                        with open(jsonlpath, 'r') as fd:
                            for line in fd:
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
        print('done', flush=True)
        
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


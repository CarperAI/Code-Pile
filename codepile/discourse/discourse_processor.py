from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
import gzip
import zstd
import os
import shutil
import hashlib
import tarfile
import zipfile
import json
import html2text
from multiprocessing import Pool
import multiprocessing as mp
import boto3
import botocore.exceptions
import time
import random
import traceback, sys
import urllib.request
import re
import pathlib

S3_BUCKET = "s-eai-neox"
S3_BUCKET_PATH = "data/codepile/discourse/"

s3client = boto3.client('s3')

text_maker = html2text.HTML2Text()
text_maker.unicode_snob = True
text_maker.ignore_links = True
text_maker.ignore_images = True
text_maker.images_to_alt = False
text_maker.inline_links = True
text_maker.mark_code = True
text_maker.escape_snob = False
text_maker.open_quote = '"'
text_maker.close_quote = '"'
text_maker.body_width = 0
text_maker.single_line_break = True
text_maker.use_automatic_links = True

regex_topic = re.compile('/t/([^/]+)/(\d+)')
regex_additional = re.compile('/t/([^/]+)/additional_posts.json')

class DiscourseProcessor(Processor):
    def __init__(self, temp_dir, data_dir):
        self.data_dir = data_dir
        self.temp_dir = temp_dir
        self.index = False
        self.batchsize = 250

    def analyze(self, site):
        print('ok cool lets go', site)

        # Potential stats for Sites:
        # - total number of categories
        # - total number of topics
        # - crawled topics
        # - total number of posts
        # - category tags

        # Potential stats for Topics:
        # - how many Posts? (replies)
        # - what's the total length (word_count)?
        # - number of likes?
        # - has accepted answer?
        # - is it pinned?
        # - how many participants?
        # - how many posts have the author and participants made across the whole site (n00bs or experts)?
        # - how many links does the post contain?
        # - how many attachments?
        # - how old is the topic?
        # - how long has it been active (time between first and most recent posts)?

        # Potential stats for Posts:
        # - incoming link count
        # - number of reads
        # - score
        # - number of links
        # - is from admin?
        # - accepted answer?


        pass

    def process(self, sites, *args, **kwargs):
        os.chdir(self.data_dir)
        sitelist = self.parse_site_list(sites)

        for site in sitelist:
            checksums = self.load_checksums()

            zipfilename = self.get_zip_name(site)
            zipfilepath = self.get_zip_path(site)

            print('verifying zipfile', zipfilepath)
            # check if site.tar.gz exists and has the expected checksum
            if self.verify_checksum(site):
                # extract site.tar.gz to tempdir
                print('extracting zipfile', zipfilepath)
                extracted = self.extract_site_zip(site)
                self.convert_site(site)
                if extracted:
                    self.cleanup_site_temp(site)
            else:
                print('ERROR: checksum doesn\'t match: %s' % (zipfilepath))

        return

    def get_zip_name(self, site):
        return '%s.tar.gz' % (site)

    def get_zip_path(self, site):
        zipfilename = self.get_zip_name(site)
        return 'discourse/%s' % (zipfilename)

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
        return os.path.join(self.data_dir, 'discourse', siteroot)
        #return '%s/discourse/%s' % (self.data_dir, siteroot)

    def get_index(self):
        if not self.index:
            with open('%s/discourse/index.json' % (self.data_dir)) as fd:
                sites = json.loads(fd.read())
                self.index = sites

        return self.index

    def parse_site_list(self, site, fullurl=False):
        sitelist = []
        if site == 'all':
            with open('discourse/index.json', 'r') as indexfd:
                urls = json.loads(indexfd.read())
                for url in urls:
                    if fullurl:
                        sitelist.append(url)
                    else:
                        parts = url.split('/')
                        sitelist.append(parts[2])
        else:
            sitelist = site.split(',')
        return sitelist

    def compress(self, site):
        sitelist = self.parse_site_list(site)

        zippool = Pool(processes=max(1, os.num_cpus() - 1))
        for site in sitelist:
            sitepath = self.get_site_path(site)
            if os.path.isdir(sitepath):
                # spawn zip process
                job = zippool.apply_async(self.compress_site, args=[site])

        zippool.close()
        zippool.join()

        print('Updating checksums')
        checksums = self.load_checksums()
        md5pool = Pool(processes=min(1, os.num_cpus()-1))
        for site in sitelist:
            md5pool.apply_async(self.checksum_site, args=[site], callback=self.update_checksum)

        md5pool.close()
        md5pool.join()
        self.save_checksums()

    def get_topic_posts(self, site, topicid, postids):
        urlbase = '%s/t/%d/posts' % (site, topicid) + '?'
        separator = 'post_ids[]='
        step = 100

        sitepath = self.get_site_path(site)
        #with open(sitepath + '/t/')
        for i in range(0, len(postids), step):
            url = urlbase + separator + ('&' + separator).join(map(str, postids[i:i+step]))
            print('get the url', url)
        #post_ids[]=' + '&post_ids[]='.join(map(str, postids))



    def compress_site(self, site):
        zipfilepath = self.get_zip_path(site)
        cwd = os.getcwd()
        #os.chdir(self.data_dir + '/discourse')
        zipfile = tarfile.open(zipfilepath, 'w:gz', dereference=True)
        #print('Creating zip:', zipfilepath)
        os.chdir('discourse')
        zipfile.add(site)
        zipfile.close()
        os.chdir(cwd)
        print('Created zip: %s%s' % (self.data_dir, zipfilepath))
        return True

    def extract_site_zip(self, site):
        zipfilepath = self.get_zip_path(site)
        if not os.path.isdir(self.temp_dir + '/' + site):
            print('Extracting', zipfilepath, self.temp_dir)
            zipfile = tarfile.open(zipfilepath)

            zipfile.extractall(self.temp_dir)
            zipfile.close()
            print('done')
            return True
        else:
            print('Dir already existed', self.temp_dir + '/' + site)
        return False

    def cleanup_site_temp(self, site):
        sitedir = os.path.join(self.temp_dir, site)
        if os.path.isdir(sitedir):
            print('Cleaning up temp dir', sitedir)
            shutil.rmtree(sitedir)

    def convert_site(self, site):
        sitepath = self.get_site_path(site)
        sitepath_tmp = self.get_site_path(site, True)
        jsonl_path = sitepath_tmp[0:-1] + '.jsonl'
        print('heyo', sitepath, sitepath_tmp, jsonl_path)
        with open(jsonl_path, 'w') as outfile:
            def blargh(f):
                self.convert_callback(f, outfile)
            pool = Pool(processes=min(1, os.num_cpus() - 1))
            jobs = []
            batch = []
            topicroot = os.path.join(sitepath_tmp, 't')
            for topicdir in os.listdir(topicroot):
                #print('e', topicdir)

                topicfilepath = os.path.join(topicroot, topicdir)
                if os.path.isdir(topicfilepath):
                    for topicfile in os.listdir(topicfilepath):
                        if topicfile != 'additional_posts.json':
                            topicpath = os.path.join(topicfilepath, topicfile)
                            batch.append(topicpath)
                elif os.path.isfile(topicfilepath) and topicdir != 'additional_posts.json':
                    batch.append(topicfilepath)

                if len(batch) >= self.batchsize:
                    job = pool.apply_async(self.convert_site_files, args=[batch], callback=blargh)
                    jobs.append(job)
                    batch = []

            if len(batch) > 0:
                job = pool.apply_async(self.convert_site_files, args=[batch], callback=blargh)
                jobs.append(job)

            #for job in jobs:
            #    job.get()

            #print(topicfiles)
            #pool.map(self.convert_site_file, topicfiles)
            pool.close()
            pool.join()
        print('done', flush=True)
        print('compressing... ', end='', flush=True)
        zstdpath = self.get_site_path(site, False, False) + '.jsonl.zstd'
        with open(jsonl_path, 'rb') as f_in, open(zstdpath, 'wb') as f_out:
            f_out.write(zstd.ZSTD_compress(f_in.read()))
        print(zstdpath, flush=True)


    def convert_site_files(self, files):
        converted = []
        #print('CONVERT BATCH:', files)
        for file in files:
            try:
                newdata = self.convert_site_file(file)
                converted.append(newdata)
            except Exception as e:
                print('oh no', e)
                self.log_process_failure(file, 'exception', str(e))
                traceback.print_exception(*sys.exc_info())

        #print('converted: %d of %d' % (len(converted), len(files)))
        return converted

    def convert_site_file(self, file):
        with open(file, 'r') as fd:
            topic = json.loads(fd.read())
            poststream = topic['post_stream']

            #if 'nsfw' in topic['tags'] or 'NSFW' in topic['tags']:
            #    # TODO - there should really be a more intelligent and unified way of filtering results, and we should be logging whenever we cull results from the dataset
            #    return False

            posts = {}
            for post in poststream['posts']:
                posts[post['id']] = post

            stream = []
            if 'stream' in poststream:
                stream = poststream['stream']
            text = ''
            authors = []

            basedir = os.path.dirname(file)
            additionalposts = os.path.join(basedir, 'additional_posts.json')
            if os.path.isfile(additionalposts):
                with open(additionalposts, 'r') as apfd:
                    data = json.loads(apfd.read())
                    for d in data:
                        posts[d['id']] = d

            replies = [
                'To which %s replied\n\n    %s',
                'Then %s said\n\n    %s',
                'So %s replied\n\n    %s',
                '%s replied\n\n    %s',
                'After which, %s added\n\n    %s',
            ]

            if topic['reply_count'] < 1:
                return False;

            postauthor = ''
            authors = []

            missing = []
            for id in stream:
                post = None

                # get post data
                if id in posts:
                    post = posts[id]
                    #print('found it', id)

                # process post data
                if post:
                    author = post['username']
                    #if 'name' in post and post['name'] != '' and post['name'] != None:
                    #    author += ' (' + post['name'] + ')'
                    if author == None:
                        author = 'UnknownUser'

                    if not author in authors:
                        authors.append(author)
                    anonymized_author = 'User%d' % (authors.index(author) + 1)
                    contents = text_maker.handle(post['cooked']).replace('\u2019', "'").strip()

                    for i in range(0, len(authors)):
                        contents = contents.replace('@' + authors[i], '@User%d' % (i + 1))


                    if text == '':
                        text = '%s: %s\n%s\n' % (anonymized_author, topic['title'], contents)
                        postauthor = author
                    else:
                        text += '%s: %s\n' % (anonymized_author, contents)
                else:
                    text += '( there was a missing post )\n\n'
                    hasmissing = True
                    missing.append(id)
                    #print(post)
                    #print('ERROR: missing post', id, post['reply_count'], topic['topic_id'], topic['reply_count'], topic['like_count'])
                    #return False;
                    pass

            if len(missing) > 0:
                self.log_process_failure(file, 'missing', ' '.join(map(str, missing)))
            topictags = ''
            topicimageurl = ''

            if 'tags' in topic:
                topictags = ' '.join(topic['tags'])
            if 'image_url' in topic:
                topicimageurl = topic['image_url'],

            newdata = {
                    "text": text,
                    "meta": {
                        "source": "discourse",
                        "url": file.replace(self.temp_dir, 'https://'),
                        "title": topic['title'],
                        "views": topic['views'],
                        "has_accepted_answer": 'accepted_answer' in topic,
                        "word_count": topic['word_count'],
                        "like_count": topic['like_count'],
                        "reply_count": topic['reply_count'],
                        "participant_count": topic['participant_count'],
                        "posts": topic['posts_count'],
                        "tags": topictags,
                        #"author": postauthor,
                        #"participants": ', '.join(authors),
                        "image_url": topicimageurl,
                        "created_at": topic['created_at'],
                        "last_posted_at": topic['last_posted_at'],

                    }
            }
            #print(newdata)
            #print('=' * 20 + '\n\n' + text)
            return newdata
        return False

    def convert_callback(self, returnval, outfile):
        for line in returnval:
            if line:
                print('.', end='', flush=True)
                outfile.write(json.dumps(line) + '\n')
            else:
                print(' ', end='', flush=True)

    def log_process_failure(self, file, failtype, err):
        with open('log-' + failtype + '.txt', 'a') as log:
            log.write(file.replace(self.temp_dir, '') + ': ' + err + '\n')

    def load_checksums(self):
        try:
            self.checksums = {}
            with open('discourse/checksums.txt', 'r') as csfd:
                #print(lines)
                for line in csfd:
                    #print(line)
                    parts = line.replace(r'  ', ' ').split(' ')
                    #print(parts)
                    self.checksums[parts[1].strip()] = parts[0].strip()
        except:
            print("couldn't load checksums")
            pass
        return self.checksums

    def save_checksums(self):
        #try:
            checksums = ''
            for site in self.checksums:
                checksums += "%s %s\n" % (self.checksums[site], site)
            with open('discourse/checksums.txt', 'w') as csfd:
                csfd.write(checksums)
                print(checksums)
        #except:
        #    print("couldn't save checksums")



    def verify_checksum(self, site):
        zipfilename = self.get_zip_name(site)
        zipfilepath = self.get_zip_path(site)

        if os.path.isfile(zipfilepath):
            #print('got the zip', zipfilepath, site)

            # validate site.tar.gz checksum
            #print(self.checksums)
            if zipfilename in self.checksums:
                expectedchecksum = self.checksums[zipfilename]
                actualchecksum = self.checksum_file(zipfilepath)
                #print('expected: %s\tactual: %s' % (expectedchecksum, actualchecksum))
                return expectedchecksum == actualchecksum
        return False 

    def update_checksum(self, sitechecksum):
        for site in sitechecksum:
            zipfilename = self.get_zip_name(site)
            self.checksums[zipfilename] = sitechecksum[site]

    def checksum_site(self, site):
        zipfilepath = self.get_zip_path(site)
        checksum = {}
        checksum[site] = self.checksum_file(zipfilepath)
        return checksum

    def checksum_file(self, filepath):
        return hashlib.md5(open(filepath, 'rb').read()).hexdigest()

    def sync(self, sites):
        sitelist = self.parse_site_list(sites)
        for site in sitelist:
            sitepath = self.get_site_path(site)
            zip_filename = sitepath + '.jsonl.zstd'
            s3_filename = site + '.jsonl.zstd'
            try:
                s3client.upload_file(zip_filename, S3_BUCKET, S3_BUCKET_PATH + s3_filename)
                print('uploaded: s3://%s%s%s' % (S3_BUCKET, S3_BUCKET_PATH, s3_filename), flush=True)
            except botocore.exceptions.NoCredentialsError:
                print('syncing file failed: s3://%s%s%s' % (S3_BUCKET, S3_BUCKET_PATH, s3_filename), flush=True)
            except boto3.exceptions.S3UploadFailedError:
                print('upload failed: s3://%s%s%s' % (S3_BUCKET, S3_BUCKET_PATH, s3_filename), flush=True)

    def get_license_file(self):
        if not self.licensefile:
            self.licensefile = open('%s/discourse/licenses.tsv' % (self.data_dir), 'w')
        return self.licensefile

    def close_license_file(self):
        if self.licensefile:
            self.licensefile.close()

    def get_license(self, sites):
        self.licensecount = 0
        self.licensefile = None
        self.foo = 0
        try:
            sitelist = self.parse_site_list(sites, True)
            #print(sitelist)
                #pool = Pool(processes=1) # FIXME shouldn't be hardcoded
            with mp.pool.ThreadPool(20) as pool:
                for site in sitelist:
                    print('#' * 10)
                    job = pool.apply_async(self.fetch_and_process_license, callback=self.handle_license, args=(site, ))
                    #self.handle_license(self.fetch_and_process_license(site))
                print('waiting')
                pool.close()
                pool.join()
                print('ok')
        except Exception as e:
            print('eek', e)
        self.close_license_file()
        print('got a bunch of licenses', self.licensecount, len(sitelist))

    def fetch_and_process_license(self, site):
        try:
            #print(('=' * 10) + site)
            sitepath = self.get_site_path(site)
            tospath = sitepath + 'tos'

            contents = None
            licensetype = 'UNKNOWN'

            if os.path.isfile(tospath):
                with open(tospath, 'r') as f:
                    contents = f.read()
                    #print('from file', site)
            if not contents or len(contents) == 0:
                tosurl = site
                if site[-1] != '/':
                    tosurl += '/'
                tosurl += 'tos'
                req = urllib.request.Request(tosurl, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'})
                with urllib.request.urlopen(req, timeout=5) as res:
                    contents = res.read().decode('utf-8')
                    #print('from url', site)
                    if not os.path.isdir(sitepath):
                        os.makedirs(sitepath)
                    with open(tospath, 'w') as f:
                        f.write(contents)
                #print(contents)

            if contents:
                m = re.findall(r'https?://(:?www\.)?creativecommons\.org/licenses/([^/">]+)', contents)
                if m:
                    licensetype = 'cc-' + m[0][1]
                elif re.search(r'https?://(:?www\.)?creativecommons\.org/publicdomain/zero', contents):
                    licensetype = 'cc0'
                elif re.search('Content you submit to the forum belongs to you, and you decide what permission to give others for it.', contents):
                    licensetype = 'unlicensed'
            return (site, licensetype)
        except Exception as e:
            print('failed to fetch license', site + 'tos', e)
            licensetype = 'FAILED'
        return (site, licensetype)

    def handle_license(self, licensedata):
        print('got a license', licensedata)
        self.licensecount += 1
        licensefile = self.get_license_file()
        if licensefile:
            licensefile.write('%s\t%s\n' % (licensedata[0], licensedata[1]))
            licensefile.flush()
    def handle_license_error(self, wat):
        print('huh?', wat)
    def convert_raw(self, site):
        sitelist = self.parse_site_list(site)
        pool = Pool(processes=64)
        jobs = []

        for site in sitelist:
                    #self.convert_raw_site(site)
            jobs.append(pool.apply_async(self.convert_raw_site, args=[site]))

        #print(topicfiles)
        #pool.map(self.convert_site_file, topicfiles)
        pool.close()
        pool.join()

    def convert_raw_site(self, site):
        #print('convert site', site)
        sitepath = self.get_site_path(site)
        sitepath_tmp = self.get_site_path(site, True)
        tarball_path = sitepath[0:-1] + '.tar.gz'
        topic_path = sitepath + 't/'
        jsonl_path = '%s%s-topics-raw.jsonl' % (sitepath, site)
        additional_path = '%s%s-additional-posts.jsonl' % (sitepath, site)
        #print('convert site', sitepath, sitepath_tmp, jsonl_path, tarball_path, topic_path)

        # TODO:
        # - extract tarball to memory
        # - for each topic:
        #   - merge t/topic-slug/<topicid> json into <site>-topics-raw.jsonl
        #   - merge t/topic-slug/additional_posts.json into <site>-additional-posts.jsonl

        topicids = {}
        #pool = mp.pool.ThreadPool(100)
        additionalfile = None
        if os.path.isdir(topic_path):
            # Raw files in a filesystem
            try:
                with open(jsonl_path, 'w') as jsonlfile:
                    for topicdir in os.listdir(topic_path):
                        print('e', topicdir)

                        topicfilepath = os.path.join(topic_path, topicdir)
                        if os.path.isdir(topicfilepath):
                            topicid = None
                            hasadditional = False
                            for topicfile in os.listdir(topicfilepath):
                                #pool.apply_async(self.convert_raw_topic, args=(site, sitezip, file), callback=self.handle_raw_topic)
                                if topicfile == 'additional_posts.json':
                                    hasadditional = True
                                else:
                                    topicid = topicfile
                            if topicid != None:
                                jsonlfile.write(self.convert_raw_topic_file(site, '/t/' + topicdir + '/' + str(topicid), topicid))
                                if hasadditional:
                                    if additionalfile == None:
                                        additionalfile = open(additional_path, 'w')
                                    additionalfile.write(self.convert_raw_topic_file(site, '/t/' + topicdir + '/additional_posts.json', topicid))
            except Exception as e:
                print('error while converting: ' + str(e))

        elif os.path.isfile(tarball_path):
            # Raw files in a tarball
            print('Opening zip: ' + tarball_path)
            sitezip = tarfile.open(tarball_path, 'r')
            print(sitepath)
            pathlib.Path(sitepath).mkdir(parents=True, exist_ok=True)
            with open(jsonl_path, 'w') as jsonlfile:
                for file in sitezip:
                    print(file.name)
                    try:
                        #pool.apply_async(self.convert_raw_topic, args=(site, sitezip, file), callback=self.handle_raw_topic)
                        self.handle_raw_topic(self.convert_raw_topic_zip(site, sitezip, file.name))
                    except Exception as e:
                        print('aaaa', str(e))


                    
            #    print('go on now')
        else:
            print('WARNING - missing tar.gz file: %s' % (tarball_path))
        if additionalfile:
            additionalfile.close()
        print('converted site: ' + site)
    def convert_raw_topic_zip(self, site, sitezip, filename):
        try:
            matches_topic = regex_topic.search(filename)
            matches_additional = regex_additional.search(filename)
            if matches_topic:
                #topicids[matches_topic[1]] = matches_topic[2]
                #print('compose json...')
                #jsonstr = '{"domain": "%s", "path": "%s", "contents": %s}\n' % (site, matches_topic[0], sitezip.extractfile(filename).read().decode('utf-8'))
                #jsonlfile.write(sitezip.extractfile(filename).read().decode('utf-8') + '\n')
                #additionalfile.write(jsonstr)
                return self.get_topic_json(site, matches_topic[0], sitezip.extractfile(filename).read().decode('utf-8'))
            elif matches_additional:
                #jsonstr = '{"topicid": %s, "posts": %s}\n' % (topicids[matches_additional[1]], sitezip.extractfile(filename).read().decode('utf-8'))
                #return jsonstr
                return self.get_additionalposts_json(topicids[matches_additional[1]], sitezip.extractfile(filename).read().decode('utf-8'))
        except Exception as e:
            print('uh oh: ' + str(e))
    def convert_raw_topic_file(self, site, topicpath, topicid):
        try:
            sitepath = self.get_site_path(site)
            matches_topic = regex_topic.search(topicpath)
            matches_additional = regex_additional.search(topicpath)
            if matches_topic:
                #topicids[matches_topic[1]] = matches_topic[2]
                #print('compose json...')
                #jsonstr = '{"domain": "%s", "path": "%s", "contents": %s}\n' % (site, matches_topic[0], sitezip.extractfile(filename).read().decode('utf-8'))
                #jsonlfile.write(sitezip.extractfile(filename).read().decode('utf-8') + '\n')
                #additionalfile.write(jsonstr)
                with open(sitepath + topicpath) as tfd:
                    return self.get_topic_json(site, topicpath, tfd.read())
            elif matches_additional:
                #jsonstr = '{"topicid": %s, "posts": %s}\n' % (topicids[matches_additional[1]], sitezip.extractfile(filename).read().decode('utf-8'))
                #return jsonstr
                #return self.get_additionalposts_json(topicids[matches_additional[1]], sitezip.extractfile(filename).read().decode('utf-8'))
                with open(sitepath + topicpath) as tfd:
                    return self.get_additionalposts_json(topicid, tfd.read())
        except Exception as e:
            print('uh oh: ' + str(e))

    def get_topic_json(self, site, path, contents):
        return '{"domain": "%s", "path": "%s", "contents": %s}\n' % (site, path, contents)

    def get_additionalposts_json(self, topicid, posts):
        return '{"topicid": %s, "posts": %s}\n' % (topicid, posts)

    def handle_raw_topic(self, response):
        print(response)
        if response[0] == 'topic':
            topicfile = self.get_topic_file(response[1])
            topicfile.write(response[2])
        elif response[0] == 'posts':
            postsfile = self.get_additionalposts_file(response[1])
            postsfile.write(response[2])

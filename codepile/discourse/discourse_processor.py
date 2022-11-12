from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
import gzip
import zstd
import os
import shutil
import hashlib
import tarfile
import json
import html2text
from multiprocessing import Pool
import boto3
import botocore.exceptions
import time
import random

S3_BUCKET = "s-eai-neox"
S3_BUCKET_PATH = "data/codepile/discourse/"

s3client = boto3.client('s3')

text_maker = html2text.HTML2Text()
text_maker.unicode_snob = True
text_maker.ignore_links = True
text_maker.ignore_images = True
text_maker.images_to_alt = False
text_maker.ignore_images = True
text_maker.mark_code = True
text_maker.escape_snob = False
text_maker.open_quote = '"'
text_maker.close_quote = '"'
text_maker.body_width = 0


class DiscourseProcessor(Processor):
    def __init__(self, temp_dir, data_dir):
        self.data_dir = data_dir
        self.temp_dir = temp_dir
        self.index = False

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

    def parse_site_list(self, site):
        sitelist = []
        if site == 'all':
            with open('discourse/index.json', 'r') as indexfd:
                urls = json.loads(indexfd.read())
                for url in urls:
                    parts = url.split('/')
                    sitelist.append(parts[2])
        else:
            sitelist = site.split(',')
        return sitelist

    def compress(self, site):
        sitelist = self.parse_site_list(site)

        zippool = Pool(processes=6)
        for site in sitelist:
            sitepath = self.get_site_path(site)
            if os.path.isdir(sitepath):
                # spawn zip process
                job = zippool.apply_async(self.compress_site, args=[site])

        zippool.close()
        zippool.join()

        print('Updating checksums')
        checksums = self.load_checksums()
        md5pool = Pool(processes=6)
        for site in sitelist:
            md5pool.apply_async(self.checksum_site, args=[site], callback=self.update_checksum)

        md5pool.close()
        md5pool.join()
        self.save_checksums()

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
        with open(sitepath_tmp + '.jsonl', 'w') as outfile:
            def blargh(f):
                self.convert_callback(f, outfile)
            pool = Pool(processes=4)
            jobs = []
            batch = []
            topicroot = os.path.join(sitepath_tmp, 't')
            for topicdir in os.listdir(topicroot):
                #print('e', topicdir)

                topicfilepath = os.path.join(topicroot, topicdir)
                if os.path.isdir(topicfilepath):
                    for topicfile in os.listdir(topicfilepath):
                        topicpath = os.path.join(topicfilepath, topicfile)
                        batch.append(topicpath)
                elif os.path.isfile(topicfilepath):
                    batch.append(topicfilepath)

                if len(batch) > 100:
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
        with open(sitepath_tmp + '.jsonl', 'rb') as f_in, open(zstdpath, 'wb') as f_out:
            f_out.write(zstd.ZSTD_compress(f_in.read()))
        print(zstdpath, flush=True)


    def convert_site_files(self, files):
        converted = []
        #print('CONVERT BATCH:', files)
        for file in files:
            newdata = self.convert_site_file(file)
            converted.append(newdata)
        return converted

    def convert_site_file(self, file):
        try:
            with open(file, 'r') as fd:
                topic = json.loads(fd.read())
                #print(topic)
                poststream = topic['post_stream']

                posts = {}
                for post in poststream['posts']:
                    posts[post['id']] = post

                stream = poststream['stream']
                text = ''
                authors = []

                replies = [
                    'To which %s replied\n\n    %s',
                    'Then %s said\n\n    %s',
                    'So %s replied\n\n    %s',
                    '%s replied\n\n    %s',
                    'After which, %s added\n\n    %s',
                ]

                if topic['reply_count'] < 1:
                    return False;

                missing = []
                for id in stream:
                    if id in posts:
                        post = posts[id]
                        contents = text_maker.handle(post['cooked']).replace('\u2019', "'").strip().replace('\n', '\n    ')
                        author = '@' + post['username']
                        if 'name' in post and post['name'] != '' and post['name'] != None:
                            author = post['name'] + ' (@' + post['username'] + ')'

                        if text == '':
                            newtext = '%s posted a new topic, subject "%s". It read:\n\n   %s\n\n' % (author, topic['title'], contents)
                            if newtext == None:
                                print('AHHH, WHAT?', author, topic['title'], contents)
                            text = newtext
                        else:
                            text += (random.choice(replies) + '\n\n') % (author, contents)
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

                newdata = {
                        "text": text,
                        "meta": {
                            "source": "discourse",
                            "url": "",

                        }
                }
                #print(newdata)
                #print('=' * 20 + '\n\n' + text)
                return newdata
        except Exception as e:
            #print("Exception during conversion", e)
            print('!', end='', flush=True)
            self.log_process_failure(file, 'exception', str(e))
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


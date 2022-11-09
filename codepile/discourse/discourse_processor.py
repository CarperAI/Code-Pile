from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
import gzip
import zstd
import os
import hashlib
import tarfile
import json
import html2text
from multiprocessing import Pool
import boto3
import botocore.exceptions

S3_BUCKET = "s-eai-neox"
S3_BUCKET_PATH = "data/codepile/discourse/"

s3client = boto3.client('s3')

class DiscourseProcessor(Processor):
    def __init__(self, temp_dir, data_dir):
        self.data_dir = data_dir
        self.temp_dir = temp_dir

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
                self.extract_site_zip(site)
                self.convert_site(site)

        return

    def get_zip_name(self, site):
        return '%s.tar.gz' % (site)

    def get_zip_path(self, site):
        zipfilename = self.get_zip_name(site)
        return 'discourse/%s' % (zipfilename)
    def get_site_path(self, site, temp=False):
        if temp:
            return ('%s/%s' % (self.temp_dir, site)).replace('//', '/')
        return '%s/discourse/%s' % (self.data_dir, site)
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
        else:
            print('Dir already existed', self.temp_dir + '/' + site)
        return True

    def convert_site(self, site):
        sitepath = self.get_site_path(site)
        with open(sitepath + '.jsonl', 'w') as outfile:
            def blargh(f):
                self.convert_callback(f, outfile)
            pool = Pool(processes=40)
            jobs = []

            for topicdir in os.listdir(sitepath + '/t'):
                #print('e', topicdir)
                for topicfile in os.listdir(sitepath + '/t/' + topicdir):
                    topicpath = '%s/t/%s/%s' % (sitepath, topicdir, topicfile)
                    job = pool.apply_async(self.convert_site_file, args=[topicpath], callback=blargh)
                    jobs.append(job)

            #for job in jobs:
            #    job.get()

            #print(topicfiles)
            #pool.map(self.convert_site_file, topicfiles)
            pool.close()
            pool.join()
        print('compressing... ', end='')
        with open(sitepath + '.jsonl', 'rb') as f_in, open(sitepath + '.jsonl.zstd', 'wb') as f_out:
            f_out.write(zstd.ZSTD_compress(f_in.read()))
        print(sitepath + '.jsonl.zstd')


    def convert_site_file(self, file):
        #print('HEY', file)
        data = json.loads(open(file, 'r').read())
        #print(data)
        poststream = data['post_stream']

        posts = {}
        for post in poststream['posts']:
            posts[post['id']] = post

        stream = poststream['stream']
        text = ''
        for id in stream:
            if id in posts:
                text += html2text.html2text(posts[id]['cooked']) + '\n\n'
            else:
                #print('ERROR: missing post', id)
                pass

        newdata = {
                "text": text,
                "meta": {
                }
        }
        #print(newdata)
        return newdata

    def convert_callback(self, returnval, outfile):
        print('.', end='', flush=True)
        outfile.write(json.dumps(returnval) + '\n')


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


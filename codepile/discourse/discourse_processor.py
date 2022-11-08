from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
import gzip
import os
import hashlib
import tarfile
import json
import html2text
from multiprocessing import Pool

class DiscourseProcessor(Processor):
    def __init__(self, site, data_dir, temp_dir):
        self.data_dir = data_dir
        self.temp_dir = temp_dir
        self.site = site

    def analyze(self):
        print('ok cool lets go', self.site)

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

    def process(self, *args, **kwargs):
        # TODO - transform raw JSON data into whatever format we need for training
        # accept site as argument
        os.chdir(self.data_dir)
        site = 'talk.dallasmakerspace.org'
        checksums = self.load_checksums()

        zipfilename = self.get_zip_name(site)
        zipfilepath = self.get_zip_path(site)

        print('check zipfile', zipfilepath)
        # check if site.tar.gz exists and has the expected checksum
        if self.verify_checksum(site):
            # extract site.tar.gz to tempdir
            print('yup, process it')
            self.extract_site_zip(site)

            self.convert_site(site)
            # allocate multiprocessing pool of however many CPUs we have
            # for each topic:
                # transform json into lm_dataformat by flattening poststream

        return

    def get_zip_name(self, site):
        return '%s.tar.gz' % (site)

    def get_zip_path(self, site):
        zipfilename = self.get_zip_name(site)
        return 'discourse/zipped/%s' % (zipfilename)

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
        sitepath = '%s/%s' % (self.temp_dir, site)
        with open(sitepath + '/items.jsonl', 'w') as outfile:
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

            for job in jobs:
                job.get()

            #print(topicfiles)
            #pool.map(self.convert_site_file, topicfiles)
            print('ok go')
            pool.close()
            pool.join()

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
        #print("WOW OK COOL", returnval, outfile)
        print('.', end='', flush=True)
        outfile.write(json.dumps(returnval) + '\n')


    def load_checksums(self):
        try:
            self.checksums = {}
            with open('discourse/zipped/checksums.txt', 'r') as csfd:
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

    def verify_checksum(self, site):
        zipfilename = self.get_zip_name(site)
        zipfilepath = self.get_zip_path(site)

        if os.path.isfile(zipfilepath):
            #print('got the zip', zipfilepath, site)

            # validate site.tar.gz checksum
            #print(self.checksums)
            if zipfilename in self.checksums:
                expectedchecksum = self.checksums[zipfilename]
                actualchecksum = hashlib.md5(open(zipfilepath, 'rb').read()).hexdigest()
                #print('expected: %s\tactual: %s' % (expectedchecksum, actualchecksum))
                return expectedchecksum == actualchecksum
        return False 



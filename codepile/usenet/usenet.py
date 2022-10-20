from codepile.dataset import DatasetInfo, Dataset

from .forum import mbox

from xml.etree import ElementTree as ET

import internetarchive as ia

import shutil
import os
import glob
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

import json
import boto3


# Usenet comp archive
USENET_COMP = 'usenet-comp'
BOOKS_S3_BUCKET = "s-eai-neox"


class UsenetDataset(Dataset):
    def __init__(self, config):
        self.config = config
        self.info = DatasetInfo(
            id='UsenetComp',
            description='Archive of all comp usenet discussions',
            size=2.4e11,
            source_uri='https://archive.org/details/usenet-comp',
            dataset_pros='Gold mine of conversations for major programming languages.',
            dataset_cons='Misses modern programming languages like Python.',
            languages=['english', ],
            coding_languages=['java', 'c', 'javascript', 'cobol', 'others', ],
            modalities=['discussion', 'source_code', ],
            source_license='none',
            source_citation='Usenet',
            data_owner='Jehoshaph A Chandran',
            contributers=['Jehoshaph A Chandran', ],
            data_end=datetime(2013, 8, 19),
            data_start=datetime(1980, 1, 1),
        )

    def info(self):
        return self.info

    def id(self):
        return self.info.id

    def download(self, *args, **kwargs):
        """
        Download and process the entire comp raw dataset (or) a subset of archives
        :param kwargs: files - (optional) list of comp archives to download, empty will download the entire archive
        :return:
        """
        # Download
        if not os.path.exists(self.config.raw_data_dir):
            os.makedirs(self.config.raw_data_dir)

        # Download from s3
        if kwargs.get('s3'):
            s3 = boto3.client('s3')
            usenet_comp_zipfile = os.path.join(self.config.raw_data_dir, 'usenet-comp.zip')
            s3.download_file(
                BOOKS_S3_BUCKET, 'data/codepile/usenet/usenet-comp-raw.zip', usenet_comp_zipfile,
            )
            # Unzipping in place
            shutil.unpack_archive(
                usenet_comp_zipfile, self.config.tmpdir
            )
        else:
            # Default - Use IA
            # Download the archive (or specific files from the archive) to the temp dir
            if kwargs.get('files'):
                ia.download(USENET_COMP, destdir=self.config.raw_data_dir, files=kwargs.get('files'))
            else:
                ia.download(USENET_COMP, destdir=self.config.raw_data_dir)

        self.process()

    def process(self, *args, **kwargs):
        # Process
        # Unzipping all files to the temp dir
        if os.path.exists(self.config.tmpdir):
            shutil.rmtree(self.config.tmpdir)

        os.makedirs(self.config.tmpdir)

        for file in glob.glob(os.path.join(self.config.raw_data_dir, USENET_COMP, '*')):
            if file.endswith('.mbox.zip'):
                shutil.unpack_archive(file, self.config.tmpdir)

        # Processing each email group
        if not os.path.exists(self.config.output_data_dir):
            os.makedirs(self.config.output_data_dir)

        # Create a log file
        logfile = os.path.join(self.config.output_data_dir, f'{USENET_COMP}.log')
        # Clearing the log file
        open(logfile, 'w').close()

        # Create an out folder
        out_file = os.path.join(self.config.output_data_dir, f'{USENET_COMP}.parquet')

        for file in glob.glob(os.path.join(self.config.tmpdir, '*')):
            if file.endswith('.mbox'):
                try:
                    threads = mbox.process_forum(file)
                    forum_content = []
                    forum_metadata = []
                    forum_name = os.path.basename(file)
                    for thread in threads:
                        # Forum content in xml
                        # forum_content.append(
                        #     ET.tostring(thread.export_xml(), encoding='utf-8', method='xml')
                        # )

                        # Forum content in plain text
                        forum_content.append(thread.export_string())

                        # Metadata for this forum's threads
                        m_metadata = thread.get_metadata()
                        m_metadata.update({'forum_name': forum_name})
                        forum_metadata.append(json.dumps(m_metadata))

                    forum_frame = pd.DataFrame({
                        'metadata': forum_metadata,
                        'content': forum_content,
                    })
                    forum_table = pa.Table.from_pandas(forum_frame)
                    pq.write_to_dataset(forum_table, root_path=out_file)

                    f = open(logfile, 'a')
                    f.write(f'Success {file}: includes {len(threads)}\n')
                    f.close()

                except Exception as e:
                    # Appending to the log file
                    f = open(logfile, 'a')
                    f.write('Error {file}: {e}\n'.format(file=file, e=str(e)))
                    f.close()

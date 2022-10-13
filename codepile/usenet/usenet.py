from codepile.dataset import DatasetInfo, Dataset

from .forum import mbox

from xml.etree import ElementTree as ET

import internetarchive as ia

import shutil
import os
import glob
from datetime import datetime

import pandas as pd


# Usenet comp archive
USENET_COMP = 'usenet-comp'


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
        # 1. Download
        if not os.path.exists(self.config.raw_data_dir):
            os.makedirs(self.config.raw_data_dir)

        # Download the archive (or specific files from the archive) to the temp dir
        if kwargs.get('files'):
            ia.download(USENET_COMP, destdir=self.config.raw_data_dir, files=kwargs.get('files'))
        else:
            ia.download(USENET_COMP, destdir=self.config.raw_data_dir)

        # 2. Process
        # Unzipping to the temp dir
        if not os.path.exists(self.config.tmpdir):
            os.makedirs(self.config.tmpdir)

        for file in glob.glob(os.path.join(self.config.raw_data_dir, USENET_COMP, '*')):
            if file.endswith('.mbox.zip'):
                shutil.unpack_archive(file, self.config.tmpdir)

        # Processing each email group
        if not os.path.exists(self.config.output_data_dir):
            os.makedirs(self.config.output_data_dir)

        threads_content = []
        for file in glob.glob(os.path.join(self.config.tmpdir, '*')):
            if file.endswith('.mbox'):
                threads = mbox.process_forum(file)
                for thread in threads:
                    # tree = ET.ElementTree(thread.export_xml())
                    threads_content.append(
                        ET.tostring(thread.export_xml(), encoding='utf-8', method='xml')
                    )

        # Writing parquet to dest folder
        pf = pd.DataFrame(threads_content, columns=['content'])
        pf.to_parquet(os.path.join(self.config.output_data_dir, 'usenet.parquet'))

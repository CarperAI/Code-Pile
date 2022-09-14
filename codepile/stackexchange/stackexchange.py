from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset

from datetime import datetime

import internetarchive as ia

'''
# example
StackExchangeInfo = DatasetInfo(
        identifier='StackExchange',
        description='',
        data_end=datetime(2022,1,1),
        data_start=10,
        size=10,
        storage_format='tar',
        #storage_uri='/root',
        cpu_hours=1,
        gpu_hours=1,
        ram_requirements=1,
        tempfile_requirement=1,
        source_uri='https://archive.org/details/stackexchange',
        dataset_pros='l',
        dataset_cons='l',
        languages=[''],
        coding_languages=[''],
        modalities=['discussion'],
        source_license='gpl',
        source_citation='this',
        data_owner='me',
        contributers=['me']
        )
'''

class StackExchangeScraper(Scraper):
    def scrape(self) -> RawDataset:
        item = ia.get_item('stackexchange')
        metadata = item.metadata
        ia.download('stackexchange', checksum=True, verbose=True, destdir=self.target_dir)

        return RawDataset(storage_uris=['file:///{self.target_dir}'],
                metadata=str(metadata))


class StackExchangeDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = StackExchangeScraper(tempdir, target_dir)
    def download(self):
        self.scraper.download()

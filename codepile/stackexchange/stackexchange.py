from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset

from datetime import datetime

import internetarchive as ia

from codepile.stackexchange.processor import StackExchangeProcessor

STACKEXCHANGEINFO = DatasetInfo(
        id='StackExchange',
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

class StackExchangeScraper(Scraper):
    def scrape(self) -> RawDataset:
        target_dir = self.config.raw_data_dir
        exclude_files = ["stackoverflow.com-PostHistory.7z", "stackoverflow.com-Votes.7z", "stackoverflow.com-Badges.7z"] # exclude some large files that are not needed. We can only do this for stackoverflow.com data as the dumps are available for each table separately. We could also exclude some sites that are not english based.
        item = ia.get_item('stackexchange')
        file_names = []
        for file in item.files:
            if file['name'] not in exclude_files:
                file_names.append(file['name'])
        metadata = item.metadata
        item.download(files=file_names, checksum=True, verbose=True, destdir=target_dir)

        return RawDataset(storage_uris=['file:///{target_dir}'],
                metadata=str(metadata))


class StackExchangeDataset(Dataset):
    def __init__(self, config):
        self.config = config
        self.scraper = StackExchangeScraper(config, self.id)
        self.processor = StackExchangeProcessor(config, self.id)

    def download(self):
        self.scraper.scrape()

    @property
    def info(self):
        return STACKEXCHANGEINFO

    @property
    def id(self):
        return "StackExchange"
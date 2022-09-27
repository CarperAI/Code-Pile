import sys
import argparse
from codepile.stackexchange.stackexchange import *
from codepile.dataset import Dataset, Scraper, Processor
from functools import reduce

from pydantic import BaseModel, DirectoryPath

import json

# todo
CODEPILEINFO = DatasetInfo(
        id='CodePile',
        description='',
        data_end=datetime(2022,1,1),
        data_start=10,
        size=10,
        storage_format='.jsonl.zst',
        #storage_uri='/root',
        cpu_hours=1,
        gpu_hours=1,
        ram_requirements=1,
        tempfile_requirement=1,
        source_uri='https://github.com/CarperAI/Code-Pile',
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


class CodePileScraper(Scraper):
    def __init__(self, config, dataset_id, *args, **kwargs):
        self.config = config

    # this should be already jsonl.zst
    # dataset specific processing should maybe be done to save storage
    # subparts should maybe just be downloaded/collected
    # not really scraped anymore
    def scrape(self, subdatasets) -> RawDataset:
        uris = []
        for d in subdatasets:
            ds = d.download()
            uris.append(ds.storage_uris)

        return RawDataset(storage_uris=reduce(lambda x,y: x+y, uris))


class CodePileProcessor(Processor):
    def __init__(self, config, *args, **kwargs):
        self.config = config

    def process(self, subdatasets, *args, **kwargs):
        for d in subdatasets:
            d.process()


class CodePile(Dataset):
    def __init__(self, config):
        self.config = config
        self.scraper = CodePileScraper(self.config, self.id)
        self.processor = CodePileProcessor(self.config, self.id)

        self.subdatasets = []

        subsets = [
            StackExchangeDataset
            ]
        dataset_ids = [] 
        for d in subsets:
            # todo, solve this properly 
            assert d.id not in dataset_ids, 'dataset ids have to be unique'
            self.subdatasets.append(d(self.config))

    def download(self):
        self.scraper.scrape(self.subdatasets)

    def process(self):
        self.processor.process(self.subdatasets)

    @property
    def id(self):
        return "CodePileDataset"

    @property
    def info(self):
        return CODEPILEINFO


class Config(BaseModel):
    raw_data_dir : DirectoryPath
    output_data_dir : DirectoryPath
    tmpdir : DirectoryPath


def download(args):
    config = Config.parse_raw(args.config.read())
    ds = CodePile(config)
    raw_data = ds.download()


def process(args):
    config = Config.parse_raw(args.config.read())
    ds = CodePile(config)
    ds.process()


def configure(args):
    config = Config(
            raw_data_dir=args.raw_data_dir,
            output_data_dir=args.output_data_dir,
            tmpdir=args.tmpdir
            )
    print(config.json())


def cli(cli_args, *args, **kwargs):
    parser = argparse.ArgumentParser('codepile dataset tool')

    subparsers = parser.add_subparsers(required=True)

    configurator_parser = subparsers.add_parser('configure')
    configurator_parser.add_argument('raw_data_dir', type=str)
    configurator_parser.add_argument('output_data_dir', type=str)
    configurator_parser.add_argument('tmpdir', type=str)
    configurator_parser.set_defaults(func=configure)

    download_parser = subparsers.add_parser('download')
    download_parser.add_argument('config', type=argparse.FileType('r'))
    download_parser.set_defaults(func=download)

    process_parser = subparsers.add_parser('process')
    process_parser.add_argument('config', type=argparse.FileType('r'))
    process_parser.set_defaults(func=process)


    args = parser.parse_args(cli_args[1:])
    args.func(args)


if __name__ == "__main__":
    cli(sys.argv)

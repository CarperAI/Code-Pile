import sys
import argparse
from codepile.stackexchange.stackexchange import *
from codepile.discourse.discourse import *
from codepile.dataset import Dataset

class CodePile(Dataset):
    def __init__(self, tempdir, target_dir):
        self.subdatasets = []
        print('hi go')
        subsets = [
            #StackExchangeDataset
            DiscourseDataset
            ]
        for d in subsets:
            self.subdatasets.append(d(tempdir, target_dir))

    def download(self):
        for d in self.subdatasets:
            print('ok scrape')
            d.scraper.scrape()

    def process(self):
        for d in self.subdatasets:
            d.processor.process()

    def merge(self):
        raise NotImplementedError()


def download(args):
    ds = CodePile(args.tempdir, args.output_dir)
    ds.download()


def process(args):
    ds = CodePile(args.tempdir, args.output_dir)
    ds.process()


def cli(cli_args, *args, **kwargs):
    parser = argparse.ArgumentParser('codepile dataset')

    print('cli')
    subparsers = parser.add_subparsers()

    download_parser = subparsers.add_parser('download')
    download_parser.add_argument('output_dir', type=str)
    download_parser.add_argument('tempdir', type=str)
    download_parser.set_defaults(func=download)

    process_parser = subparsers.add_parser('process')
    process_parser.add_argument('input_dir', type=str)
    process_parser.add_argument('output_dir', type=str)
    process_parser.add_argument('tempdir', type=str)

    process_parser.set_defaults(func=process)

    args = parser.parse_args(cli_args[1:])
    args.func(args)

    if len(cli_args) == 1:
        parser.print_help()

if __name__ == "__main__":
    cli(sys.argv)
    


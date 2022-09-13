import sys
import argparse
from codepile.stackexchange import *


def cli(cli_args, *args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument('output_dir', type=str)

    print(StackExchangeInfo)
    parser.parse_args(cli_args[1:])

if __name__ == "__main__":
    cli(sys.argv)
    


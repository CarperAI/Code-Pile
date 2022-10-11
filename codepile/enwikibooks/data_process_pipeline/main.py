"""Filtering."""

import os
import logging

import argparse

from datasets import Dataset
import pandas as pd

from filtering import DatasetFiltering

from deduplicate import exact_deduplicate_dataset
from utils import check_num_proc


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def parseArgs():
    parser = argparse.ArgumentParser(description="Filtering.")
    parser.add_argument(
        "--data_file",
        type=str,
        default="./computing_wikibooks.parquet.gzip",
        help="'load_dataset' returns all files that match the Unix style pattern passed by 'data_file'",
    )
    parser.add_argument(
        "--lang_dataset_id",
        type=str,
        default="en",
        help="ID of the language in which the dataset is written.",
    )
    parser.add_argument(
        "--path_fasttext_model",
        type=str,
        default="models/lid.176.bin",
        help="Path to the Fasttext model used for language identification.",
    )
    parser.add_argument(
        "--path_sentencepiece_model",
        type=str,
        default="models/en.sp.model",
        help="Path to the Sentence Piece model used to tokenize text for perplexity scores.",
    )
    parser.add_argument(
        "--path_kenlm_model",
        type=str,
        default="models/en.arpa.bin",
        help="Path to the KenLM model used to compute perplexity scores.",
    )
    parser.add_argument(
        "--num_proc",
        type=int,
        default=-1,
        help="Number of processes for multiprocessing. Default at the number of processors available.",
    )
    parser.add_argument(
        "--path_dir_save_dataset",
        type=str,
        default="./data_cleaned/",
        help="Path to the directory where the filtered version of the dataset will be saved.",
    )

    parser.add_argument(
        "--tokenization",
        type=str,
        default="character",
        help="Character, punctuation or space",
    )

    parser.add_argument(
        "--window_size",
        type=int,
        default=6,
        help="Size of the token window, average arabic word length is 5",
    )

    parser.add_argument(
        "--hamming_distance",
        type=int,
        default=4,
        help="Similarity threshold out of 64 bits",
    )

    parser.add_argument(
        "--num_blocks",
        type=int,
        default=6,
        help="Must be larger than the hamming_distance",
    )

    parser.add_argument(
        "--ignore_punctuation",
        type=bool,
        default=True,
        help="Ignore punctuation when hashing, cannot be true when punctuation is used for tokenization",
    )

    parser.add_argument(
        "--lowercase",
        type=bool,
        default=True,
        help="Lowercase the text when hashing",
    )

    parser.add_argument(
        "--text_column",
        type=str,
        default="text",
        help="Column name for the text to be hashed",
    )

    parser.add_argument(
        "--index_column",
        type=str,
        default="id",
        help="Column name for the index",
    )

    parser.add_argument(
        "--cache_dir",
        type=str,
        default="outputs/id_cache",
        help="Path to the directory where the filtered version of the dataset will be saved.",
    )

    parser.add_argument(
        "--output_dir",
        type=str,
        default="outputs/",
        help="Path to the directory where the filtered version of the dataset will be saved.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parseArgs()

    df = pd.read_parquet(args.data_file)
    dataset = Dataset.from_pandas(df)

    dataset_filtering = DatasetFiltering(
        dataset=dataset,
        lang_dataset_id=args.lang_dataset_id,
        path_fasttext_model=args.path_fasttext_model,
        path_sentencepiece_model=args.path_sentencepiece_model,
        path_kenlm_model=args.path_kenlm_model,
        num_proc=check_num_proc(args.num_proc),
        path_dir_save_dataset=args.path_dir_save_dataset,
    )
    dataset_filtering.modifying_documents()
    dataset_filtering.filtering()
    # save parquet
    out_file = os.path.join(args.output_dir, f"filtered_{args.data_file.split('/')[-1]}")
    exact_deduplicate_dataset(dataset_filtering.ds, args)
    logger.info("Saving filtered / deduplicated parquet at %s", out_file)
    dataset_filtering.ds.to_pandas().to_parquet(out_file)

if __name__ == "__main__":
    main()


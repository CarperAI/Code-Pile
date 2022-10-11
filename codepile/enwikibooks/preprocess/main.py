"""Filtering."""

from multiprocessing import cpu_count
import os
import logging

import argparse

from datasets import load_dataset, Dataset
import pandas as pd

from filtering import DatasetFiltering

from tqdm import tqdm


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def check_num_proc(num_proc: int = -1) -> int:
    """
    Check the number of processors. Return a safe-checked value.

    Parameters
    ----------
    num_proc : int, optional
        Number of processors to use, by default -1

    Returns
    -------
    int
        Number of processors to use

    Raises
    ------
    ValueError
        If the input exceeds the number of processors available
    """
    maximum: int = cpu_count()
    if num_proc > maximum:
        raise ValueError(
            f"{num_proc} exceeds the maximum number ({maximum}) of processors"
        )

    if num_proc == -1:
        num_proc = maximum
    else:
        print(f"Using {num_proc} processors out of {maximum} can be slow")

    return num_proc


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
    dataset_filtering.ds.to_pandas().to_parquet(os.path.join(args.output_dir, f"filtered_{args.data_file}"))
    # dataset_filtering.save_dataset()


# #---------------- Deduplication ----------------#
    ds = dataset_filtering.ds

    logger.info(f"Done preprocess {len(ds)} records")

    if not os.path.exists(args.cache_dir):
        os.makedirs(args.cache_dir, exist_ok=True)

    #output_dir
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)

    urls = ds.map(
        lambda x: {
            "url" : x['url'],
            "text": x["text"].replace("\n", " "),
        },
        num_proc=check_num_proc(args.num_proc),
        desc="Extracting URLs",
    )
    logger.info(
        f"Extracted URLs: {len(urls['url'])}, Unique URLs: {len(set(urls['url']))}"
    )

    # Save text data for substring deduplication
    urls.to_csv(
        os.path.join(args.output_dir, "text.csv"),
        num_proc=check_num_proc(args.num_proc),
        index=False,
        header=False,
        columns=["text"],
    )
    urls.to_csv(
        os.path.join(args.output_dir, "ids.csv"),
        num_proc=check_num_proc(args.num_proc),
        index=False,
        header=False,
        columns=["id"],
    )

    del urls

    logger.info(f"Start hashing {len(ds)} records")
    if args.ignore_punctuation:
        assert (
            args.tokenization != "punctuation"
        ), f"Cannot ignore punctuation when tokenization is set to `punctuation`"

    ds = ds.map(
        hashing,
        fn_kwargs={
            "tokenization": args.tokenization,
            "window_size": args.window_size,
            "column": args.text_column,
            "ignore_punctuation": args.ignore_punctuation,
            "lowercase": args.lowercase,
            "output": INTERNAL_HASH,
        },
        num_proc=check_num_proc(args.num_proc),
        desc="Hashing",
    )
    logger.info(f"Done hashing {len(ds)} records")

    logger.info(f"Start querying {len(ds)} records")
    matches = simhash.find_all(
        tqdm(ds[INTERNAL_HASH], total=len(ds)),
        args.num_blocks,
        args.hamming_distance,
    )
    logger.info(f"Done querying {len(ds)} records, found {len(matches)} matches")

    

if __name__ == "__main__":
    main()


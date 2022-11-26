import datasets
import logging
import lm_dataformat as lmd
from dataclasses import dataclass
import multiprocessing as mp
import argparse
import ast
import json
import os
from pathlib import Path

from codepile.stats.src.utils import stat_config_map, LangDetection, Tokenization

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


STATS_DUMP_PATH = "stats/stat_dump"


def read_json_file(json_file_path: str) -> dict:
    with open(json_file_path, "r") as f:
        return json.load(f)


def write_json_file(json_file_path: str, data: dict):
    with open(json_file_path, "w") as f:
        json.dump(data, f, indent=2)


def check_path_or_create(dataset_idt: str):
    path = os.path.join(STATS_DUMP_PATH, dataset_idt)
    if not Path(path).is_dir():
        os.makedirs(path)
        logger.info(f"Didn't find {path}, so created it...")


# TODO : Handle various modes of loading...
def load_dataset_custom(dataset_name_or_path: str, **kwargs) -> datasets.Dataset:
    """
    Custom method to load the dataset into the memory
    args:
        dataset_name_or_path (str) : Name of the dataset or path to the dataset
    """
    if Path(dataset_name_or_path).is_dir():
        dataset = datasets.load_from_disk(dataset_name_or_path)
        return dataset
    else:
        dataset = datasets.load_dataset("json",data_files=[dataset_name_or_path], split="train", **kwargs)
        return dataset


@dataclass
class StatAttribute:
    """
    Class to store the statistics of the dataset
    """

    name: str
    token_length: float
    meta_footprint: str


class StatFuncBuilder:
    """
    Builder utility for `StatFuncBuilder` module
    """

    def __init__(self, config_path: str) -> None:
        self.config = read_json_file(config_path)
        logger.info("Loaded config from {}".format(config_path))

    def build(self) -> list:
        """
        Builds the `StatFuncBuilder` module returns a list of functions to be built
        """
        stat_func_list = []
        for stat_config in self.config["stat_fn"]:
            stat_func_list.append(stat_config)
        return stat_func_list


class Statistics:
    def __init__(self, dataset_path_or_name: str, read_flag="lmd") -> None:
        if read_flag == "datasets":
            self.dataset = load_dataset_custom(dataset_path_or_name)
        else:
            self.dataset = lmd.Reader(dataset_path_or_name).stream_data(get_meta=True)

    def map_fn(self, example: dict[object]):
        """
        Map function for the dataset
        """
        return example

    def map_batch_fn(self, examples: dict[list]):
        """
        Map function for the dataset
        """
        return examples

    def get_stat_dataset(
        self, num_proc: int, batched: bool = False, map_fn_batch=None, map_fn=None
    ):
        """
        Method to get the statistics of the entire dataset
        """
        if batched:
            if not map_fn_batch:
                map_fn_batch = self.map_batch_fn
            stat_dataset = self.dataset.map(
                map_fn_batch, batched=batched, num_proc=num_proc
            )
        else:
            if not map_fn:
                logger.info("No map function passed, using default map_fn")
                map_fn = self.map_fn
 
            stat_dataset = self.dataset.map(map_fn, num_proc=num_proc)
        return stat_dataset

    def get_stat_and_write(
        self, output_path: str, num_proc: int, batched: bool = False, map_fn=None
    ):
        """
        Method to get the statistics of the entire dataset and write the output
        """
        if batched:
            stat_dataset = self.dataset.map(batched=batched, num_proc=num_proc)
        else:
            stat_dataset = self.dataset.map(num_proc=num_proc)
        stat_dataset.to_parquet(output_path)  # Write as a parquet file to the path
        return "Sucess"


def master_map_fn(example: dict[object]):
    """
    Map function for the dataset
    """
    stats = {}

    stats["meta"] = example["meta"]
    stats["tok_len"] = len(stat_config_map["tokenize"].tokenize(example["text"]))
   # stats["lang"] = stat_config_map["lang_detect"].detect(example["text"])
    stats["len_char"] = stat_config_map["len_char"](example["text"])
    stats["len_utf8bytes"] = stat_config_map["len_utf8bytes"](example["text"])
    stats["len_words"] = stat_config_map["len_words"](example["text"])
    return stats


class GetMeta:
    def __init__(self, dataset_path: str, read_flag="lmd") -> None:
        """
        Get Metadata for people to write stats easily...
        args:
            dataset_path (str) : Path to the dataset
            read_flag (str) : Flag to read the dataset (lmd | datasets)
        """
        self.dataset_path = dataset_path
        self.dataset_idt = dataset_path.split(os.sep)[-1]
        if read_flag == "lmd":
            logger.info(f"Dataset path : {self.dataset_path}")
            self.dataset: iter = lmd.Reader(dataset_path).stream_data(get_meta=True)
        elif read_flag == "datasets":
            self.dataset: datasets.Dataset = load_dataset_custom(dataset_path)
        self.read_flag = read_flag

    def get_meta(self) -> dict:
        """
        Get the metadata from the dataset
        """
        if self.read_flag == "lmd":
            datapoint = next(iter(self.dataset))
            metadata = datapoint[1]
            if isinstance(metadata, dict):
                return {
                    "example_meta": metadata,
                    "meta_keys": list(metadata.keys()),
                    "len": None,
                }
            elif isinstance(metadata, str):
                return {
                    "example_meta": ast.literal_eval(metadata),
                    "meta_keys": list(ast.literal_eval(metadata).keys()),
                    "len": None,
                }
            else:
                raise TypeError("Metadata is neither a dict nor a string")
        elif self.read_flag == "datasets":
            single_meta_obj = self.dataset[0]["meta"]
            if isinstance(single_meta_obj, str):
                meta_dict = {
                    "example_meta": ast.literal_eval(single_meta_obj),
                    "meta_keys": list(ast.literal_eval(single_meta_obj).keys()),
                    "len": len(self.dataset),
                }
                return meta_dict
            else:
                meta_dict = {
                    "example_meta": single_meta_obj,
                    "meta_keys": list(ast.literal_eval(single_meta_obj).keys()),
                    "len": len(self.dataset),
                }
                return meta_dict
        else:
            raise ValueError("Invalid read_flag")

    def get_meta_and_write(self):
        """
        Get the metadata from the dataset and write it to a file
        """
        meta = self.get_meta()
        check_path_or_create(self.dataset_idt)
        metadata_path = os.path.join(STATS_DUMP_PATH, self.dataset_idt, "meta.json")
        write_json_file(metadata_path, meta)
        return meta


if __name__ == "__main__":
    argparse.ArgumentParser("Statistics Module")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_path", type=str, help="Path to the dataset")
    parser.add_argument(
        "--read_flag", type=str, help="Flag to read the dataset (lmd | datasets)"
    )
    parser.add_argument(
        "--analyze", type=str, help="Flag to analyze the dataset (meta | stat)"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to the config file",
        default="stats/config/stats_config.json",
    )
    parser.add_argument(
        "--num_proc", type=int, help="Path to the config file", default=mp.cpu_count()
    )

    args = parser.parse_args()

    print(json.dumps(vars(args), indent=4))

    if args.analyze == "meta":
        if Path(args.dataset_path).is_dir():
            dataset_idt: str = args.dataset_path.split(os.path.sep)[-1]
        else:
            dataset_idt: str = args.dataset_path
        read_flag: str = args.read_flag
        analysis_path: str = os.path.join(STATS_DUMP_PATH, dataset_idt)

        meta = GetMeta(args.dataset_path, read_flag=read_flag).get_meta_and_write()
    if args.analyze == "stat":
        if Path(args.dataset_path).is_dir():
            dataset_idt: str = args.dataset_path.split(os.path.sep)[-1]
        else:
            dataset_idt: str = args.dataset_path
        read_flag: str = args.read_flag
        analysis_path: str = os.path.join(STATS_DUMP_PATH, dataset_idt)
        config: dict = read_json_file(args.config)

        stat_class = Statistics(args.dataset_path, read_flag=read_flag, config=config)
        stat_dataset = stat_class.get_stat_dataset(
            map_fn=master_map_fn, batched=True, num_proc=args.num_proc
        )

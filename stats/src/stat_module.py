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

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)



STATS_DUMP_PATH = "stats/stat_dump"

def read_json_file(json_file_path:str)->dict:
    with open(json_file_path,"r") as f:
        return json.load(f)

def write_json_file(json_file_path:str,data:dict):
    with open(json_file_path,"w") as f:
        json.dump(data,f,indent=2)


def check_path_or_create(dataset_idt:str):
    path = os.path.join(STATS_DUMP_PATH,dataset_idt)
    if not Path(path).is_dir():
        os.makedirs(path)
        logger.info(f"Didn't find {path}, so created it...")
    


#TODO : Handle various modes of loading...
def load_dataset_custom(dataset_name_or_path:str,**kwargs)->datasets.Dataset:
    """
    Custom method to load the dataset into the memory
    args:
        dataset_name_or_path (str) : Name of the dataset or path to the dataset
    """
    if Path(dataset_name_or_path).is_dir():
        dataset = datasets.load_from_disk(dataset_name_or_path)
        return dataset
    else:
        dataset = datasets.load_dataset(dataset_name_or_path,split="train",**kwargs)
        return dataset

def batch_stat(examples:dict[list],stats_dict:dict):
    #Tokenization Check

    for example in examples:

        pass
    return stats_dict



@dataclass
class StatAttribute:
    """
    Class to store the statistics of the dataset
    """
    name : str
    token_length : float
    meta_footprint : str


class StatFuncBuilder:
    """
    Builder utility for `StatFuncBuilder` module
    """
    def __init__(self) -> None:
        pass
    

class StatModule:
    def __init__(self,dataset_path_or_name:str) -> None:
        self.dataset = load_dataset_custom(dataset_path_or_name)
        self.stat_dict = {
        } #Master stat dictionary

    def map_fn(self,example:dict[object]):
        """
        Map function for the dataset
        """
        return example

    def map_batch_fn(self,examples:dict[list]):
        """
        Map function for the dataset
        """
        return examples

    def get_stat_dataset(self,num_proc:int,batched:bool=False):
        """
        Method to get the statistics of the entire dataset
        """
        stat_dataset = self.dataset.map()









class GetMeta:
    def __init__(self,dataset_path:str,read_flag="lmd") -> None:
        """
        Get Metadata for people to write stats easily...
        args:
            dataset_path (str) : Path to the dataset
            read_flag (str) : Flag to read the dataset (lmd | datasets)
        """
        self.dataset_path = dataset_path
        self.dataset_idt = dataset_path.split(os.sep)[-1]
        if read_flag == "lmd":
            self.dataset : iter = lmd.Reader(dataset_path).stream_data(get_meta=True)
        elif read_flag == "datasets":
            self.dataset : datasets.Dataset = load_dataset_custom(dataset_path)
        self.read_flag = read_flag

    def get_meta(self)->dict:
        """
        Get the metadata from the dataset
        """
        if self.read_flag == "lmd":
            datapoint = next(iter(self.dataset))
            metadata = datapoint[1]
            if isinstance(metadata,dict):
                return metadata
            elif isinstance(metadata,str):
                return ast.literal_eval(metadata)
            else:
                raise TypeError("Metadata is neither a dict nor a string")
        elif self.read_flag == "datasets":
            single_meta_obj = self.dataset[0]["meta"]
            if isinstance(single_meta_obj,str):
                meta_dict =  {"meta" : ast.literal_eval(single_meta_obj), "meta_keys" : list(ast.literal_eval(single_meta_obj).keys()) , "len" : len(self.dataset) }
                return meta_dict
            else:
                meta_dict =  {"meta" : single_meta_obj,  "meta_keys" : list(ast.literal_eval(single_meta_obj).keys()) , "len":len(self.dataset)}
                return meta_dict
        else:
            raise ValueError("Invalid read_flag")

    def get_meta_and_write(self):
        """
        Get the metadata from the dataset and write it to a file
        """
        meta = self.get_meta()
        check_path_or_create(self.dataset_idt)
        metadata_path = os.path.join(STATS_DUMP_PATH,self.dataset_idt,"meta.json")
        write_json_file(metadata_path,meta)
        return meta


    

    
if __name__ == "__main__":
    argparse.ArgumentParser("Statistics Module")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_path",type=str,help="Path to the dataset")
    parser.add_argument("--read_flag",type=str,help="Flag to read the dataset (lmd | datasets)")
    parser.add_argument("--analyze",type=str,help="Flag to analyze the dataset (meta | stat)")

    args = parser.parse_args()

    print(json.dumps(vars(args),indent=4))

    if args.analyze == "meta":
        if Path(args.dataset_path).is_dir():
            dataset_idt : str = args.dataset_path.split(os.path.sep)[-1]
        else:
            dataset_idt : str = args.dataset_path
        read_flag : str = args.read_flag
        analysis_path : str = os.path.join(STATS_DUMP_PATH,dataset_idt)
        
        #meta = GetMeta(dataset_idt,read_flag=read_flag).get_meta_and_write()
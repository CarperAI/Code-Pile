import datasets
import logging
import lm_dataformat as lmd
from dataclasses import dataclass
import multiprocessing as mp
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


#TODO : Handle various modes of loading...
def load_dataset_custom(dataset_name_or_path:str):
    """
    Custom method to load the dataset into the memory
    args:
        dataset_name_or_path (str) : Name of the dataset or path to the dataset
    """
    dataset = datasets.load_from_disk(dataset_name_or_path)
    return dataset

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

    def get_stat_dataset(self,num_proc:int,batched:bool=False):
        """
        Method to get the statistics of the entire dataset
        """
        stat_dataset = self.dataset.map()


def batch_stat(examples:dict[list],stats_dict:dict):
    #Tokenization Check

    for example in examples:

        pass
    return stats_dict






class GetMeta:
    def __init__(self,dataset_path:str,read_flag="lmd") -> None:
        """
        Get Metadata for people to write stats easily...
        args:
            dataset_path (str) : Path to the dataset
            read_flag (str) : Flag to read the dataset (lmd | datasets)
        """
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
                return metadata
            else:
                raise TypeError("Metadata is neither a dict nor a string")
        elif self.read_flag == "datasets":
            return self.dataset[0]["meta"]
        meta = self.dataset.meta
        return meta




    

    
if __name__ == "__main__":

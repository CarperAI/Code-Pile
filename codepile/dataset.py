from typing import Union, Optional, Literal, Any
import os
from abc import ABC, abstractmethod
import uuid
import pydantic
from pydantic import BaseModel, AnyUrl, FileUrl
from datetime import datetime


MODALITY = Literal['discussion', 'code_review', 'source_code', 'unittest']

# todo, add/remove usefull/less attributes
class DatasetInfo(BaseModel):
    id: str
    description: str
    # the last time when new information was incorporated into the dataset
    # aka when was the latest sample collected
    data_end: datetime
    # the beginning of the datasets data
    data_start: Optional[datetime]
    # estimated size in bits
    size: int

    # compute cost needed for processing
    # usefull information for rebuilding
    cpu_hours: Optional[int]
    gpu_hours: Optional[int]
    ram_requirement: Optional[int]
    tempfile_requirement: Optional[int]

    # the main sources website/description/entrypoint/domain
    source_uri: AnyUrl

    # what are the advantages of including this dataset
    # like a good fit for the downstream modelling tasks
    dataset_pros: str
    # what are the disadvantages of including this dataset
    # like biases
    dataset_cons: str

    # the languages that are present from the source download
    languages: list[str]
    # the programming languages that are present from the source download
    coding_languages: list[str]
    # the language modalities that are present in the dataset:
    # like discussion, code_review, source_code, unittest
    modalities: list[MODALITY]
    # to track copyright 
    source_license: str
    # a citation for acknowledging the data source
    # as this is convention in academia
    source_citation: str
    # a single person responsible for the dataset
    data_owner: str
    contributers: list[str]


# todo, change this by factoring out scraping and downloading and collecting
SourceType = Literal['bulk', 'api', 'staticpages', 'dynamicpages']


class DatasetSources(BaseModel):
    # stores the urls from where the data can be collected
    sources : list[AnyUrl]
    sourcetype : SourceType
    # storage format of the blobs that are captured from the source
    source_format: str 


class RawDataset(BaseModel):
    # where the raw dataset files is stored after the scrape
    storage_uris: list[Union[AnyUrl, FileUrl]]
    # hashes of t
    storage_hashes: Optional[dict[Union[AnyUrl, FileUrl], str]]
    # possible locks for parallel writing to the storage_uris
    storage_locks: Optional[list[Any]]
    # wether the download is complete
    # if more finegrained saving of state is needed, handle it customly
    # in the scraper
    complete: bool = False
    
    # miscellanous metadata we additionally want to track
    metadata: Optional[str]


class Scraper(ABC):
    # logic for downloading/scraping the datasets
    def __init__(self, config, dataset_id: str, *args, **kwargs):
        self.config = config
        self.dataset_id = dataset_id

    def scrape(self) -> RawDataset:
        raise NotImplementedError()


class Processor(ABC):
    # logic for processing the datasets
    # filtering out bad data
    # data transformations
    # if you wanna use a kind of workflow system for speed, implement it in here
    def __init__(self, config, dataset_id: str, *args, **kwargs):
        self.config = config
        self.dataset_id = dataset_id

    def process(self, raw_data: RawDataset, *args, **kwargs):
        raise NotImplementedError()


class Analyser(ABC):
    # logic for getting basic statistics of the dataset
    def analyse(self, config):
        self.config = config
        raise NotImplementedError()


class Dataset(ABC):
    def __init__(self, config, *args, **kwargs):
        self.config = config

        self.info : DatasetInfo = None

        self.scraper = None
        self.processor = None 
        self.analyser = None

    def download(self, *args, **kwargs) -> RawDataset:
        self.raw_data = self.scraper.scrape()

        p = os.path.join(self.config.tmpdir, self.info.id)
        os.makedirs(p, exist_ok=True)
        with open(os.path.join(p, 'raw_data.json'), 'w') as f:
            f.write(self.raw_data.json())

        return self.raw_data

    def process(self, *args, **kwargs):
        p = os.path.join(self.config.tmpdir, self.info.id)
        with open(os.path.join(p, 'raw_data.json'), 'r') as f:
            raw_data = RawDataset.parse_raw(f.read())
        self.processor.process(raw_data, *args, **kwargs)

    def analyse(self, *args, **kwargs):
        self.analyser.analyse()

    @property
    @abstractmethod
    def info(self) -> DatasetInfo:
        return self.info

    @property
    @abstractmethod
    def id(self) -> str:
        # if datasetinfo will be populated later,
        # hardcode this value for now, but it needs to be unique
        # to prevent processing conflicts
        return self.info.id




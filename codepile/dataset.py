from typing import Union, Optional, TypeAlias, Literal, Any
from abc import ABC
import uuid
import pydantic
from pydantic import BaseModel, AnyUrl
from datetime import datetime


MODALITY = Literal['discussion', 'code_review', 'source_code', 'unittest']

class DatasetInfo(BaseModel):
    identifier: str
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


SourceType = Literal['bulk', 'api', 'staticpages', 'dynamicpages']

class DatasetSources(BaseModel):
    # stores the urls from where the data can be collected
    sources : list[AnyUrl]
    sourcetype : SourceType
    # storage format of the blobs that are captured from the source
    source_format: str 


class RawDataset(BaseModel):
    # where the raw dataset files is stored after the scrape
    storage_uris: list[AnyUrl]
    # possible locks for parallel writing to the storage_uris
    storage_locks: list[Any]
    # wether the download is complete
    # if more finegrained saving of state is needed, handle it customly
    # in the scraper
    complete: bool


class Scraper(ABC):
    # logic for downloading/scraping the datasets
    pass


class Processor(ABC):
    # logic for processing the datasets
    # filtering out bad data
    # data transformations
    # if you wanna use kind a workflow, implement it in here
    pass


class Analyser(ABC):
    # logic for getting basic statistics of the dataset
    pass


class Merger(ABC):
    # for merging datasets, not for assembling a single dataset
    # can use Analyser for 
    pass


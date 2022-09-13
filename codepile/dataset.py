from typing import Union, Optional, TypeAlias, Literal
import uuid
import pydantic
from pydantic import BaseModel, AnyUrl
from datetime import datetime


BulkDownloadSource: TypeAlias = AnyUrl
ApiDownloadSource: TypeAlias = AnyUrl
StaticScrapeSource: TypeAlias = AnyUrl
RenderedScrapeSource: TypeAlias = AnyUrl

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
    # storage format of the blobs that are captured from the source
    storage_format: str 

    # where the data for this dataset is currently stored
    #storage_uri: str

    # compute cost
    # usefull information for rebuilding
    cpu_hours: Optional[int]
    gpu_hours: Optional[int]
    ram_requirement: Optional[int]
    tempfile_requirement: Optional[int]

    # how the source data was obtained
    source: Union[BulkDownloadSource, ApiDownloadSource, StaticScrapeSource, RenderedScrapeSource]

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


'''
# example
StackOverflowInfo = DatasetInfo(
        identifier='name',
        description='s',
        data_end=datetime(2022,1,1),
        data_start=10,
        size=10,
        storage_format='tar',
        #storage_uri='/root',
        cpu_hours=1,
        gpu_hours=1,
        ram_requirements=1,
        tempfile_requirement=1,
        source='src',
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
        '''

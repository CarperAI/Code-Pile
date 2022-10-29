"""The Pile V2 dataset."""

import json

import datasets
from boto3.session import Session
import boto3
S3_BUCKET = "s-eai-neox"

_DESCRIPTION = "PileV2"
# _DATA_URLS = {
#     "Pubmed Central": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/data_0_time1666921283_PubMedDataset.jsonl.zst",
#     "Books3": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/data_0_time1666905734_Books3.jsonl.zst",
#     "Project Gutenberg": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/gutenberg.jsonl.zst",
#     "FreeLaw": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/FreeLaw_Opinions.jsonl.zst",
#     "Ubuntu IRC": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/ubuntu_irc_until_2020_9_1.jsonl.zst",
#     "Wikipedia (en)": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/enwiki_20221006_lmdataformat.jsonl.zst",
#     "EuroParl": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/EuroParliamentProceedings_1996_2011.jsonl.zst",
#     "DM Mathematics": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/data_0_time1666879910_DMMathDataset.jsonl.zst",
#     "Apache Software Foundation Public Mail Archives": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/data_0_time1666883223_ASFPublicMail.jsonl.zst",
#     "StackExchange": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666432830_StackExchangeDataset.jsonl.zst",
#     "Comptetitive Programming": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666186790_CPDataset.jsonl.zst",
#     "WikiBooks Computing": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666163947_WikiBookDataset.jsonl.zst",
#     "Usenet": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666343531_UsenetDataset.jsonl.zst",
#     "AI4Code notebooks": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666171903_AI4Code_Kaggle.jsonl.zst",
#     "The Stack": "s3://s-eai-neox/data/codepile/lm_dataformat_pilev2/the_stack_dedup/data_0_time1666956816_default.jsonl.zst",
#     "LeetCode Forum": "s3://s-eai-neox/data/codepile/lm_dataformat_codepile/data_0_time1666600580_LeetCodeDataset.jsonl.zst"
# }

_DATA_URLS = {
    "DM Mathematics": "data/codepile/lm_dataformat_pilev2/data_0_time1666879910_DMMathDataset.jsonl.zst",
    "Ubuntu IRC": "data/codepile/lm_dataformat_pilev2/ubuntu_irc_until_2020_9_1.jsonl.zst",
}

_FEATURES = {
    "Pubmed Central": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Books3": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Project Gutenberg": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "FreeLaw": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Ubuntu IRC": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Wikipedia (en)": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "EuroParl": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "DM Mathematics": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Apache Software Foundation Public Mail Archives": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "StackExchange": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Comptetitive Programming": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "WikiBooks Computing": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "Usenet": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "AI4Code notebooks": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "The Stack": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    ),
    "LeetCode Forum": datasets.Features(
        {
            "text": datasets.Value("string"),
            "meta": datasets.Value("string"),
        }
    )
}

_CITATION = """\
@inproceedings{pile,
    title = {Pile: The Billion Word Benchmark for General Language Understanding
        }
}"""


class ThePileConfig(datasets.BuilderConfig):
    """BuilderConfig for The Pile."""

    def __init__(self, *args, subsets, **kwargs):
        """BuilderConfig for The Pile.
        Args:
            subsets (:obj:`List[str]`): List of subsets to load.
            **kwargs: keyword arguments forwarded to super.
        """
        super().__init__(
            *args,
            name="+".join(subsets),
            **kwargs,
        )
        self.subsets = subsets


class ThePile(datasets.GeneratorBasedBuilder):
    """The Pile dataset."""

    VERSION = datasets.Version("1.1.0")

    BUILDER_CONFIG_CLASS = ThePileConfig
    BUILDER_CONFIGS = [ThePileConfig(subsets=[subset]) for subset in _DATA_URLS]
    DEFAULT_CONFIG_NAME = "FreeLaw"

    def _info(self):
        """Give information and typings for the dataset."""
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=_FEATURES.get(self.config.name),
            # If there's a common (input, target) tuple from the features,
            # specify them here. They'll be used if as_supervised=True in
            # builder.as_dataset.
            supervised_keys=None,
            # Homepage of the dataset for documentation
            homepage=None,
            # License for the dataset if available
            license=None,#_LICENSES.get(self.config.name, "Multiple: see each subset license"),
            # Citation for the dataset
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        
        def download_s3(url: str, local_path: str):
            s3 = boto3.client('s3')
            s3.download_file(S3_BUCKET, url, local_path)
        
        
        """Return SplitGenerators."""
        data_urls = {subset: _DATA_URLS[subset] for subset in self.config.subsets}
        archive = dl_manager.download_custom(data_urls, download_s3)

        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={
                    "files": {
                        subset: dl_manager.iter_archive(archive[subset])
                        if ".tar" in data_urls[subset]
                        else archive[subset]
                        for subset in self.config.subsets
                    },
                },
            ),
        ]

    def _generate_examples(self, files):
        """Yield examples as (key, example) tuples."""
        key = 0
        if isinstance(files, list):
            import zstandard as zstd

            for path in files:
                with zstd.open(open(path, "rb"), "rt", encoding="utf-8") as f:
                    for row in f:
                        data = json.loads(row)
                        yield key, data
                        key += 1
        else:
            for subset in files:    
                import zstandard as zstd
                with zstd.open(open(files[subset], "rb"), "rt", encoding="utf-8") as f:
                    for row in f:
                        data = json.loads(row)
                        yield key, data
                        key += 1
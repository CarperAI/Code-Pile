from unittest import TestCase

import os

from codepile.codepile import Config
from codepile.usenet.usenet import UsenetDataset

import pyarrow.parquet as pq


class TestUsenetDataset(TestCase):
    def setUp(self):
        output_data_dir = 'data/output/'
        raw_data_dir = 'data/raw/'
        tmpdir = '/tmp/usenet/'

        if not os.path.exists(output_data_dir):
            os.makedirs(output_data_dir)

        if not os.path.exists(raw_data_dir):
            os.makedirs(raw_data_dir)

        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)

        config = Config(
            output_data_dir=output_data_dir,
            raw_data_dir=raw_data_dir,
            tmpdir=tmpdir,
        )

        usenet_dataset = UsenetDataset(config)
        # Testing on two of the smaller archives
        usenet_dataset.download(files=['comp.ai.games.mbox.zip', 'comp.lang.basic.visual.mbox.zip', ])

        self.test_data = pq.read_table('test/usenet-comp.parquet').to_pandas()
        self.df = pq.read_table(os.path.join(output_data_dir, 'usenet-comp.parquet')).to_pandas()

    def test_same_cols(self):
        self.setUp()
        self.assertEqual(self.test_data.columns.tolist(), self.df.columns.tolist())

import os

from pyspark.sql import SparkSession
from unittest import TestCase

from codepile.stackexchange.stackexchange import StackExchangeDataset
from codepile.codepile import Config
import pytest

class TestStackExchangeDataset(TestCase):
    def test_processor_row_count(self):
        this_dir = os.path.dirname(__file__)
        dump_dir = os.path.join(this_dir, "./data/dumps")
        temp_dir = os.path.join(this_dir, "./data/temp")
        config = Config(
            raw_data_dir=dump_dir,
            tmpdir=temp_dir,
            output_data_dir=temp_dir
        )
        se_dataset = StackExchangeDataset(config)

        se_dataset.process(force_unzip=True, force_xml_conversion=True, force_process=True)

        site1_output_dir = os.path.join(temp_dir, "eosio.meta.stackexchange.com", "questions")
        site1_output_success_file = os.path.join(site1_output_dir, "_SUCCESS")
        site2_output_dir = os.path.join(temp_dir, "tezos.meta.stackexchange.com", "questions")
        site2_output_success_file = os.path.join(site1_output_dir, "_SUCCESS")
        assert os.path.exists(site1_output_success_file), "Output files isn't generated"
        assert os.path.exists(site2_output_success_file), "Output files isn't generated"

        spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", temp_dir).config("spark.driver.memory", "8G").master("local[1]").appName('stackexchange-tests').getOrCreate()

        df1 = spark.read.parquet(site1_output_dir)
        df2 = spark.read.parquet(site2_output_dir)
        self.assertEqual(df1.count(), 27, "Output row count doesn't match")
        self.assertEqual(df2.count(), 28, "Output row count doesn't match")

from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import read_data_to_df


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("tpcds_first_query") \
            .getOrCreate()

    def test_dateDimDF(self):
        sample_df = read_data_to_df(self.spark, "query_1_data/date_dim/")
        result_count = sample_df.count()
        self.assertEqual(result_count, 366, "Record count should be 366")

    def test_customerDF(self):
        sample_df = read_data_to_df(self.spark, "query_1_data/customer/")
        result_count = sample_df.filter("c_customer_sk = 1658292").count()
        self.assertEqual(result_count, 1, "Record count should be 1")

    def test_storeReturnDF(self):
        sample_df = read_data_to_df(self.spark, "query_1_data/store_returns/")
        result_count = sample_df.filter("sr_customer_sk = 49886089").count()
        self.assertEqual(result_count, 1, "Record count should be 1")

    def test_storeDF(self):
        sample_df = read_data_to_df(self.spark, "query_1_data/store/")
        result_count = sample_df.count()
        self.assertEqual(result_count, 49, "Record count should be 49")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

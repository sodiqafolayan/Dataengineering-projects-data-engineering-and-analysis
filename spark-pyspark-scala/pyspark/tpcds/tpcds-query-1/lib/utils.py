from pyspark import SparkConf

import configparser


def read_data_to_df(spark, data):
    df = spark.read.format("parquet").load(data)
    return df


def tpcds_app_config():
    # Instantiating sparkConf object
    spark_conf = SparkConf()
    # Instantiating configParser
    config = configparser.ConfigParser()
    # Reading in the spark.conf file
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

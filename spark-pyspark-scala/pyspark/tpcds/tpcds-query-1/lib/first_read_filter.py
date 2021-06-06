# Databricks notebook source
store_data_list = ["s3://jwyant-tpcds/processed/30tb/store/",
                   "parquet",
                   ["s_store_sk", "s_store_id", "s_store_name",
                    "s_company_id", "s_company_name", "s_city",
                    "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset",
                    "s_tax_precentage"],
                   "s_state == 'TN'"]

store_returns_data_list = ["s3://jwyant-tpcds/processed/30tb/store_returns/",
                           "parquet",
                           ["sr_item_sk", "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk",
                            "sr_store_sk", "sr_return_quantity", "sr_return_amt", "sr_return_tax",
                            "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost",
                            "sr_refunded_cash", "sr_reversed_charge", "sr_store_credit",
                            "sr_net_loss", "sr_returned_date_sk"],
                           [310, 393, 535, 537, 555, 739, 816, 990, 1105, 1127, 1150, 1212, 1246,
                            1259, 1269, 1497, 1525, 1617, 192, 340, 404, 479, 658, 893, 895, 935,
                            945, 972, 977, 1102, 1397, 339, 491, 496, 538, 647, 662, 678, 784, 789,
                            908, 988, 997, 1273, 1296, 1324, 1491, 1514, 1556],
                           "sr_store_sk"]

customer_data_list = ["s3://jwyant-tpcds/processed/30tb/customer/",
                      ["c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk",
                       "c_first_name", "c_last_name", "c_last_review_date_sk"]]

date_dimension_data_list = ["s3://jwyant-tpcds/processed/30tb/date_dim/",
                            "parquet",
                            ["*"],
                            "d_year == 2000"]


# COMMAND ----------

def read_filter_df(spark, data_list: list):
    mnt_point = "/mnt/tpc-ds-2021/"
    bucket_ext = data_list[0].split("/")[-2]
    if len(data_list) == 5:
        assert type(data_list[1]) and type(data_list[0]) and type(
            data_list[-1]) == str, "Wrong data type! Usage: [str, str, list, list, str]"
        assert type(data_list[2]) and type(data_list[3]) == list, "Wrong data type! Usage: [str, str, list, list, str]"
        df = spark.read.format(data_list[1]) \
            .load(data_list[0]) \
            .select(*data_list[2]) \
            .filter(col(data_list[-1]).isin(data_list[3])) \
            .sample(0.1)

    elif len(data_list) == 4:
        assert type(data_list[1]) and type(data_list[0]) and type(
            data_list[-1]) == str, "Wrong data type! Usage: [str, str, list, str]"
        assert type(data_list[2]) == list, "Wrong data type! Usage: [str, str, list, str]"
        df = spark.read.format(data_list[1]) \
            .load(data_list[0]) \
            .select(*data_list[2]) \
            .filter(data_list[-1])
    else:
        assert len(data_list) == 2, "The length of the provided list should be 2"
        assert type(data_list[0]) == str, "Wrong data type! Usage: [str, list]"
        assert type(data_list[-1]) == list, "Wrong data type! Usage: [str, list]"
        df = spark.read.format("parquet") \
            .load(data_list[0]) \
            .select(*data_list[1]) \
            .sample(0.1)
    df.write.format("parquet").mode("overwrite").save(mnt_point + bucket_ext)
    return df


# COMMAND ----------

dateDimDF = read_data_to_df(spark, date_dimension_data_list)
customerDF = read_data_to_df(spark, customer_data_list)
storeReturnDF = read_data_to_df(spark, store_returns_data_list)
storeDF = read_data_to_df(spark, store_data_list)


# COMMAND ----------

def read_filter_df(spark, data_list: list):
    mnt_point = "/mnt/tpc-ds-2021/"
    bucket_ext = data_list[0].split("/")[-2]
    df = spark.read.format(data_list[1]) \
        .load(data_list[0]) \
        .select(*data_list[2]) \
        .filter(col(data_list[-1]).isin(data_list[3])) \
        .sample(0.1)
    df.write.format("parquet").partitionBy("sr_store_sk").mode("overwrite").save(mnt_point + bucket_ext)
    return df

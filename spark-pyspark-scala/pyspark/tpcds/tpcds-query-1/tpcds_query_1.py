from pyspark.sql import SparkSession

from pyspark.sql.functions import broadcast, concat_ws

from lib.tpcds_logger import TpcdsLog4J
from lib.utils import tpcds_app_config, read_data_to_df


if __name__ == "__main__":
    conf = tpcds_app_config()
    spark = spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = TpcdsLog4J

    dateDimDF = read_data_to_df(spark, "query_1_data/date_dim/")
    customerDF = read_data_to_df(spark, "query_1_data/customer/")
    storeReturnDF = read_data_to_df(spark, "query_1_data/store_returns/")
    storeDF = read_data_to_df(spark, "query_1_data/store/")

    mm = storeReturnDF.filter("sr_customer_sk = 49886089").count()
    print(mm)

    cc = customerDF.filter("c_customer_sk = 1658292").count()
    print(cc)
    # customerDF.show(5, False)
    # dateDimDF.show(5, False)
    # customerDF.show(5, False)
    # storeReturnDF.show(5, False)
    # storeDF.show(5, False)

    c_sr_join_expr = storeReturnDF.sr_customer_sk == customerDF.c_customer_sk
    s_sr_join_expr = storeReturnDF.sr_store_sk == storeDF.s_store_sk
    d_sr_join_expr = dateDimDF.d_date_sk == storeReturnDF.sr_returned_date_sk

    cus_sr_s_daDF = (storeReturnDF.select("sr_store_sk", "sr_customer_sk",
                                          "sr_return_amt", "sr_returned_date_sk")
                     .join(customerDF.select("c_customer_sk", "c_first_name", "c_last_name",
                                             concat_ws(" ", "c_first_name", "c_last_name")
                                             .alias("full_name")), c_sr_join_expr)
                     .drop("sr_customer_sk", "c_first_name", "c_last_name")
                     .join(broadcast(storeDF.select("s_store_sk", "s_store_id",
                                                    "s_state")), s_sr_join_expr).drop("sr_store_sk")
                     .join(broadcast(dateDimDF.select("d_date_sk", "d_date_id",
                                                      "d_date")), d_sr_join_expr).drop("d_date"))

    cus_sr_s_daDF.createOrReplaceTempView("c_sr_s_d_temp")

    average_returnDF = spark.sql("""
            SELECT full_name,
                   ROUND(AVG(sr_return_amt), 2) average_return
            FROM c_sr_s_d_temp
            GROUP BY full_name
            HAVING average_return > (SELECT ROUND(AVG(sr_return_amt), 2 * 1.2) FROM c_sr_s_d_temp)""")

    average_returnDF.write.format("parquet").mode("overwrite").save("query_1_result/")

    # user = input()

    # storeReturnDF.select(avg("sr_return_amt")).show()
    # storeDF = read_filter_df(spark, store_data_list)  # 49 Rows
    # storeReturnDF = read_filter_df(spark, store_returns_data_list)  # 6,711,683 Rows
    # customerDF = read_filter_df(spark, customer_data_list)  # 8,003,401
    # dateDimensionDF = read_filter_df(spark, date_dimension_data_list)  # 366

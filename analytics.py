
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as spark_data_types
from pyspark.sql.window import Window



spark = SparkSession.builder.master("local[1]") \
          .appName("SparkByExamples.com") \
          .getOrCreate()

df = spark.read.json("transactions.json")

df.withColumn("account_id",col("account_id.$numberInt") )\
    .withColumn("bucket_end_date",col("bucket_end_date.$date.$numberLong")) \
    .withColumn("bucket_start_date",col("bucket_start_date.$date.$numberLong")) \
    .withColumn("transactions",explode("transactions")) \
    .select("account_id","bucket_end_date","bucket_start_date","transactions.amount.$numberInt","transactions.price","transactions.total","transactions.transaction_code") \
    .withColumnRenamed("account_id","key") \
    .withColumnRenamed("$numberInt","amount").createOrReplaceTempView("tb_transactions")


batch_df = spark.sql("select * from tb_transactions")
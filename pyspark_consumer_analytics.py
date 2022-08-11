from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as spark_data_types
from pyspark.sql.window import Window
import os

# setup kafka host
kafka__servers = (
    os.getenv("KAFKA_HOST")
    if os.getenv("KAFKA_HOST") is not None
    else "localhost:29092"
)

# setup kafka topic that will be used
kafka_topic = (
    os.environ.get("KAFKA_TOPIC")
    if os.environ.get("KAFKA_TOPIC") is not None
    else "transactions"
)


def sink_to_kafka(df, topic, kafka__servers):
    """
    insert dataframe into kafka topic
    """

    df.select("key", to_json(struct("*")).alias("json_datas")).withColumnRenamed(
        "json_datas", "value"
    ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format(
        "kafka"
    ).option(
        "kafka.bootstrap.servers", kafka__servers
    ).option(
        "topic", topic
    ).option(
        "checkpointLocation",
        "/tmp/kafka-checkpointLocation/{}".format(topic),
    ).save()


def aggregation_processing(batch_df, batch_id):

    """
    datas schema that will be aggregated in this functions
    +----------+---------------+-----------------+------+--------------------+--------------------+----------------+
    |account_id|bucket_end_date|bucket_start_date|amount|               price|               total|transaction_code|
    +----------+---------------+-----------------+------+--------------------+--------------------+----------------+
    |    903651|  1482796800000|     149385600000|  6521|144.4148580461602...|941729.2893190111...|            sell|
    |    903651|  1482796800000|     149385600000|  2655|103.4539861839674...|274670.3333184335...|            sell|
    |    903651|  1482796800000|     149385600000|  5606|21.15933960953433...|118619.2578510494...|             buy|
    |    903651|  1482796800000|     149385600000|  5574|24.94773935980459...|139058.6991915507...|            sell|
    |    903651|  1482796800000|     149385600000|  8514|22.49516145956553...|191523.8046667409...|            sell|
    """
    # aggregation_query_withgroup basic agregation with group
    df_aggregation_query_withgroup = (
        batch_df.select("key", "total")
        .groupBy("key")
        .agg(mean("total"), count("total"))
    )
    
    sink_to_kafka(
        df_aggregation_query_withgroup, "aggregation_query_withgroup", kafka__servers
    )

    # aggregation_with_pivoting create pivot table
    df_aggregation_with_pivoting = (
        batch_df.select("key", "transaction_code", "total")
        .withColumn("total", batch_df.total.cast(spark_data_types.DoubleType()))
        .groupBy("key")
        .pivot("transaction_code")
        .sum("total")
    )
    sink_to_kafka(
        df_aggregation_with_pivoting, "aggregation_with_pivoting", kafka__servers
    )

    # ranking_functions using row_number
    windowSpec = Window.partitionBy("key").orderBy("total")
    df_ranking_functions = batch_df.select("key", "total").withColumn(
        "row_number", row_number().over(windowSpec)
    )

    sink_to_kafka(df_ranking_functions, "ranking_functions", kafka__servers)

    # analytic_functions with cume_dist
    df_analytic_functions = batch_df.select("key", "total").withColumn(
        "cume_dist", cume_dist().over(windowSpec)
    )
    sink_to_kafka(df_analytic_functions, "analytic_functions", kafka__servers)

    # use rollups for aggregation
    df_rollups = batch_df.select("key", "total").rollup("key").agg(avg("total"))
    sink_to_kafka(df_rollups, "rollups", kafka__servers)

    # use cube for aggregation
    df_cube = batch_df.select("key", "total").cube("key").agg(avg("total"))
    sink_to_kafka(df_cube, "cube", kafka__servers)


# initialize spark session
spark = (
    SparkSession.builder.config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    )
    .appName("analytics_consumer")
    .master("local[*]")
    .getOrCreate()
)


# read data from kafka
df_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka__servers)
    .option("failOnDataLoss", "false")
    .option("subscribe", kafka_topic)
    .load()
)

# define schema that will be used for dataframe
json_datas = """
{
    "_id": {
      "$oid": "5ca4bbc1a2dd94ee58161cb1"
    },
    "account_id": {
      "$numberInt": "443178"
    },
    "transaction_count": {
      "$numberInt": "66"
    },
    "bucket_start_date": {
      "$date": {
        "$numberLong": "-28598400000"
      }
    },
    "bucket_end_date": {
      "$date": {
        "$numberLong": "1483401600000"
      }
    },
    "transactions": [
      {
        "date": {
          "$date": {
            "$numberLong": "1063065600000"
          }
        },
        "amount": {
          "$numberInt": "7514"
        },
        "transaction_code": "buy",
        "symbol": "adbe",
        "price": "19.1072802650074180519368383102118968963623046875",
        "total": "143572.1039112657392422534031"
      }
    ]
  }
"""

# convert string json to spark struct
df_for_get_schema = spark.read.json(spark.sparkContext.parallelize([json_datas]))


# transform data from kafka to schema that already define
df_temp = df_from_kafka.selectExpr("CAST(value AS STRING)", "timestamp")
df = df_temp.select(
    from_json(col("value"), df_for_get_schema.schema).alias("transactions"), "timestamp"
)

df.printSchema()
df.select("transactions.*", "timestamp").withWatermark(
    "timestamp", "5 seconds"
).createOrReplaceTempView("tb_transactions")


df_transformed = (
    spark.sql("select * from tb_transactions")
    .withColumn("account_id", col("account_id.$numberInt"))
    .withColumn("bucket_end_date", col("bucket_end_date.$date.$numberLong"))
    .withColumn("bucket_start_date", col("bucket_start_date.$date.$numberLong"))
    .withColumn("transactions", explode("transactions"))
    .select(
        "account_id",
        "bucket_end_date",
        "bucket_start_date",
        "transactions.amount.$numberInt",
        "transactions.price",
        "transactions.total",
        "transactions.transaction_code",
    )
    .withColumnRenamed("account_id", "key")
    .withColumnRenamed("$numberInt", "amount")
)


query = df_transformed.writeStream.foreachBatch(aggregation_processing).start()

query.awaitTermination()

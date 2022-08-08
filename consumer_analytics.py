from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as spark_data_types
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

kafka_topic_name = "ds_salaries_2"
kafka_bootstrap_servers = "localhost:29092"

spark = (
    SparkSession.builder.config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    )
    .appName("spark_app_name")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("failOnDataLoss", "false")
    .option("subscribe", kafka_topic_name)
    .load()
)

df_temp = df_from_kafka.selectExpr("CAST(value AS STRING)", "timestamp")
datas_schema = "id INT,work_year DOUBLE,experience_level STRING,employment_type STRING,job_title STRING,salary DOUBLE,salary_currency STRING,salary_in_usd DOUBLE, employee_residence STRING,remote_ratio STRING,company_location STRING,company_size STRING"

df = df_temp.select(from_csv(col("value"), datas_schema).alias("salaries"), "timestamp")

df.printSchema()
df.select("salaries.*", "timestamp").withWatermark(
    "timestamp", "5 seconds"
).createOrReplaceTempView("tb_salaries")


def process_row(batch_df, batch_id):

    def write_tokafka(df,topic):
        df.select(
        "key", to_json(struct("*")).alias("json_datas")
    ).withColumnRenamed(
        "json_datas", "value"
    ).selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)"
    ).write.format(
        "kafka"
    ).option(
        "kafka.bootstrap.servers", kafka_bootstrap_servers
    ).option(
        "topic", topic
    ).option(
        "checkpointLocation",
        "/tmp/kafka-checkpointLocation/{}".format(topic),
    ).save()

    # aggregation_query_withgroup
    df_aggregation_query_withgroup = batch_df.select("job_title", "salary_in_usd").withColumnRenamed(
        "job_title", "key"
    ).groupBy("key").agg(mean("salary_in_usd"), count("salary_in_usd"))

    write_tokafka(df_aggregation_query_withgroup,"aggregation_query_withgroup")

    # aggregation_with_pivoting
    df_aggregation_with_pivoting = batch_df.withColumnRenamed("id", "key").select(
        "key", "job_title", "salary_in_usd"
    ).withColumn(
        "salary_in_usd", batch_df.salary_in_usd.cast(spark_data_types.DoubleType())
    ).groupBy(
        "key"
    ).pivot(
        "job_title"
    ).sum(
        "salary_in_usd"
    )
    write_tokafka(df_aggregation_with_pivoting,"aggregation_with_pivoting")


    # ranking_functions
    windowSpec = Window.partitionBy("job_title").orderBy("salary_in_usd")
    df_ranking_functions = batch_df.select("job_title", "salary_in_usd").withColumn(
        "row_number", row_number().over(windowSpec)
    ).withColumnRenamed("job_title", "key")

    write_tokafka(df_ranking_functions,"ranking_functions")

    # analytic_functions
    df_analytic_functions = batch_df.select("job_title", "salary_in_usd").withColumn(
        "cume_dist", cume_dist().over(windowSpec)
    ).withColumnRenamed("job_title", "key")

    write_tokafka(df_analytic_functions,"analytic_functions")

    # rollups
    df_rollups = batch_df.select("job_title","salary_in_usd").withColumnRenamed("job_title","key").rollup("key").agg(mean('salary_in_usd'), count('salary_in_usd'))
    write_tokafka(df_rollups,"rollups")

    # cube
    df_cube = batch_df.select("job_title","salary_in_usd").withColumnRenamed("job_title","key").cube("key").agg(mean('salary_in_usd'), count('salary_in_usd'))
    write_tokafka(df_cube,"cube")



query = (
    spark.sql("select * from tb_salaries").writeStream.foreachBatch(process_row).start()
)
query.awaitTermination()

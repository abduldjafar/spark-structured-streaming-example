from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import os
 


def writeStreamToKafka(spark,query,topic,checkpointLocation,kafka_bootstrap_servers):
    write_stream = spark.sql(query) \
        .select("key",to_json(struct("*")).alias("json_datas")) \
        .withColumnRenamed("json_datas","value") \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
            .format("kafka") \
            .outputMode("complete") \
            .option("kafka.bootstrap.servers",kafka_bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpointLocation) \
            .start()
    return write_stream

def run_spark(spark_app_name,query_file):
    kafka_topic_name = "ds_salaries_2"
    kafka_bootstrap_servers = 'localhost:29092'

    spark = SparkSession \
            .builder \
            .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
            .appName(spark_app_name) \
            .master("local[*]") \
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df_from_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("failOnDataLoss", "false") \
            .option("subscribe", kafka_topic_name) \
            .load()

    df_temp = df_from_kafka.selectExpr("CAST(value AS STRING)", "timestamp")
    datas_schema = "id INT,work_year DOUBLE,experience_level STRING,employment_type STRING,job_title STRING,salary DOUBLE,salary_currency STRING,salary_in_usd DOUBLE, employee_residence STRING,remote_ratio STRING,company_location STRING,company_size STRING"

    df = df_temp \
            .select(from_csv(col("value"), datas_schema) \
                    .alias("salaries"), "timestamp")

    df.printSchema()

    df.select("salaries.*","timestamp").withWatermark("timestamp", "5 seconds").createOrReplaceTempView("tb_salaries_{}".format(spark_app_name))
 
    #check if file is present
    if os.path.isfile(query_file):
        #open text file in read mode
        text_file = open(query_file, "r")
    
        #read whole file to a string
        aggregation_query = text_file.read()
    
        #close file
        text_file.close()
    

     

    aggregation_processing = writeStreamToKafka(spark,aggregation_query,spark_app_name,"/tmp/spark-temp/{}".format(query_file),kafka_bootstrap_servers)
    aggregation_processing.awaitTermination()

if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser()
    
    # Adding optional argument
    parser.add_argument("-a", "--App", help = "app name")
    parser.add_argument("-q", "--Query", help = "query file")
    
    # Read arguments from command line
    args = parser.parse_args()
    
    if args.App and args.Query:
        print("Displaying Output as: % s" % args.App)
        run_spark(spark_app_name=args.App,query_file=args.Query)

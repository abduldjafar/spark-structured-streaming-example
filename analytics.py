
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as spark_data_types
from pyspark.sql.window import Window



spark = SparkSession.builder.master("local[1]") \
          .appName("SparkByExamples.com") \
          .getOrCreate()

df = spark.read.option("header",True).csv("ds_salaries.csv")
df.printSchema()

df.createOrReplaceTempView("tb_salaries")

aggregation_with_grouping = spark.sql(
    """
            select 
            job_title as key,
            avg(salary_in_usd) as value
        from tb_salaries
        group by key
        
    """
)

df.select("job_title", "salary_in_usd").withColumnRenamed("job_title","key").groupBy("key").agg(mean('salary_in_usd'), count('salary_in_usd'))


aggregation_with_pivoting = spark.sql(
    """
    SELECT tb_salaries.*,tb_salaries.id as key FROM tb_salaries
    PIVOT (
        SUM(salary_in_usd) AS sum_salary_in_usd, AVG(salary_in_usd) AS avg_salary_in_usd
        FOR job_title IN ("Data Scientist" as data_scientist)
    )
    """
).select("key",to_json(struct("*"))).withColumnRenamed("to_json(struct(key, value))","value")

df.select("id","job_title","salary_in_usd").withColumn("salary_in_usd",df.salary_in_usd.cast(spark_data_types.DoubleType())).groupBy("id").pivot("job_title").sum("salary_in_usd")

ranking_functions = spark.sql(
    """
        select 
            job_title, 
            salary_in_usd,
            rank() over (
                partition by job_title
                order by salary_in_usd
            ) as rnk
        from tb_salaries
    """
)

windowSpec  = Window.partitionBy("job_title").orderBy("salary_in_usd")
df.select("job_title","salary_in_usd").withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)


analytic_functions = spark.sql(
    """
        select 
            job_title, 
            salary_in_usd,
            cume_dist() over (
                partition by job_title
                order by salary_in_usd
            ) as cume_dist
        from tb_salaries
    """
)



aggregation_with_rollups = spark.sql(
    """
    select 
            job_title as key,
            avg(salary_in_usd) as value
        from tb_salaries
        group by rollup(key)
    """
)

df.select("job_title","salary_in_usd").withColumnRenamed("job_title","key").cube("key").agg(mean('salary_in_usd'), count('salary_in_usd')).show()
aggregation_with_cube = spark.sql(
    """
    select 
            job_title as key,
            avg(salary_in_usd) as value
        from tb_salaries
        group by cube(key)
    """
).select("key",to_json(struct("*"))).withColumnRenamed("to_json(struct(key, value))","value")
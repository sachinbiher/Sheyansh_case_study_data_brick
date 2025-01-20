# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/mnt/raw/datasetv01  /")

# COMMAND ----------

average_list = []


for i in range(1,len(df.columns)):
    df = df.withColumn(f"V{i}", col(f"V{i}").cast(DoubleType()))
    avg = df.agg({f'V{i}': 'avg'}).collect()[0][0]
    df = df.fillna(value=avg,subset=[f"V{i}"])

display(df)

# COMMAND ----------

df = df.withColumn("ETLLastModifiedDate", current_timestamp())

# COMMAND ----------

create_schema_query = """CREATE SCHEMA IF NOT EXISTS databricks_case_study_99.raw_lnding"""
spark.sql(create_schema_query)

# COMMAND ----------

create_table_query = """CREATE OR REPLACE TABLE databricks_case_study_99.raw_lnding.datasetv01
(
    Time	 INT,
    V1	 DOUBLE,
    V2	 DOUBLE,
    V3	 DOUBLE,
    V4	 DOUBLE,
    V5	 DOUBLE,
    V6	 DOUBLE,
    V7	 DOUBLE,
    V8	 DOUBLE,
    V9	 DOUBLE,
    V10	 DOUBLE,
    V11	 DOUBLE,
    V12	 DOUBLE,
    ETLLastModifiedDate TIMESTAMP
)
using delta
"""
spark.sql(create_table_query)

# COMMAND ----------

df.write.format("delta").mode('append').saveAsTable("databricks_case_study_99.raw_lnding.datasetv01")
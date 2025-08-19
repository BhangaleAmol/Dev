# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True).printSchema()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/airlines', recurse=True)

# COMMAND ----------

spark. \
    read. \
    csv('dbfs:/databricks-datasets/asa/airlines', header=True). \
    groupBy('Year', 'Month', 'DayOfMonth'). \
    count(). \
    write. \
    parquet(f'/user/{username}/airlines', mode='overwrite')

# COMMAND ----------



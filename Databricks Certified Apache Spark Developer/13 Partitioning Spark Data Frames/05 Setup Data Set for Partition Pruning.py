# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/asa/airlines', recurse=True)

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True). \
    write. \
    partitionBy('Year'). \
    csv(f'/user/{username}/asa/airlines', header=True, mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/asa/airlines/')

# COMMAND ----------

spark.read.csv(f'/user/{username}/asa/airlines/', header=True).count()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/asa/airlines/Year=2004')

# COMMAND ----------



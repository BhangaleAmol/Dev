# Databricks notebook source
import getpass
username = getpass.getuser()

# COMMAND ----------

df = spark.read.parquet(f'/user/{username}/airlines')

# COMMAND ----------

len(df.inputFiles())

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/airlines')

# COMMAND ----------



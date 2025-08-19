# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC * `persist`
# MAGIC * `cache` => **persist** with **MEMORY_AND_DISK**.

# COMMAND ----------

df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

help(df.persist)

# COMMAND ----------

help(df.cache)

# COMMAND ----------

from pyspark import StorageLevel

# COMMAND ----------

help(StorageLevel)

# COMMAND ----------



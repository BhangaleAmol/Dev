# Databricks notebook source
df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.write

# COMMAND ----------

help(df.write.partitionBy)

# COMMAND ----------

# json does not have keyword argument related to partitioning
help(df.write.json)

# COMMAND ----------

# parquet have keyword argument partitionBy
help(df.write.parquet)

# COMMAND ----------



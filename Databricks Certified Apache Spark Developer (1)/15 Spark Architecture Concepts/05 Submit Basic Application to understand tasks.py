# Databricks notebook source
df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.count()

# COMMAND ----------



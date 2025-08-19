# Databricks notebook source
orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Partition data by date

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

orders. \
    withColumn('order_date', date_format('order_date', 'yyyyMMdd')). \
    show()

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/retail_db/orders_partitioned_by_date', recurse=True)

# COMMAND ----------

orders. \
    withColumn('order_date', date_format('order_date', 'yyyyMMdd')). \
    coalesce(1). \
    write. \
    partitionBy('order_date'). \
    parquet(f'/user/{username}/retail_db/orders_partitioned_by_date')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db/orders_partitioned_by_date')

# COMMAND ----------

orders.count()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_date').dtypes

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_date').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_date').count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Partition data by month

# COMMAND ----------

orders. \
    withColumn('order_month', date_format('order_date', 'yyyyMM')). \
    show()

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/retail_db/orders_partitioned_by_month', recurse=True)

# COMMAND ----------

orders. \
    withColumn('order_month', date_format('order_date', 'yyyyMM')). \
    coalesce(1). \
    write. \
    partitionBy('order_month'). \
    parquet(f'/user/{username}/retail_db/orders_partitioned_by_month')

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/retail_db/orders_partitioned_by_month', recurse=True)

# COMMAND ----------

orders. \
    withColumn('order_month', date_format('order_date', 'yyyyMM')). \
    coalesce(1). \
    write. \
    parquet(f'/user/{username}/retail_db/orders_partitioned_by_month', partitionBy='order_month')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db/orders_partitioned_by_month')

# COMMAND ----------

orders.count()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_month').dtypes

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_month').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db/orders_partitioned_by_month').count()

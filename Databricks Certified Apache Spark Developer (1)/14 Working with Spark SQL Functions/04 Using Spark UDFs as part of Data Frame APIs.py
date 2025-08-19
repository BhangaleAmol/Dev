# Databricks notebook source
help(spark.udf.register)

# COMMAND ----------

df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.show()

# COMMAND ----------

dc = spark.udf.register('date_convert', lambda d: int(d[:10].replace('-', '')))

# COMMAND ----------

dc

# COMMAND ----------

df.select(dc('order_date').alias('order_date')).show()

# COMMAND ----------

df.filter(dc('order_date') == 20140101).show()

# COMMAND ----------

df. \
    groupBy(dc('order_date').alias('order_date')). \
    count(). \
    withColumnRenamed('count', 'order_count'). \
    show()

# COMMAND ----------



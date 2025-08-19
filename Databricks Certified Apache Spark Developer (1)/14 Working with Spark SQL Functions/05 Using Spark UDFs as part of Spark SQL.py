# Databricks notebook source
help(spark.udf.register)

# COMMAND ----------

dc = spark.udf.register('date_convert', lambda d: int(d[:10].replace('-', '')))

# COMMAND ----------

df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.selectExpr('date_convert(order_date) AS order_date').show()

# COMMAND ----------

dc

# COMMAND ----------

df.select(dc('order_date')).show()

# COMMAND ----------

df.createOrReplaceTempView('orders')

# COMMAND ----------

spark.sql('''
    SELECT o.*, date_convert(order_date) AS order_date_as_int
    FROM orders AS o
'''). \
    show()

# COMMAND ----------

spark.sql('''
    SELECT o.*, date_convert(order_date) AS order_date_as_int
    FROM orders AS o
    WHERE date_convert(order_date) = 20140101
'''). \
    show()

# COMMAND ----------

spark.sql('''
    SELECT date_convert(order_date) AS order_date, count(*) AS order_count
    FROM orders AS o
    GROUP BY 1
'''). \
    show()

# COMMAND ----------



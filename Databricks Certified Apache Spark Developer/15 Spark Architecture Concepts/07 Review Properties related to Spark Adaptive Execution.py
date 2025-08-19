# Databricks notebook source
spark

# COMMAND ----------

spark.conf

# COMMAND ----------

spark.conf.get('spark.master')

# COMMAND ----------

spark.conf.set('spark.master', 'local')

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------



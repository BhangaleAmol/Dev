# Databricks notebook source
from datetime import datetime
from pyspark import StorageLevel

# COMMAND ----------

# SPARK CONFIG
spark.conf.set('spark.databricks.optimizer.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.databricks.io.cache.enabled', 'true')
spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)

# COMMAND ----------

# VARIABLES
run_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

# COMMAND ----------

# MAGIC %run ./global_variables

# COMMAND ----------

# MAGIC %run ./func_azure_data_factory

# COMMAND ----------

# MAGIC %run ./func_helpers

# COMMAND ----------

# MAGIC %run ./func_input

# COMMAND ----------

# MAGIC %run ./func_transformation

# COMMAND ----------

# MAGIC %run ./func_validation

# COMMAND ----------

# MAGIC %run ./func_hive

# COMMAND ----------

# MAGIC %run ./func_incremental

# COMMAND ----------

# MAGIC %run ./func_mail

# COMMAND ----------

# MAGIC %run ./func_devops

# COMMAND ----------

# MAGIC %run ./func_keys

# COMMAND ----------

# MAGIC %run ./func_synapse

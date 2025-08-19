# Databricks notebook source
# LOAD LIBRARIES
import os
import pyspark.sql.functions as f
import datetime
import re
import numpy as np
import datetime as dt
import databricks.koalas as ks
import pandas as pd

from pyspark.sql.dataframe import DataFrame
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

# COMMAND ----------

# SETUP COMMON CONFIG
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)

# COMMAND ----------

# MAGIC %run ./global_variables

# COMMAND ----------

# MAGIC %run ./func_azure_table

# COMMAND ----------

# MAGIC %run ./func_data_transformation

# COMMAND ----------

# MAGIC %run ./func_data_validation

# COMMAND ----------

# MAGIC %run ./func_field_mappings

# COMMAND ----------

# MAGIC %run ./func_hive_operations

# COMMAND ----------

# MAGIC %run ./func_incremental

# COMMAND ----------

# MAGIC %run ./func_input_parameters

# COMMAND ----------

# MAGIC %run ./func_schema

# COMMAND ----------

# MAGIC %run ./func_helpers

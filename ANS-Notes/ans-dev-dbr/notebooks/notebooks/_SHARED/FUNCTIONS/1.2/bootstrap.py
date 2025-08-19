# Databricks notebook source
# LOAD LIBRARIES

import datetime
import datetime as dt
import databricks.koalas as ks
import json
import numpy as np
import os
import pandas as pd
import pyspark.sql.functions as f
import re
import requests
import time


from datetime import date, datetime, timedelta, timezone
from delta.tables import *

from pyspark import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import round

# COMMAND ----------

# GLOBAL VARIABLES
ENV_NAME = os.getenv('ENV_NAME')
STORAGE_NAME = "edmans{0}data001".format(ENV_NAME)
SCOPE_NAME = "edm-ans-{0}-dbr-scope".format(ENV_NAME)
DATALAKE_ENDPOINT = 'abfss://datalake@edmans{0}data001.dfs.core.windows.net'.format(ENV_NAME)

TENANT_ID = 'e49ea3fe-87f8-44df-a7ba-2131f4fb91f1'
SVC_EDM_DBR_ID = dbutils.secrets.get(scope = SCOPE_NAME, key = "svc-edm-dbr-id")
SVC_EDM_DBR_SECRET = dbutils.secrets.get(scope = SCOPE_NAME, key = "svc-edm-dbr-secret")

SUBCRIPTION_ID = 'a64abb5d-8b1a-45bf-9eca-7096de5d4fac'
RESOURCE_GROUP = 'edm-ans-{}-rg'.format(ENV_NAME)
DATA_FACTORY = 'edm-ans-{}-df'.format(ENV_NAME)

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
NOTEBOOK_NAME = os.path.basename(NOTEBOOK_PATH)

# COMMAND ----------

# SETUP COMMON CONFIG
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)

# COMMAND ----------

# MAGIC %run ./func_general

# COMMAND ----------

# MAGIC %run ./func_mails

# COMMAND ----------

# MAGIC %run ./func_azure_table

# COMMAND ----------

# MAGIC %run ./func_data_transformation

# COMMAND ----------

# MAGIC %run ./func_transformation_groups

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

# MAGIC %run ./func_data_transformation_ebs

# COMMAND ----------

# MAGIC %run ./func_schema

# COMMAND ----------

# MAGIC %run ./func_helpers

# COMMAND ----------

# MAGIC %run ./func_keys

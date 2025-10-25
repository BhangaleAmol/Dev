# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.catalog import alter_table_partition
from databricks.functions.shared.logger import set_logger

# COMMAND ----------

set_logger('edm', 'debug')
alter_table_partition('s_core.account_agg', ['_DELETED', '_SOURCE', '_DATE'])

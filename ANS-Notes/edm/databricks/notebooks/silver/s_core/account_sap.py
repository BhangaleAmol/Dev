# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.tests import *
read_test_data('/s_core/account_sap/sapp01.kna1.csv').display()

# COMMAND ----------

# libraries
from databricks.functions.shared.input import print_config
import databricks.functions.s_core as core
from databricks.functions.s_core.account_sap import *

# COMMAND ----------

# bootstrap
import databricks.functions.shared.bootstrap as boot
boot.bootstrap()

# COMMAND ----------

# logging
from databricks.functions.shared.logger import set_logger
set_logger('edm', 'debug')

# COMMAND ----------

# debug config
config = '''
{
    "table_name": "account_sap",
    "incremental": "true"
}
'''

# COMMAND ----------

params = core.header({'config': config})

# COMMAND ----------

print_config(params)

# COMMAND ----------

datasets = read_data(params)

# COMMAND ----------

df = transform_data(datasets)
df.display()

# COMMAND ----------

save_data(df, params)

# COMMAND ----------

soft_delete(params)

# COMMAND ----------

update_keys(params)

# COMMAND ----------

update_timestamps(datasets, params)

# COMMAND ----------

core.footer(params)

# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

# libraries
from databricks.functions.shared.input import print_config
import databricks.functions.s_core as core
from databricks.functions.s_core.account_sf import *

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
    "table_name": "account_sf"
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

core.footer(params)

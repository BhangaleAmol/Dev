# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.input import print_config
import databricks.functions.s_core as core
from databricks.functions.s_core.account_col import *

# COMMAND ----------

import databricks.functions.shared.bootstrap as boot
boot.bootstrap()

# COMMAND ----------

from databricks.functions.shared.logger import set_logger
set_logger('edm', 'debug')

# COMMAND ----------

config = '''
{
    "table_name": "account_col"
}
'''

# COMMAND ----------

params = core.header({'config': config})

# COMMAND ----------

print_config(params)

# COMMAND ----------

datasets = read_data()

# COMMAND ----------

df = transform_data(datasets)

# COMMAND ----------

save_data(df, params)

# COMMAND ----------

core.footer(params)

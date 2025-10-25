# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.input import print_config
import databricks.functions.b_col as b_col
from databricks.functions.b_col.merge_to_delta import *

# COMMAND ----------

import databricks.functions.shared.bootstrap as boot
boot.bootstrap()

# COMMAND ----------

from databricks.functions.shared.logger import set_logger
set_logger('edm', 'debug')

# COMMAND ----------

config = '''
{
    "database_name":"col",
    "overwrite":"false",
    "source_folder":"/datalake/COL/raw_data/delta_data",
    "target_folder":"/datalake/COL/raw_data/full_data",
    "active":"true",
    "handle_delete":"false",
    "incremental":"false",
    "source_name":"VW_QV_Invoices",
    "table_name":"VW_QV_INVOICES"
}
'''

# COMMAND ----------

params = b_col.header({'config': config})

# COMMAND ----------

print_config(params)

# COMMAND ----------

datasets = read_data(params.get('source_file'))

# COMMAND ----------

df = transform_data(datasets)

# COMMAND ----------

save_data(df, params)

# COMMAND ----------

b_col.footer(params)

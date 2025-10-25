# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.catalog import add_column, column_exists, alter_column_type, alter_table_partition
from databricks.functions.shared.logger import set_logger

# COMMAND ----------

set_logger('edm', 'debug')

table_names = [
    's_core.account_col', 's_core.account_ebs', 's_core.account_kgd', 's_core.account_prms',
    's_core.account_sap', 's_core.account_sf', 's_core.account_tot', 's_core.account_agg']

# add new column
for table_name in table_names:

    if not column_exists(table_name, '_DATE'):
        add_column(table_name, '_DATE', 'date', after_column='territory_ID')
        spark.sql(
            f'UPDATE {table_name} SET _DATE = TO_DATE(_MODIFIED)')

# add missing column
if not column_exists('s_core.account_prms', 'owner_ID'):
    add_column('s_core.account_prms', 'owner_ID',
               'string', after_column='modifiedBy_ID')

alter_column_type('s_core.account_prms', '_PART', 'string')

# change partitions
for table_name in table_names:
    alter_table_partition(table_name, ['_DELETED', '_DATE'])

# COMMAND ----------

# MAGIC %pip install azure-identity==1.12.0
# MAGIC %pip install azure-mgmt-datafactory==3.1.0

# COMMAND ----------

from databricks.functions.shared.azure_data_factory import run_pipeline

import os
env_name = os.environ.get('ENV_NAME')
if env_name == 'test':

    parameters = {"table_name": ['vw_company']}
    run_pipeline('col_master_pipeline', parameters, wait=True)

    parameters = {"table_name": ['account_col', 's_core.account_agg']}
    run_pipeline('cor_master_pipeline', parameters, wait=True)

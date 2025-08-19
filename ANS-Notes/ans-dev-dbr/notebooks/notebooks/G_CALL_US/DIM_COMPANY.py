# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_call_us')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['COMPANY_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_company')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/g_call_us/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 's_callsystems.dim_company'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  dim_company_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  dim_company_df = load_full_dataset(source_table)
  
dim_company_df.display()

# COMMAND ----------

# SAMPLING
if sampling:
  dim_company_df = dim_company_df.limit(10)

# COMMAND ----------

# TRANSFORM
dim_company_f = (
  dim_company_df
  .select('COMPANY_ID', 'COMPANY_ID_NK', 'COMPANY_NAME', 'COUNTRY')
  .filter("_SOURCE = 'DEFAULT' OR _SOURCE = 'SF'")
  .filter("_DELETED = false")
)

dim_company_f.display()

# COMMAND ----------

# LOAD
register_hive_table(dim_company_f, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(dim_company_f, target_table, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(dim_company_df, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_callsystems.dim_company')
  update_run_datetime(run_datetime, target_table, 's_callsystems.dim_company')

# COMMAND ----------



# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.tm_customer_location_v

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'tm_customer_location_v')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

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
source_table = 's_core.customer_location_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  customer_location_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  customer_location_agg = load_full_dataset(source_table)
  
customer_location_agg.createOrReplaceTempView('customer_location_agg')
customer_location_agg.display()

# COMMAND ----------

# SAMPLING
if sampling:
  customer_location_agg = customer_location_agg.limit(10)
  customer_location_agg.createOrReplaceTempView('customer_location_agg')

# COMMAND ----------

main_df = spark.sql("""
SELECT
_ID,
accountID,
accountNumber,
addressId,
addressLine1,
addressLine2,
addressLine3,
addressLine4,
createdOn,
city,
country,
emailAddress,
mdmId,
name,
partySiteNumber,
phoneNumber,
postalCode,
siteCategory,
siteUseCode,
state
from customer_location_agg
WHERE _SOURCE = 'EBS'
and not _DELETED
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("createdOn"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# VALIDATE DATA
check_distinct_count(main_f, '_ID')

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
   spark.table('s_core.customer_location_agg')
  .select('_ID')
  .filter("_DELETED IS FALSE")
  .filter("_SOURCE = 'EBS'")
  )

apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_core.customer_location_agg')
  update_run_datetime(run_datetime, target_table, 's_core.customer_location_agg')

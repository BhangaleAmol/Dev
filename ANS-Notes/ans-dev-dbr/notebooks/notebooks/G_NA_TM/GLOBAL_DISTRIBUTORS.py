# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.global_distributors

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['account_id'])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'global_distributors')
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
source_table = 's_core.account_agg'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  account_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  account_agg = load_full_dataset(source_table)
  
account_agg.createOrReplaceTempView('account_agg')
account_agg.display()

# COMMAND ----------

# SAMPLING
if sampling:
  account_agg = account_agg.limit(10)
  account_agg.createOrReplaceTempView('account_agg')

# COMMAND ----------

main_df = spark.sql("""
 select
  account_agg.createdOn,
  account_agg._id account_id,
  account_agg.accountid,
  account_agg.accountNumber,
  account_agg.name,
  account_agg.registrationId,
  nvl(
    global_customer_master.GlobalCustomer,
    account_agg.name
  ) GlobalCustomer,
  account_agg.customerSegmentation,
  account_agg.customerTier,
  case
    when itd_flag.AccountNumber = account_agg.accountNumber then itd_flag.ITDOTD
    when otd_flag.RegistrationID = account_agg.RegistrationID then otd_flag.ITDOTD
  end itd_otd_flag
from
  s_core.account_agg
  left join qv.global_customer_master on global_customer_master.customernumbers = account_agg.accountNumber
  left join (
    select
      distinct AccountNumber,
      ITDOTD
    from
      amazusftp1.exclusion_list
    WHERE
      ITDOTD LIKE 'ITD%'
  ) itd_flag on itd_flag.AccountNumber = account_agg.accountNumber
  left join (
    select
      distinct RegistrationID,
      ITDOTD
    from
      amazusftp1.exclusion_list
    WHERE
      ITDOTD LIKE 'OTD%'
  ) otd_flag on otd_flag.RegistrationID = account_agg.RegistrationID
where
  account_agg._deleted = 'false'
  and account_agg._source = 'EBS'
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

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

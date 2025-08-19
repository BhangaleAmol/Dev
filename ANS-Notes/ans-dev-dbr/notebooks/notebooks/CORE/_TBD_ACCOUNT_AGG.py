# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.account_col', prune_days) 
  account_col = (spark.table('s_core.account_col')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
  
  cutoff_value = get_cutoff_value(table_name, 's_core.account_ebs', prune_days) 
  account_ebs = (spark.table('s_core.account_ebs')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
  
  cutoff_value = get_cutoff_value(table_name, 's_core.account_kgd', prune_days) 
  account_kgd = (spark.table('s_core.account_kgd')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
   
  cutoff_value = get_cutoff_value(table_name, 's_core.account_sap', prune_days) 
  account_sap = (spark.table('s_core.account_sap')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
  
  cutoff_value = get_cutoff_value(table_name, 's_core.account_sf', prune_days) 
  account_sf = (spark.table('s_core.account_sf')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
  
  cutoff_value = get_cutoff_value(table_name, 's_core.account_tot', prune_days) 
  account_tot = (spark.table('s_core.account_tot')
                .filter(f"_MODIFIED > '{cutoff_value}'"))
  
else:
  account_col = spark.table('s_core.account_col')
  account_ebs = spark.table('s_core.account_ebs')
  account_kgd = spark.table('s_core.account_kgd')
  account_sap = spark.table('s_core.account_sap')
  account_sf = spark.table('s_core.account_sf')
  account_tot = spark.table('s_core.account_tot')

# COMMAND ----------

account_col = account_col.filter("_ID != '0'")
account_ebs = account_ebs.filter("_ID != '0'")
account_kgd = account_kgd.filter("_ID != '0'")
account_sap = account_sap.filter("_ID != '0'")
account_sf = account_sf.filter("_ID != '0'")
account_tot = account_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  account_col = account_col.limit(10)
  account_ebs = account_ebs.limit(10)
  account_kgd = account_kgd.limit(10) 
  account_sap = account_sap.limit(10)
  account_sf = account_sf.limit(10)
  account_tot = account_tot.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

account_col_f = (
  account_col
  .select(columns)
  .transform(apply_schema(schema))
)

account_ebs_f = (
  account_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

account_kgd_f = (
  account_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

account_sap_f = (
  account_sap
  .select(columns)
  .transform(apply_schema(schema))
)

account_sf_f = (
  account_sf
  .select(columns)
  .transform(apply_schema(schema))
)

account_tot_f = (
  account_tot
  .select(columns)
  .transform(apply_schema(schema))
) 

# COMMAND ----------

main_f = (
  account_col_f
  .union(account_ebs_f)
  .union(account_kgd_f)
  .union(account_sap_f)
  .union(account_sf_f)
  .union(account_tot_f)
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(account_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_col')
  update_run_datetime(run_datetime, table_name, 's_core.account_col')
  
  cutoff_value = get_incr_col_max_value(account_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.account_ebs')
  
  cutoff_value = get_incr_col_max_value(account_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.account_kgd')
  
  cutoff_value = get_incr_col_max_value(account_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_sap')
  update_run_datetime(run_datetime, table_name, 's_core.account_sap')
  
  cutoff_value = get_incr_col_max_value(account_sf, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_sf')
  update_run_datetime(run_datetime, table_name, 's_core.account_sf')
  
  cutoff_value = get_incr_col_max_value(account_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.account_tot')
  update_run_datetime(run_datetime, table_name, 's_core.account_tot')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

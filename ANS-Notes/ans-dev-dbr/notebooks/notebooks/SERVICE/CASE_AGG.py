# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.case

# COMMAND ----------

if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_service.case_sf', prune_days) 
  case_sf = load_agg_incr_dataset('s_service.case_sf','_MODIFIED',cutoff_value)
  
else:
  case_sf = spark.table('s_service.case_sf').filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  case_sf = case_sf.limit(10)

# COMMAND ----------

main_f = (
  case_sf
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)
add_unknown_record(main_f, table_name)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:

  cutoff_value = get_incr_col_max_value(case_sf, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_service.case_sf')
  update_run_datetime(run_datetime, table_name, 's_service.case_sf')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service

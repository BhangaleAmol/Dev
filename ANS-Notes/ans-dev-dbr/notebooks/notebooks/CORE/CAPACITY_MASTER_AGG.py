# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.capacity_master

# COMMAND ----------

overwrite = True

# COMMAND ----------

if incremental:  
  cutoff_value = get_cutoff_value(table_name, 's_core.capacity_master_tmb2', prune_days) 
  capacity_master_tmb2 = load_agg_incr_dataset('s_core.capacity_master_tmb2','_MODIFIED', cutoff_value)  
else:
  capacity_master_tmb2 = spark.table('s_core.capacity_master_tmb2') 

# COMMAND ----------

capacity_master_tmb2 = capacity_master_tmb2.filter("_ID != '0'")

# COMMAND ----------

if sampling:
  capacity_master_tmb2 = capacity_master_tmb2.limit(10)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

capacity_master_tmb2_f = (
  capacity_master_tmb2
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  capacity_master_tmb2_f  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

if not (test_run or sampling):  
  cutoff_value = get_incr_col_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.capacity_master_tmb2')
  update_run_datetime(run_datetime, table_name, 's_core.capacity_master_tmb2') 

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

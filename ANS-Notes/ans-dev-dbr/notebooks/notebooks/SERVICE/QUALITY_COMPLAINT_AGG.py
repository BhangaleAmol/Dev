# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.quality_complaint

# COMMAND ----------

if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_service.quality_complaint_twd', prune_days) 
  quality_complaint_twd = ( 
                            spark.table('s_service.quality_complaint_twd')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
  
else:
  quality_complaint_twd = spark.table('s_service.quality_complaint_twd').filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  quality_complaint_twd = quality_complaint_twd.limit(10)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

quality_complaint_twd_f = (
  quality_complaint_twd
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  quality_complaint_twd_f
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
if not test_run:

  cutoff_value = get_incr_col_max_value(quality_complaint_twd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_service.quality_complaint_twd')
  update_run_datetime(run_datetime, table_name, 's_service.quality_complaint_twd')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service

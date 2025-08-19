# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.quality_creditnote_approval

# COMMAND ----------

if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_service.quality_creditnote_approval_com', prune_days) 
  quality_creditnote_approval_com = ( 
                                      spark.table('s_service.quality_creditnote_approval_com')
                                      .filter(f"_MODIFIED > '{cutoff_value}'")
                                    )
  
else:
  quality_creditnote_approval_com = spark.table('s_service.quality_creditnote_approval_com').filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  quality_creditnote_approval_com = quality_creditnote_approval_com.limit(10)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

quality_creditnote_approval_com_f = (
  quality_creditnote_approval_com
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  quality_creditnote_approval_com
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

  cutoff_value = get_incr_col_max_value(quality_creditnote_approval_com, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_service.quality_creditnote_approval_com')
  update_run_datetime(run_datetime, table_name, 's_service.quality_creditnote_approval_com')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service

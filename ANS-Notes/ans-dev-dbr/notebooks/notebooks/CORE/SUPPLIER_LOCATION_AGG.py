# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.supplier_location_ebs', prune_days) 
  supplier_location_ebs = load_agg_incr_dataset('s_core.supplier_location_ebs','_MODIFIED',cutoff_value)
  
  
else:
  supplier_location_ebs = spark.table('s_core.supplier_location_ebs')
  

# COMMAND ----------

supplier_location_ebs = supplier_location_ebs.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  supplier_location_ebs = supplier_location_ebs.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.supplier_location

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

supplier_location_ebs_f = (
  supplier_location_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
        supplier_location_ebs_f
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
  cutoff_value = get_incr_col_max_value(supplier_location_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.supplier_location_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.supplier_location_ebs')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

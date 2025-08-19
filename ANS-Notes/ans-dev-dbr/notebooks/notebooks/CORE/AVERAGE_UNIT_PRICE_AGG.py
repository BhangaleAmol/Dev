# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.average_unit_price_sapqv', prune_days) 
  average_unit_price_sapqv = ( 
                                spark.table('s_core.average_unit_price_sapqv')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  
else:
  average_unit_price_sapqv = spark.table('s_core.average_unit_price_sapqv')
  

# COMMAND ----------

average_unit_price_sapqv = average_unit_price_sapqv.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  average_unit_price_sapqv = average_unit_price_sapqv.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.average_unit_price

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

average_unit_price_sapqv_f = (
  average_unit_price_sapqv
  .select(columns)
  .transform(apply_schema(schema))
)


# COMMAND ----------

main_f = (
  average_unit_price_sapqv_f
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
  cutoff_value = get_incr_col_max_value(average_unit_price_sapqv, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.average_unit_price_sapqv')
  update_run_datetime(run_datetime, table_name, 's_core.average_unit_price_sapqv')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

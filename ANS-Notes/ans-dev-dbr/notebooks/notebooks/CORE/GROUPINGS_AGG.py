# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.groupings_tot', prune_days) 
  groupings_tot = load_agg_incr_dataset('s_core.groupings_tot','_MODIFIED',cutoff_value)
    
else:
  groupings_tot = spark.table('s_core.groupings_tot')
  

# COMMAND ----------

groupings_tot = groupings_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  groupings_tot = groupings_tot.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.groupings

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

groupings_tot_f = (
  groupings_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
          groupings_tot_f
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
  cutoff_value = get_incr_col_max_value(groupings_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.groupings_tot')
  update_run_datetime(run_datetime, table_name, 's_core.groupings_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

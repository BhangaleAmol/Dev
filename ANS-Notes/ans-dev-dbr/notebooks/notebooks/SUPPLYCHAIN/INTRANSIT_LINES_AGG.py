# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.intransit_lines_col', prune_days)
  intransit_lines_col = ( 
                          spark.table('s_supplychain.intransit_lines_col')
                          .filter(f"_MODIFIED > '{cutoff_value}'")
                        )
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.intransit_lines_tot', prune_days)
  intransit_lines_tot = ( 
                          spark.table('s_supplychain.intransit_lines_tot')
                          .filter(f"_MODIFIED > '{cutoff_value}'")
                        )
  
else:
  intransit_lines_col = spark.table('s_supplychain.intransit_lines_col')
  intransit_lines_tot = spark.table('s_supplychain.intransit_lines_tot')

# COMMAND ----------

# LOAD DATASETS
intransit_lines_col = spark.table('s_supplychain.intransit_lines_col').filter("_ID != '0'")
intransit_lines_tot = spark.table('s_supplychain.intransit_lines_tot').filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  intransit_lines_col = intransit_lines_col.limit(10)
  intransit_lines_tot = intransit_lines_tot.limit(10)  

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.intransit_lines

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

intransit_lines_col_f = (
  intransit_lines_col
  .select(columns)
  .transform(apply_schema(schema))
)

intransit_lines_tot_f = (
  intransit_lines_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
         intransit_lines_col_f
  .union(intransit_lines_tot_f)
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
  cutoff_value = get_incr_col_max_value(intransit_lines_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.intransit_lines_col')
  update_run_datetime(run_datetime, table_name, 's_supplychain.intransit_lines_col')
  
  cutoff_value = get_incr_col_max_value(intransit_lines_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.intransit_lines_tot')
  update_run_datetime(run_datetime, table_name, 's_supplychain.intransit_lines_tot')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain

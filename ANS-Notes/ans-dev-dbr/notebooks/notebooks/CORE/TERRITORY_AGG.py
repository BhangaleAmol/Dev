# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_col', prune_days) 
  territory_col = (
                        spark.table('s_core.territory_col')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_ebs', prune_days) 
  territory_ebs = (
                        spark.table('s_core.territory_ebs')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_kgd', prune_days) 
  territory_kgd = (
                        spark.table('s_core.territory_kgd')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_sap', prune_days) 
  territory_sap = (
                        spark.table('s_core.territory_sap')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_tot', prune_days) 
  territory_tot = (
                        spark.table('s_core.territory_tot')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
else:
  territory_col = spark.table('s_core.territory_col')
  territory_ebs = spark.table('s_core.territory_ebs')
  territory_kgd = spark.table('s_core.territory_kgd')
  territory_sap = spark.table('s_core.territory_sap')
  territory_tot = spark.table('s_core.territory_tot')
  

# COMMAND ----------

territory_col = territory_col.filter("_ID != '0'")
territory_ebs = territory_ebs.filter("_ID != '0'")
territory_kgd = territory_kgd.filter("_ID != '0'")
territory_sap = territory_sap.filter("_ID != '0'")
territory_tot = territory_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  territory_col = territory_col.limit(10)
  territory_ebs = territory_ebs.limit(10)
  territory_kgd = territory_kgd.limit(10)
  territory_sap = territory_sap.limit(10)
  territory_tot = territory_tot.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.territory

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

territory_col_f = (
  territory_col
  .select(columns)
  .transform(apply_schema(schema))
)

territory_ebs_f = (
  territory_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

territory_kgd_f = (
  territory_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

territory_sap_f = (
  territory_sap
  .select(columns)
  .transform(apply_schema(schema))
)

territory_tot_f = (
  territory_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  territory_col_f
  .union(territory_ebs_f)
  .union(territory_kgd_f)
  .union(territory_sap_f)
  .union(territory_tot_f)
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
  cutoff_value = get_incr_col_max_value(territory_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_col')
  update_run_datetime(run_datetime, table_name, 's_core.territory_col')
  
  cutoff_value = get_incr_col_max_value(territory_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.territory_ebs')
  
  cutoff_value = get_incr_col_max_value(territory_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.territory_kgd')
  
  cutoff_value = get_incr_col_max_value(territory_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_sap')
  update_run_datetime(run_datetime, table_name, 's_core.territory_sap')
  
  cutoff_value = get_incr_col_max_value(territory_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_tot')
  update_run_datetime(run_datetime, table_name, 's_core.territory_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

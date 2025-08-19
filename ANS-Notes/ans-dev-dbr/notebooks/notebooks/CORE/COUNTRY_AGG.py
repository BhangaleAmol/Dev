# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.country_col', prune_days) 
  country_col = (
                        spark.table('s_core.country_col')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.country_ebs', prune_days) 
  country_ebs = (
                        spark.table('s_core.country_ebs')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.country_kgd', prune_days) 
  country_kgd = (
                        spark.table('s_core.country_kgd')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.country_sap', prune_days) 
  country_sap = (
                        spark.table('s_core.country_sap')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.country_tot', prune_days) 
  country_tot = (
                        spark.table('s_core.country_tot')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
else:
  country_col = spark.table('s_core.country_col')
  country_ebs = spark.table('s_core.country_ebs')
  country_kgd = spark.table('s_core.country_kgd')
  country_sap = spark.table('s_core.country_sap')
  country_tot = spark.table('s_core.country_tot')
  

# COMMAND ----------

country_col = country_col.filter("_ID != '0'")
country_ebs = country_ebs.filter("_ID != '0'")
country_kgd = country_kgd.filter("_ID != '0'")
country_sap = country_sap.filter("_ID != '0'")
country_tot = country_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  country_col = country_col.limit(10)
  country_ebs = country_ebs.limit(10)
  country_kgd = country_kgd.limit(10)
  country_sap = country_sap.limit(10)
  country_tot = country_tot.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.country

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

country_col_f = (
  country_col
  .select(columns)
  .transform(apply_schema(schema))
)

country_ebs_f = (
  country_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

country_kgd_f = (
  country_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

country_sap_f = (
  country_sap
  .select(columns)
  .transform(apply_schema(schema))
)

country_tot_f = (
  country_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  country_col_f
  .union(country_ebs_f)
  .union(country_kgd_f)
  .union(country_sap_f)
  .union(country_tot_f)
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
  cutoff_value = get_incr_col_max_value(country_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.country_col')
  update_run_datetime(run_datetime, table_name, 's_core.country_col')
  
  cutoff_value = get_incr_col_max_value(country_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.country_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.country_ebs')
  
  cutoff_value = get_incr_col_max_value(country_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.country_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.country_kgd')
  
  cutoff_value = get_incr_col_max_value(country_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.country_sap')
  update_run_datetime(run_datetime, table_name, 's_core.country_sap')
  
  cutoff_value = get_incr_col_max_value(country_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.country_tot')
  update_run_datetime(run_datetime, table_name, 's_core.country_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

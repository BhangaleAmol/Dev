# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.territory_assignments_ss', prune_days) 
  territory_assignments_ss = (
                        spark.table('s_core.territory_assignments_ss')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
    
else:
  territory_assignments_ss = spark.table('s_core.territory_assignments_ss')
  

# COMMAND ----------

territory_assignments_ss = territory_assignments_ss.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  territory_assignments_ss = territory_assignments_ss.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.territory_assignments

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

territory_assignments_ss_f = (
  territory_assignments_ss
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (territory_assignments_ss_f
         .transform(attach_unknown_record)
          .select(columns)
          .transform(sort_columns)
         )

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(territory_assignments_ss, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.territory_assignments_ss')
  update_run_datetime(run_datetime, table_name, 's_core.territory_assignments_ss')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

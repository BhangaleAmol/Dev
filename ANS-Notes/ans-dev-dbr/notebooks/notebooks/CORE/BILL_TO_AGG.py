# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.bill_to_sap', prune_days) 
  bill_to_sap = ( 
                 spark.table('s_core.bill_to_sap')
                 .filter(f"_MODIFIED > '{cutoff_value}'")
                )
else:
  bill_to_sap = spark.table('s_core.bill_to_sap')
  

# COMMAND ----------

bill_to_sap = bill_to_sap.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  bill_to_sap = bill_to_sap.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.bill_to

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

bill_to_sap_f = (
  bill_to_sap
  .select(columns)
  .transform(apply_schema(schema))
)


# COMMAND ----------

main_f = (
            bill_to_sap_f
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
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):  
  cutoff_value = get_incr_col_max_value(bill_to_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.bill_to_sap')
  update_run_datetime(run_datetime, table_name, 's_core.bill_to_sap')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

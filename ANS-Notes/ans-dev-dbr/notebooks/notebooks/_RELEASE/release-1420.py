# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'cor_master_pipeline'
parameters = {
  'table_name': ["USER_TWD","USER_AGG"],
  'overwrite': False,
  'incremental': 'false'
}

# COMMAND ----------

pipeline_name = 'svc_master_pipeline'
parameters = {
  'table_name': ["QUALITY_CREDITNOTE_APPROVAL_COM","QUALITY_CREDITNOTE_APPROVAL_AGG"]
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)


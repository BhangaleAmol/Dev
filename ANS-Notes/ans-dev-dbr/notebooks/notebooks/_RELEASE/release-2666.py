# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'ebsv2_master_pipeline'
parameters = {
  'table_name': ["GME_BATCH_HEADER", "GME_MATERIAL_DETAILS"],
  'overwrite': False,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'sup_master_pipeline'
parameters = {
  'table_name': ["SALES_SCHEDULE_LINES_SAP","SALES_SCHEDULE_LINES_AGG"],
  'overwrite': False,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

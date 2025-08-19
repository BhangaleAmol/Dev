# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'svc_master_pipeline'
parameters = {
  'table_name': ["OPPORTUNITY_SF","OPPORTUNITY_AGG","OPPORTUNITY_LINES_SF","OPPORTUNITY_LINES_AGG","CASE_SF","CASE_AGG"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'ss_master_pipeline'
parameters = {
  'table_name': ["EDM_CONTROL_TABLE"],
  'send_event': False  
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

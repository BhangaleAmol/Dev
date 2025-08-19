# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'sup_master_pipeline'
parameters = {
  'table_name': ["INTRANSIT_HEADERS_COL","INTRANSIT_HEADERS_TOT","INTRANSIT_HEADERS_AGG","INTRANSIT_LINES_COL","INTRANSIT_LINES_TOT","INTRANSIT_LINES_AGG"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

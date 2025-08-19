# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'cor_master_pipeline'
parameters = {
  'table_name': ["AVERAGE_UNIT_PRICE_FC_SAPQV","AVERAGE_UNIT_PRICE_FC_AGG"],
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

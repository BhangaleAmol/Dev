# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_silver_notebook

# COMMAND ----------

pipeline_name = 'ma_master_pipeline'
parameters = {
  'table_name': ["SHIPMENT"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

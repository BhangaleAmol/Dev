# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'cor_master_pipeline'
parameters = {
  'table_name': ["PRODUCT_ORG_COL","PRODUCT_ORG_AGG", "TRANSACTION_TYPE_COL", "TRANSACTION_TYPE_AGG"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# COMMAND ----------

pipeline_name = 'sup_master_pipeline'
parameters = {
  'table_name': ["SALES_ORDER_HEADERS_COL"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

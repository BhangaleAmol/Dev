# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'ebsv2_master_pipeline'
parameters = {
  'table_name': ["RA_CUSTOMER_TRX_LINES_ALL"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# COMMAND ----------

pipeline_name = 'ebs_master_pipeline'
parameters = {
  'table_name': ["W_SALES_INVOICE_LINE_F"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

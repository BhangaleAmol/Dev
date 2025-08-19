# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'cor_master_pipeline'
parameters = {
  'table_name':  ["PRODUCT_COL","PRODUCT_EBS","PRODUCT_KGD","PRODUCT_MDM","PRODUCT_PDH","PRODUCT_QV",
                 "PRODUCT_SAP","PRODUCT_SF","PRODUCT_TOT","PRODUCT_TWD","PRODUCT_AGG"],
  'overwrite': False,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

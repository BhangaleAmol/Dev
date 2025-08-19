# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap_release

# COMMAND ----------

pipeline_name = 'tdm_master_pipeline'
parameters = {
  'table_name': ["TRADE_PROMOTION_ACCRUALS_EBS","TRADE_PROMOTION_ACCRUALS_AGG"],
  'overwrite': False,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

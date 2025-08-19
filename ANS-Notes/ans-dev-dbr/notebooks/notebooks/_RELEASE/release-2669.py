# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

pipeline_name = 'cor_master_pipeline'
parameters = {
  'table_name': ["ACCOUNT_KGD","ACCOUNT_AGG","CUSTOMER_LOCATION_KGD","CUSTOMER_LOCATION_AGG","ORGANIZATION_KGD","ORGANIZATION_AGG","ORIGIN_KGD","ORIGIN_AGG","PRODUCT_KGD","PRODUCT_AGG","PRODUCT_ORG_KGD","PRODUCT_ORG_AGG","TERRITORY_KGD","TERRITORY_AGG","TRANSACTION_TYPE_KGD","TRANSACTION_TYPE_AGG"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# COMMAND ----------

pipeline_name = 'sup_master_pipeline'
parameters = {
  'table_name': ["INVENTORY_KGD","SALES_INVOICE_HEADERS_KGD","SALES_INVOICE_LINES_KGD","SALES_ORDER_HEADERS_KGD","SALES_ORDER_LINES_KGD","SALES_SHIPPING_LINES_KGD"],
  'overwrite': True,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

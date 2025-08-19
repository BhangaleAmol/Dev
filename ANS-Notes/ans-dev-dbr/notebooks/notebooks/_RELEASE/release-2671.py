# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

add_hive_table_column('s_trademanagement.point_of_sales_adj','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_ebs','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_grainger','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_hs','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_hscanada','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_ndc','rejectReason', 'string', default_value = None , after_col = 'processingType');
add_hive_table_column('s_trademanagement.point_of_sales_agg','rejectReason', 'string', default_value = None , after_col = 'processingType');

# COMMAND ----------

pipeline_name = 'tdm_master_pipeline'
parameters = {
  'table_name': ["POINT_OF_SALES_EBS","POINT_OF_SALES_AGG"],
  'overwrite': False,
  'incremental': 'false'
}
adf_controller.start_pipeline(pipeline_name, parameters=parameters)

# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

ENV_NAME = os.getenv('ENV_NAME')
ENV_NAME = ENV_NAME.upper()
ENV_NAME

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['EBS_Sales_Order','Line_Id','Batch_Id'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
source_table = get_input_param('source_table', default_value = 'g_logility.logility_drop_ship_ebs')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)
batch_id = get_input_param('batch_id', 'string', default_value = 0)

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name,source_table,None)
currentuser = str(spark.sql("select current_user()").collect()[0][0])
currenttime = datetime.now()

# COMMAND ----------

sql = f"Update {source_table} set processed = 'Y' where batch_id = '{batch_id}';"
print(sql)
spark.sql(sql)

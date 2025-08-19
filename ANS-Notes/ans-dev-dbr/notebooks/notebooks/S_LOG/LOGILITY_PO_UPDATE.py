# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

ENV_NAME = os.getenv('ENV_NAME')
ENV_NAME = ENV_NAME.upper()
ENV_NAME

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['groupingid'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
table_name = get_input_param('table_name', default_value = 's_logility.po_logility_ebs')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)
groupingid = get_input_param('groupingid', 'string', default_value = '0')

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name,table_name,None)
currentuser = str(spark.sql("select current_user()").collect()[0][0])
currenttime = datetime.now()

# COMMAND ----------

sql = f"Update {table_name} set processed = 'Y' where groupingid = '{groupingid}';"
print(sql)
spark.sql(sql)

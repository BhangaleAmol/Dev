# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
#key_columns = get_input_param('key_columns', 'string', default_value = 'item_ID,inventoryWarehouse_ID,owningBusinessUnit_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'TMP_LOM_AA_DO_L1')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# DBTITLE 1,Get current archive data set for LOM AA  DO L1
current_archive_AA = spark.sql("""
""")

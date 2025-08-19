# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'brid_ticket_item')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
main_f = spark.table('s_ticketsystems.brid_ticket_item_glpi')

# COMMAND ----------

 # LOAD
full_name = f'{database_name}.{table_name}'
register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, full_name, ['TicketID', 'ItemID'], options = {'auto_merge': True})  

# COMMAND ----------



# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
source_database = get_input_param('source_database', default_value = 'cti')
table_name = get_input_param('table_name', default_value = 'CSRS')
target_database = get_input_param('target_database', default_value = 'cti_hist')
target_folder = get_input_param('target_folder', default_value = '/datalake/CTI/raw_data/full_data_hist')

# COMMAND ----------

# SETUP VARIABLES
from datetime import date

last_day = get_last_day_of_month(date.today())
file_name = get_file_name_with_date(table_name, last_day, 'dlt')

source_table = get_table_name(source_database, table_name)
target_path = get_file_path(target_folder, file_name)
target_table = get_table_name_with_date(target_database, table_name, last_day)

print('source_table: ' + source_table)
print('target_path: ' + target_path)
print('target_table_name: ' + target_table)

# COMMAND ----------

# CLONE TABLE
clone_table(source_table, target_table, target_path)

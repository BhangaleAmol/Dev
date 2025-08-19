# Databricks notebook source
import os
import calendar
from datetime import datetime, date

# COMMAND ----------

def get_endpoint_name(container_name = 'datalake', storage_name = 'edmans{env}data001'):
  env_name = os.getenv('ENV_NAME')
  storage_name = storage_name.replace('{env}', env_name)  
  return f'abfss://{container_name}@{storage_name}.dfs.core.windows.net'

def get_file_name(file_name, file_extension = None, schema_name = None):
  if schema_name is not None:
    schema_name = f"{schema_name}."
  else:
    schema_name = ''
      
  if file_extension is not None:    
    file_extension = f".{file_extension}"
  else:
    file_extension = ''
  
  full_name = f"{schema_name}{file_name}{file_extension}"
  return full_name.lower()  
  
def get_file_name_with_date(file_name, date_value, file_extension = None):
  date_suffix = date_value.strftime("%Y_%m_%d")
  if file_extension is None:
    return "{0}_{1}".format(file_name, date_suffix).lower()
  else:
    return "{0}_{1}.{2}".format(file_name, date_suffix, file_extension).lower()  

def get_file_path(folder_name, file_name, container_name = 'datalake', storage_name = 'edmans{env}data001'):
  endpoint_name = get_endpoint_name(container_name, storage_name)
  if folder_name[0] == '/':
    folder_name = folder_name[1:]
  return f"{endpoint_name}/{folder_name}/{file_name}"

def get_folder_path(folder_name, container_name = 'datalake', storage_name = 'edmans{env}data001'):
  endpoint_name = get_endpoint_name(container_name, storage_name)
  if folder_name[0] == '/':
    folder_name = folder_name[1:]
  return f"{endpoint_name}/{folder_name}"

def get_last_day_of_month(date_value):
  rng = calendar.monthrange(date_value.year, date_value.month)
  return date(date_value.year, date_value.month, rng[1])

def get_map_table_name(table_name):
  database_name = table_name.split('.')[0]
  s_table_name = table_name.split('.')[1]  
  return f'{database_name}._map_{s_table_name}'

def last_day_of_month(value):
  rng = calendar.monthrange(value.year, value.month)
  last_day = date(value.year, value.month, rng[1])
  return (value == last_day)

def get_secret(secret_name):
  secretScope = 'edm-ans-{}-dbr-scope'.format(ENV_NAME)
  return dbutils.secrets.get(scope = secretScope, key = secret_name)

def get_table_name(database_name, table_name, schema_name = None):
  if (schema_name is not None):
    schema_name = schema_name.replace('.','_')
    table_name = "{0}.{1}_{2}".format(database_name, schema_name, table_name)
  else:
    table_name = "{0}.{1}".format(database_name, table_name)
  return table_name.lower()

def get_table_name_with_date(database_name, table_name, date_value, schema_name = None):
  date_suffix = date_value.strftime("%Y_%m_%d")  
  table_name = get_table_name(database_name, table_name, schema_name)
  table_name = "{0}_{1}".format(table_name, date_suffix)
  return table_name.lower()

def get_table_path(table_name, container_name = 'datalake', storage_name = 'edmans{env}data001'):  
  folder_name = 'datalake/' + table_name.split('.')[0]
  file_name = table_name.split('.')[1] + '.dlt'  
  return get_file_path(folder_name, file_name, container_name, storage_name)

def remove_folder(folder_path, options = {}):
  
  container_name = options.get('container_name', 'datalake')
  storage_name = options.get('storage_name', 'edmans{env}data001')
  endpoint_name = get_endpoint_name(container_name, storage_name)
  
  full_folder_path = f'{endpoint_name}{folder_path}'
  message = f'REMOVING FOLDER {full_folder_path}'
  print(message)
  dbutils.fs.rm(full_folder_path, True)

def _split_table_name(table_name):
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  return (database_name, table_name)

def create_tmp_table(sql, table_name):
  """
  Funtion saves a sql reult to a table with tmp prefix.
  """
  df = spark.sql(sql)  
  table_name = table_name.lower()
  df.createOrReplaceTempView(table_name)
  file_name = get_file_name(f'tmp_{table_name}', 'dlt') 
  target_file = get_file_path(temp_folder, file_name)
  print(file_name)
  print(target_file)
  df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

def get_run_period():
  ENV_NAME = os.getenv('ENV_NAME')
  ENV_NAME = ENV_NAME.upper()

  run_period= spark.sql("""
  select nvl(case when cvalue = '*CURRENT' and day(current_date) >= nvalue then year(current_date) * 100 + month(current_date) when cvalue = '*CURRENT' and day(current_date) < nvalue then year(add_months(current_date, -1)) * 100 + month(add_months(current_date, -1)) else cast(cvalue as integer) end, year(current_date) * 100 + month(current_date)) runPeriod from smartsheets.edm_control_table where table_id = 'TEMBO_CONTROLS' and key_value = '{0}_CURRENT_PERIOD'   and active_flg is true   and current_date between valid_from and valid_to
  """.format(ENV_NAME.upper()))

  runperiod=str(run_period.collect()[0][0])  
  return runperiod

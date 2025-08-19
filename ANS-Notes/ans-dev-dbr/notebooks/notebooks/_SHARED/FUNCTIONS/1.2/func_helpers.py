# Databricks notebook source
import os

# COMMAND ----------

def compare_schema(table_name, schema):
  table_schema = spark.table(table_name).schema
  
  columns_schema = list(schema.keys())
  columns_schema.sort()
  
  columns_table = spark.table(table_name).columns
  columns_table.sort()

  if columns_schema != columns_table:
    not_in_schema = list(set(columns_table) - set(columns_schema))
    not_in_table = list(set(columns_schema) - set(columns_table))
    
    missing_in_schema = [(c.name, c.dataType) for c in table_schema if c.name in not_in_schema]
    missing_in_table = [(key, value) for key, value in schema.items() if key in not_in_table]
    
    print(f'missing_in_schema: {missing_in_schema}')
    print(f'missing_in_table: {missing_in_table}')

def display_sorted(df):
  df.select(sorted(df.columns)).display()

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise  
  
def get_endpoint_name(container_name = 'datalake', storage_name = 'edmans{env}data001'):
  env_name = os.getenv('ENV_NAME')
  storage_name = storage_name.replace('{env}', env_name)  
  return f'abfss://{container_name}@{storage_name}.dfs.core.windows.net'  

def get_file_path(folder_name, file_name, container_name = 'datalake', storage_name = 'edmans{env}data001'):
  endpoint_name = get_endpoint_name(container_name, storage_name)
  return f"{endpoint_name}{folder_name}/{file_name}"

def get_file_paths(item_list, file_ext = 'parquet'):
  parquet_list = []
  if isinstance(item_list, str):
    item_list = dbutils.fs.ls(item_list)
    parquet_list += get_file_paths(item_list)
  else:
    for item in item_list:
      if item.isDir():
        new_item_list = dbutils.fs.ls(item.path)
        parquet_list += get_file_paths(new_item_list)
      elif f'.{file_ext}' in item.name:
        parquet_list.append(item.path)
  return parquet_list

def print_dataframe_as_dict(df):
  print('schema = {')
  for data_type in df.dtypes:    
    print("\t'{0}':'{1}',".format(data_type[0], data_type[1]))
  print('}')
  
def run_for_each_table(database_name, function, args = {}):
  table_names = [table.name for table in spark.catalog.listTables(database_name)]
  for table_name in table_names:
    table_name = database_name + '.' + table_name   
    args['table_name'] = table_name
    
    msg = '{0}: {1}'.format(function.__name__, table_name)    
    print(msg)
    
    function(**args)

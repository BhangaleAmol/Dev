# Databricks notebook source
# MAGIC %run ./func_helpers

# COMMAND ----------

def add_column(table_name, col_name, col_type, options = {}):  
  
  default_value = options.get('default_value')
  after_col = options.get('after_col')
  
  if not table_exists(table_name):
    print('TABLE DOES NOT EXIST')
    return
    
  if _column_exists(table_name, col_name):
    print(f'COLUMN {col_name} ALREADY EXISTS IN TABLE {table_name}')
    
  else:
    print(f'ADDING COLUMN {col_name} TO TABLE {table_name}')
  
    after_str = ''
    if after_col is not None:
      after_str = f"AFTER {after_col}"

    query = f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_type} {after_str})"
    print(query)
    spark.sql(query)  
  
  if default_value is not None:
    query = f"UPDATE {table_name} SET {col_name} = '{default_value}' WHERE {col_name} IS NULL"
    print(query)
    spark.sql(query)

def alter_table_partition(table_name, partitions):
  
  location = get_table_location(table_name)
  
  spark.read.table(table_name) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partitions) \
    .saveAsTable(table_name)  
  
  _configure_table(table_name)    
    
def alter_table_path(table_name, target_folder):
  
  database_name, s_table_name = _split_table_name(table_name)  
  table_path = get_table_location(table_name)  
  
  if table_path == target_folder:    
    message = f'NEW TABLE PATH {target_folder} IS THE SAME AS CURRENT PATH'
    print(message)      
  else:  
    print(f"MOVING TABLE {table_name}")
    print(f"old path: {table_path}")
    print(f"new path: {target_folder}")
    
    dbutils.fs.rm(target_folder, True)
    dbutils.fs.cp(table_path, target_folder, True)    
  
    query = f"ALTER TABLE {table_name} SET LOCATION '{target_folder}'"
    print(query)
    spark.sql(query)
    
    dbutils.fs.rm(table_path, True)

def append_into_table(df, table_name, options = {}):
  
  database_name, s_table_name = _split_table_name(table_name)
  debug = options.get('debug', True)
  incremental_column = options.get('incremental_column') 
  
  df.createOrReplaceTempView('delta')
  
  if incremental_column:
    query = "DELETE FROM {0} WHERE {1} >= (SELECT MIN({1}) FROM delta)".format(table_name, incremental_column)
    if debug: print(query)  
    spark.sql(query)
  
  query = "INSERT INTO {0} SELECT * FROM delta".format(table_name)
  if debug: print(query)
  spark.sql(query)    

def apply_hard_delete(df, table_name, key_columns, options = {}):  
  
  debug = options.get('debug', True)
  date_field = options.get('date_field')
  date_value = options.get('date_value')
  
  if isinstance(key_columns, str):    
    key_columns = [column.strip() for column in key_columns.split(',')]
    
  # keys
  keys_str = ','.join(key_columns)
  
  # where
  where_str = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in key_columns])
  
  # date  
  date_str = ''
  if date_field is not None and date_value is not None:
    date_str = " AND t.{0} = '{1}'".format(date_field, date_value)
  
  df.createOrReplaceTempView('delta')
    
  query = "DELETE FROM {0} t WHERE 1=1 {3} AND NOT EXISTS (SELECT {1} FROM delta WHERE {2})".format(table_name,keys_str,where_str,date_str)
  if debug: print(query)
  spark.sql(query)
  
def apply_soft_delete(df, table_name, key_columns, options = {}):  
  
  source_name = options.get('source_name')
  date_field = options.get('date_field')
  date_value = options.get('date_value')
  flag_name = options.get('flag_name', '_DELETED')
  flag_value = options.get('flag_value', True)
  debug = options.get('debug', True)
  
  if isinstance(key_columns, str):    
    key_columns = [column.strip() for column in key_columns.split(',')]
  
  # keys
  keys_str = ','.join(key_columns)
  
  # where
  where_str = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in key_columns])
  
  # source
  source_str = ''
  if source_name is not None:
    source_str = " AND _SOURCE = '{0}'".format(source_name)
  
  # date
  date_str = ''
  if date_field is not None and date_value is not None:
    date_str = " AND t.{0} = '{1}'".format(date_field, date_value)
  
  df.createOrReplaceTempView('delta')
  
  query = "UPDATE {0} t SET t.{1} = '{2}' WHERE 1=1{3}{4} AND NOT EXISTS (SELECT {5} FROM delta WHERE {6})".format(
    table_name, flag_name, flag_value, source_str, date_str, keys_str, where_str)
  
  if debug: print(query)
  spark.sql(query)

def clone_table(source_table, target_table, target_path):
  target_database, _ = _split_table_name(target_table) 
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")
  spark.sql(f"CREATE OR REPLACE TABLE delta.`{target_path}` CLONE {source_table}")
  spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA LOCATION '{target_path}'")

def drop_database(database_name): 
  table_names = get_tables(database_name)
  for table_name in table_names:
    drop_table(table_name)
  query = f'DROP DATABASE {database_name}'
  print(query)
  spark.sql(query) 

def drop_all_tables(database_name):
  table_names = get_tables(database_name)
  for table_name in table_names:
    drop_table(table_name)    
  
def drop_table(table_name, remove_data = True): 
  
  table_exist = table_exists(table_name)
  if (not table_exist):
    return
  
  if remove_data:
    location = get_table_location(table_name)
  
  query = f'DROP TABLE {table_name}'
  print(query)
  spark.sql(query)  
  
  if remove_data:    
    message = f'DROP DATA LAKE FOLDER ({location})'
    print(message)
    dbutils.fs.rm(location, True)  

def drop_column(table_name, col_name, options = {}):
  
  debug = options.get('debug', True)
    
  table_exist = table_exists(table_name)  
  if (not table_exist):
    if debug: print(f"TABLE [{table_name}] DOES NOT EXIST")
    return
  
  table_path = get_table_location(table_name)
  partition = _get_table_partition(table_name)
  col_name = col_name.split(',') if isinstance(col_name, str) else col_name

  for col in col_name:
    colum_exists = _column_exists(table_name, col)
    if (not colum_exists):
      if debug: print(f"COLUMN [{col}] DOES NOT EXIST IN {table_name} TABLE")
      return    
  
  spark.table(table_name) \
    .drop(*col_name) \
    .write \
    .format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", table_path) \
    .partitionBy(*partition) \
    .saveAsTable(table_name)
  
  _configure_table(table_name, debug = debug)    

def get_databases():  
  return [row.databaseName for row in spark.sql('SHOW DATABASES').collect()]

def get_table_location(table_name):
  desc = spark.sql(f'DESC FORMATTED {table_name}')  
  location = desc.filter("col_name = 'Location'").select('data_type').collect()[0][0]
  if location == 'string':
    location = desc.filter("col_name = 'Location'").select('data_type').collect()[1][0]    
  return location

def get_table_schema(table_name):
  schema_list = spark.sql(f'DESCRIBE TABLE {table_name}').collect()
  split_index = [r.col_name for r in schema_list].index('')
  return {r.col_name: r.data_type for idx, r in enumerate(schema_list) if idx < split_index}

def get_tables(database_name):
  spark.sql(f'USE {database_name}')
  data = spark.sql('SHOW TABLES').select('tableName').collect()
  return [database_name + '.' + row.tableName for row in data if row.tableName[-5:] != 'delta']   

def get_views(database_name):
  spark.sql(f'USE {database_name}')
  data = spark.sql('SHOW VIEWS').select('viewName').collect()
  return [database_name + '.' + row.viewName for row in data] 

def merge_into_table(df, table_name, key_columns, options = {}):
  
  auto_merge = options.get('auto_merge', False)
  database_name, s_table_name = _split_table_name(table_name)
  debug = options.get('debug', True)
  source_name = options.get('source_name') 
  
  if isinstance(key_columns, str):
    key_columns = [column.strip() for column in key_columns.split(',')]  
  
  if auto_merge is True:
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    if debug: print("SCHEMA EVOLUTION ON") 
  
  spark.sql(f"USE {database_name}")
  df.createOrReplaceTempView('delta')
  
  # join
  join_str = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in key_columns])  

  # source name
  if source_name:

    join_str = f"{join_str} AND t._SOURCE = '{source_name}'"
 
  query = f"MERGE INTO {table_name} t USING delta ON {join_str} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"

  if debug: print(query)
  spark.sql(query)

def register_hive_database(database_name, options = {}):
  
  debug = options.get('debug', False)
  
  query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
  if debug: print(query)
  spark.sql(query)
  
  query = f"USE {database_name}"
  if debug: print(query)
  spark.sql(query)  

def register_hive_table(df, table_name, target_folder, options = {}):
  
  _check_null_data_types(df)   
  
  database_name, s_table_name = _split_table_name(table_name)
  
  configure_table = options.get('configure_table', True)
  container_name = options.get('container_name', 'datalake')
  debug = options.get('debug', True)
  file_extension = options.get('file_extension', 'dlt')
  overwrite = options.get('overwrite', False)
  partition_column = options.get('partition_column')
  storage_name = options.get('storage_name', 'edmans{env}data001')
  
  # partition_column   
  if partition_column is None and '_PART' in df.columns:
    partition_column = ['_PART']
  
  if partition_column is not None:
    
    if isinstance(partition_column, str):
      partition_column = [column.strip() for column in partition_column.split(',')]
    
    not_found = [c for c in partition_column if c not in df.columns]    
    if len(not_found) > 0:
      message = f"SOME PARTITION COLUMNS NOT PRESENT IN DATASET"
      raise Exception(message)
  
  file_name = s_table_name + '.' + file_extension
  table_path = get_file_path(target_folder, file_name, container_name, storage_name)
  register_hive_database(database_name, options = {'debug': debug})
  
  table_exist = table_exists(table_name) 
  if table_exist and debug:
    message = f'TABLE {table_name} ALREADY EXISTS'
    print(message)
  
  if table_exist and not overwrite:
    return
  
  # overwrite existing
  if table_exist and overwrite:
    message = f'TABLE {table_name} OVERWRITE'
    if debug: print(message)  
  
  message = f"REGISTER TABLE {table_name} ({table_path})"
  if debug: print(message)

  empty_df = spark.createDataFrame([], df.schema)    
  operation = empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", table_path) \
    .option("overwriteSchema", "true")

  if partition_column is not None:
    message = f"PARTITION BY {partition_column}"
    if debug: print(message)
    operation.partitionBy(partition_column)      

  operation.saveAsTable(s_table_name)
  
  if configure_table:
    _configure_table(table_name, debug = debug)
  return True

def table_exists(table_name):  
  try:
    database_name, s_table_name = _split_table_name(table_name)
    spark.sql(f'use {database_name}')   
    table_names = [row['tableName'] for row in spark.sql('show tables').collect() if not row['isTemporary']]
    table_exists = s_table_name in table_names
  except Exception as e:
    table_exists = False
  
  return table_exists

# COMMAND ----------

def _check_null_data_types(df):
  for data_type in df.dtypes:
    if data_type[1] == 'null':
      message = 'Column {0} has NULL data type.'.format(data_type[0])
      raise Exception(message)

def _configure_table(table_name, debug = False):
  try:
    query = "ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(table_name)
    if debug: print(query)
    spark.sql(query)
  except Exception as e:
    print(e)

def _column_exists(table_name, col_name):
  columns = spark.sql("SHOW COLUMNS IN {0}".format(table_name)).select('col_name').collect()
  column_list = [row[0] for row in columns]
  return (col_name in column_list)

def _get_table_partition(table_name):
  try:
    partitions = spark.sql("SHOW PARTITIONS {0}".format(table_name)).columns
  except Exception as e:
    partitions = []
  return partitions

# COMMAND ----------



# Databricks notebook source
# MAGIC %run ./func_helpers

# COMMAND ----------

def add_hive_table_column(table_name, col_name, col_type, default_value = None, after_col = None):  
  
  if not hive_table_exists(table_name):
    print('Table does not exist'.format())
    return
    
  if table_column_exists(table_name, col_name):
    print('Column {0} already exists in {1} table'.format(col_name, table_name))
    
  else:
    print('Adding column {0} to {1} table '.format(col_name, table_name))
  
    if after_col is not None:
      after_str = "AFTER {0}".format(after_col)
    else:
      after_str = ''

    query = "ALTER TABLE {0} ADD COLUMNS ({1} {2} {3})".format(table_name, col_name, col_type, after_str)
    print(query)
    spark.sql(query)  
  
  if default_value is not None:
    query = "UPDATE {0} SET {1} = '{2}' WHERE {1} IS NULL".format(table_name, col_name, default_value)
    print(query)
    spark.sql(query)

def alter_hive_table_path(table_name, target_folder):
  
  database_name = table_name.split('.')[0].lower()
  s_table_name = table_name.split('.')[1].lower()  

  table_path = get_hive_table_location(table_name)
  
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
    
def alter_column_name(table_name, col_name, new_col_name):   
  df = spark.read.table(table_name).withColumnRenamed(col_name, new_col_name)
  location = get_hive_table_location(table_name)
  partition = get_hive_table_partition(table_name)
  df.write.format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partition) \
    .saveAsTable(table_name)
  
  configure_hive_table(table_name)

def alter_hive_table_partition(table_name, partitions):  
  df = spark.read.table(table_name)
  location = get_hive_table_location(table_name)
  df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partitions) \
    .saveAsTable(table_name)  
  
  configure_hive_table(table_name)
  
def append_into_hive_table(df, table_name, incremental_column = None):

  table_name = table_name.lower()
  
  df.createOrReplaceTempView('delta')
  
  if incremental_column:
    query = "DELETE FROM {0} WHERE {1} >= (SELECT MIN({1}) FROM delta)".format(table_name, incremental_column)
    print(query)  
    spark.sql(query)
  
  query = "INSERT INTO {0} SELECT * FROM delta".format(table_name)
  print(query)
  spark.sql(query)

def apply_hard_delete(df, table_name, key_columns, source_name = None, date_field = None, date_value = None):
  print("APPLY HARD DELETE")
  
  full_table_name = table_name
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()

  df.createOrReplaceTempView('delta')
  
  columns = [column.strip() for column in key_columns.split(',')]
  where = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in columns])
  
  if source_name is not None:
    source = " AND _SOURCE = '{0}'".format(source_name)
  else:
    source = ''
  
  if date_field is not None and date_value is not None:
    date_str = " AND t.{0} = '{1}' ".format(date_field, date_value)
  else:
    date_str = ''
  
  query = "DELETE FROM {0}.{1} t WHERE 1=1 {2} {5} AND NOT EXISTS (SELECT {3} FROM delta WHERE {4})".format(
    database_name, table_name, source, key_columns, where, date_str)
  print(query)
  spark.sql(query)  
  
def apply_soft_delete(df, table_name, key_columns, source_name = None, date_field = None, date_value = None, flag_name = '_DELETED', flag_value = True, dataset_name = None):  
  print("APPLY SOFT DELETE")
   
  full_table_name = table_name
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  
  df.createOrReplaceTempView('delta')

  columns = [column.strip() for column in key_columns.split(',')]
  where = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in columns])
  
  if source_name is not None:
    source = " AND _SOURCE = '{0}'".format(source_name)
  else:
    source = ''

  if dataset_name is not None:
    dataset = " AND _DATASET = '{0}'".format(dataset_name)
  else:
    dataset = ''
    
  if date_field is not None and date_value is not None:
    if len(date_value)>1 and type(date_value) == list:
      date_val = str(date_value).replace('[','(').replace(']',')')
      date_str = " AND to_date(t.{0}) in {1}".format(date_field, date_val)
    else:
      date_str = " AND t.{0} = '{1}'".format(date_field, date_value)
  else:
    date_str = ''

  query = "UPDATE {0}.{1} t SET t.{2} = '{3}', t._Modified = CURRENT_TIMESTAMP() WHERE 1=1{4}{5}{8} AND t.{2} IS FALSE AND NOT EXISTS (SELECT {6} FROM delta WHERE {7})".format(
    database_name, table_name, flag_name, flag_value, source, date_str, key_columns, where,dataset)
  print(query)
  spark.sql(query)
  
def configure_hive_table(table_name):
  try:
    query = "ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(table_name)
    print(query)
    spark.sql(query)
  except Exception as e:
    print(e)

def database_exists(database_name):
  databases = spark.sql("SHOW DATABASES").select('namespace').collect()
  database_list = [row[0] for row in databases]
  return database_name in database_list  
  
def delete_from_hive_table(table_name, source_name):
  print("DELETE FROM HIVE TABLE")
  
  full_table_name = table_name.lower()
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  spark.sql("USE {0}".format(database_name))
  
  if source_name is not None:    
    query = "DELETE FROM {0} t WHERE t._SOURCE = '{1}'".format(table_name, source_name)  
    print(query)
    spark.sql(query)  

def drop_hive_database(database_name):
  spark.sql(f'use {database_name}')  
  table_names = [row['tableName'] for row in spark.sql('show tables').collect()]
  for table_name in table_names:
    full_table_name = database_name + '.' + table_name
    print('DROPPING TABLE {0}'.format(full_table_name))
    drop_hive_table(full_table_name)
  print('DROPPING DATABASE {0}'.format(database_name))
  spark.sql("DROP DATABASE {0}".format(database_name))   
    
def drop_hive_table(table_name, remove_data = True): 
  
  table_name = table_name.lower()
  table_exists = hive_table_exists(table_name)
  if (not table_exists):
    return
  
  if remove_data:
    location = get_hive_table_location(table_name)
  
  query = "DROP TABLE {0}".format(table_name)
  print(query)
  spark.sql(query)  
  
  if remove_data:    
    message = "DROP DATA LAKE FOLDER ({0})".format(location)
    print(message)
    dbutils.fs.rm(location, True)  

def drop_hive_table_column(table_name, col_name):
   
  location = get_hive_table_location(table_name)
  partition = get_hive_table_partition(table_name)
  col_name = col_name.split(',') if isinstance(col_name, str) else col_name
  col_name = [col_name] if isinstance(col_name, str) else col_name
  
  df = spark.table(table_name) \
    .drop(*col_name) \
    .write \
    .format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partition) \
    .saveAsTable(table_name)
  
  configure_hive_table(table_name)
    
def get_hive_table_location(table_name):
  desc = spark.sql("DESC FORMATTED {0}".format(table_name))  
  location = desc.filter("col_name = 'Location'").select('data_type').collect()[0][0]
  if location == 'string':
    location = desc.filter("col_name = 'Location'").select('data_type').collect()[1][0]
    
  return location

def get_hive_table_partition(table_name):
  try:
    partitions = spark.sql("SHOW PARTITIONS {0}".format(table_name)).columns
  except Exception as e:
    partitions = []
  return partitions

def get_hive_table_schema(table_name):
  schema_list = spark.sql(f'DESCRIBE TABLE {table_name}').collect()
  split_index = [r.col_name for r in schema_list].index('')
  return {r.col_name: r.data_type for idx, r in enumerate(schema_list) if idx < split_index}

def hive_table_exists(table_name):
  
  full_table_name = table_name
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  try:
    spark.sql(f'use {database_name}')   
    table_names = [row['tableName'] for row in spark.sql('show tables').collect() if not row['isTemporary']]
    table_exists = table_name in table_names
  except Exception as e:
    table_exists = False
  
  return table_exists

def merge_into_hive_table(df, table_name, key_columns, options = {}):
  
  full_table_name = table_name.lower()
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  source_name = options.get('source_name')
  auto_merge = options.get('auto_merge', True)
  
  if auto_merge:
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    print("SCHEMA EVOLUTION ON")
    
  spark.sql("USE {0}".format(database_name))
  df.createOrReplaceTempView('delta')
  
  # join
  if isinstance(key_columns, str):
    key_columns = [column.strip() for column in key_columns.split(',')]  
  join_str = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in key_columns])  

  # source name
  if source_name is not None:
    filter_str = "t._SOURCE = '{0}'".format(source_name)
    join_str = join_str + " AND " + filter_str  
 
  query = "MERGE INTO {0} t USING delta ON {1} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(table_name, join_str)
  print(query)
  spark.sql(query)

def optimize_hive_table(table_name, debug = False):
  
  query = "REFRESH TABLE {0}".format(table_name)
  if debug: print(query)
  spark.sql(query)
  
  query = "ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(table_name)
  if debug: print(query)  
  spark.sql(query)
  
  query = "OPTIMIZE {0}".format(table_name)
  if debug: print(query)  
  spark.sql(query)  
  
def register_hive_table(df, table_name, target_folder, options = {}):
  
  validate_data_types(df)  
  
  full_table_name = table_name.lower()
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  partition_column = options.get('partition_column')
  target_container = options.get('target_container', 'datalake')
  target_storage = options.get('target_storage', 'edmans{env}data001')
    
  table_path = get_file_path(target_folder, f'{table_name}.par', target_container, target_storage)
  
  spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(database_name))
  spark.sql("USE {0}".format(database_name))  
  
  table_exists = hive_table_exists(full_table_name)  
  if table_exists is True:
    print("TABLE " + full_table_name + ' ALREADY EXISTS')
    
  message = "REGISTER HIVE TABLE %s (%s)" % (full_table_name, table_path)
  print(message)

  empty_df = spark.createDataFrame([], df.schema)    
  cmd = empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true")

  if table_exists is False:
    cmd.option("path", table_path)
  
  if partition_column is not None:
    cmd.partitionBy(partition_column)      

  cmd.saveAsTable(table_name)
  configure_hive_table(full_table_name)
  return True

def rename_hive_table(table_name, new_table_name):
   
  partition = get_hive_table_partition(table_name)
  location = get_hive_table_location(table_name)   
  new_location = location[:location.rfind('/')] + '/' + new_table_name + '.par'
  
  print("CREATING TABLE {0} under path {1}".format(new_table_name, new_location))  

  spark.table(table_name) \
    .write \
    .format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", new_location) \
    .partitionBy(*partition) \
    .saveAsTable(new_table_name) 
  
  configure_hive_table(new_table_name)
  drop_hive_table(table_name, True)

def table_column_exists(table_name, col_name):
  columns = spark.sql("SHOW COLUMNS IN {0}".format(table_name)).select('col_name').collect()
  column_list = [row[0] for row in columns]
  return (col_name in column_list)

def vacuum_hive_table(table_name, retention = 720):
  try:
    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.vacuum(retention)
  except Exception as e:
    print(e)

def validate_data_types(df):
  for data_type in df.dtypes:
    if data_type[1] == 'null':
      message = 'Column {0} has NULL data type.'.format(data_type[0])
      raise Exception(message)

# COMMAND ----------

def alter_column_data_type(table_name, col_name, new_type):   
  df = spark.read.table(table_name).withColumn(col_name, f.col(col_name).cast(new_type))
  location = get_hive_table_location(table_name)
  partition = get_hive_table_partition(table_name)
  df.write.format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partition) \
    .saveAsTable(table_name)
  
  configure_hive_table(table_name)

# COMMAND ----------

def alter_hive_table_column(table_name, col_name, col_type, default_value = None, after_col = None):  
  
  if not hive_table_exists(table_name):
    print('Table does not exist'.format())
    return
    
  if table_column_exists(table_name, col_name):
    print('Column {0} already exists in {1} table'.format(col_name, table_name))
    
#   else:
    print('Altering column {0} to {1} table '.format(col_name, table_name))

    if after_col is not None:
      after_str = "AFTER {0}".format(after_col)
    else:
      after_str = ''

    query = "ALTER TABLE {0} CHANGE COLUMN {1} {1} {2} {3}".format(table_name, col_name, col_type, after_str)
    print(query)
    spark.sql(query)  
  
#   if default_value is not None:
#     query = "UPDATE {0} SET {1} = '{2}' WHERE {1} IS NULL".format(table_name, col_name, default_value)
#     print(query)
#     spark.sql(query)


# COMMAND ----------

def add_unknown_record(main_f, table_name):
  print('ADD UNKNOWN RECORD')
  df = get_unknown_record(main_f.schema)
  options = {'source_name': "DEFAULT", 'auto_merge': False}
  merge_into_hive_table(df, table_name, '_ID', options)   

def merge_to_delta(main_f, table_name, target_folder, overwrite, options = {}):   
  
  auto_merge = options.get('auto_merge', False)
  target_container = options.get('target_container', 'datalake')
  target_storage = options.get('target_storage', 'edmans{env}data001')
  
  table_exists = hive_table_exists(table_name)
  
  # REGISTER TABLE
  if (overwrite and table_exists) or not table_exists:
    print('OVERWRITE TABLE')
    options = {
      'partition_column': ["_PART"], 
      'target_container': target_container, 
      'target_storage': target_storage
    }
    register_hive_table(main_f, table_name, target_folder, options)
  
  # MERGE WITH KEYS
  print('MERGE WITH KEYS')
  merge_into_hive_table(main_f, table_name, '_ID', {'auto_merge': auto_merge})

def merge_to_delta_agg(main_f, table_name, source_name, target_folder, options = {}):   
  
  auto_merge = options.get('auto_merge', False)
  overwrite = options.get('overwrite', False)
  target_container = options.get('target_container', 'datalake')
  target_storage = options.get('target_storage', 'edmans{env}data001')
  
  table_exists = hive_table_exists(table_name)
  
  # OVERWRITE
  if overwrite and table_exists:
    print('OVERWRITE SOURCE DATA')
    delete_from_hive_table(table_name, source_name)
    
  # REGISTER TABLE
  if not table_exists:
    options = {
      'partition_column': ["_SOURCE", "_PART"], 
      'target_container': target_container, 
      'target_storage': target_storage
    }
    register_hive_table(main_f, table_name, target_folder, options)
    table_exists = True
  
  # MERGE WITH KEYS
  print('MERGE WITH KEYS')
  options = {'source_name': source_name, 'auto_merge': auto_merge}
  merge_into_hive_table(main_f, table_name, '_ID', options)
  
def merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options = {}):

  partition_column = options.get('partition_column')
  incremental_column = options.get('incremental_column')
  overwrite = options.get('overwrite', False)
  append_only = options.get('append_only', False)
  sampling = options.get('sampling', False)
  incremental = options.get('incremental', False)
  target_container = options.get('target_container', 'datalake')
  target_storage = options.get('target_storage', 'edmans{env}data001')
    
  table_exists = hive_table_exists(table_name)

  # REGISTER TABLE
  if (overwrite and table_exists) or not table_exists:
    print('OVERWRITE')
    options = {
      'partition_column': partition_column, 
      'target_container': target_container, 
      'target_storage': target_storage
    }
    register_hive_table(main_f, table_name, target_folder, options)  

  # SAMPLING
  if sampling:
    print('SAMPLING')
    if key_columns is not None:
      merge_into_hive_table(main_f, table_name, key_columns)

  # APPEND ONLY
  elif append_only:
    print('APPEND ONLY')
    append_into_hive_table(main_f, table_name, incremental_column)

  # MERGE WITH KEYS
  elif key_columns is not None:
    print('MERGE WITH KEYS')
    merge_into_hive_table(main_f, table_name, key_columns)

  # FULL OVERWRITE
  elif incremental == False and key_columns is None:
    print('FULL OVERWRITE')
    options = {
      'partition_column': partition_column, 
      'target_container': target_container, 
      'target_storage': target_storage
    }
    register_hive_table(main_f, table_name, target_folder, options)
    append_into_hive_table(main_f, table_name)

  # RAISE ERROR
  else:
    raise Exception("NO JOIN CRITERIA")

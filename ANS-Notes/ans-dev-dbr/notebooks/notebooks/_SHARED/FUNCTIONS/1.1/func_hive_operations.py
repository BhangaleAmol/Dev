# Databricks notebook source
def alter_column_name(table_name, col_name, new_col_name):
   
  location = get_hive_table_location(table_name)
  partition = get_hive_table_partition(table_name)
  
  df = spark.table(table_name) \
    .withColumnRenamed(col_name, new_col_name) \
    .write \
    .format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name + '_tmp')
    
  df_tmp = spark.table(table_name + '_tmp')
  df_tmp \
    .write \
    .format('delta') \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", location) \
    .partitionBy(*partition) \
    .saveAsTable(table_name) 
  
  spark.sql("DROP TABLE {0}".format(table_name + '_tmp'))
  spark.sql("REFRESH TABLE {0}".format(table_name))
  
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

def configure_hive_table(table_name):  
  query = "ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(table_name)
  print(query)
  spark.sql(query)

def database_exists(database_name):
  databases = spark.sql("SHOW DATABASES").select('namespace').collect()
  database_list = [row[0] for row in databases]
  return database_name in database_list
  
def drop_hive_database(database_name):
  table_names = [table.name for table in spark.catalog.listTables(database_name)]
  for table_name in table_names:
    full_table_name = database_name + '.' + table_name
    print('DROPPING TABLE {0}'.format(full_table_name))
    drop_hive_table(full_table_name)
  print('DROPPING DATABASE {0}'.format(database_name))
  spark.sql("DROP DATABASE {0}".format(database_name))
  
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

def drop_hive_table(table_name, remove_data = True): 
  
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

def hive_table_exists(table_name, debug = False):
  
  full_table_name = table_name  
  database_name = table_name.split('.')[0]
  table_name = table_name.split('.')[1]
  
  table_names = [table.name for table in spark.catalog.listTables(database_name)]
  table_exists = table_name in table_names
  
  if debug:
    if table_exists is True:
      print("TABLE " + full_table_name + ' ALREADY EXISTS')
    else:
      print("TABLE " + full_table_name + ' DOES NOT EXIST')
  
  return table_exists

def table_column_exists(table_name, col_name):
  columns = spark.sql("SHOW COLUMNS IN {0}".format(table_name)).select('col_name').collect()
  column_list = [row[0] for row in columns]
  return (col_name in column_list)

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

# COMMAND ----------

def append_into_hive_table(df, database_name = None, table_name = None, incremental_column = None):
  print("APPEND INTO HIVE TABLE")
  
  if database_name is None:
    database_name = params.get('database_name').lower()
    
  if table_name is None:
    table_name = params.get('table_name').lower()
	
  if incremental_column is None:
    incremental_column = params.get('incremental_column')
  
  df.createOrReplaceTempView('delta')  
    
  if incremental_column:
    query = "DELETE FROM {0}.{1} WHERE {2} >= (SELECT MIN({2}) FROM delta)".format(database_name, table_name, incremental_column)
    print(query)  
    spark.sql(query) 
  
  query = "INSERT INTO {0}.{1} SELECT * FROM delta".format(database_name, table_name)
  print(query)
  spark.sql(query)

def apply_hard_delete(df, database_name = None, table_name = None, key_columns = None):
  print("APPLY HARD DELETE")
  
  if database_name is None:
    database_name = params.get('database_name').lower()
    
  if table_name is None:
    table_name = params.get('table_name').lower()
  
  if key_columns is None:
    key_columns = params.get('key_columns')

  df.createOrReplaceTempView('delta')
  
  columns = [column.strip() for column in key_columns.split(',')]
  where = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in columns])
  query = "DELETE FROM {0}.{1} t WHERE NOT EXISTS (SELECT {2} FROM delta WHERE {3})".format(
    database_name, table_name, key_columns, where)
  print(query)
  spark.sql(query)

def apply_soft_delete(df, database_name = None, table_name = None, key_columns = None, flag_name = '_DELETED', flag_value = True):  
  print("APPLY SOFT DELETE")
  
  if database_name is None:
    database_name = params.get('database_name').lower()
    
  if table_name is None:
    table_name = params.get('table_name').lower()
  
  if key_columns is None:
    key_columns = params.get('key_columns')
  
  df.createOrReplaceTempView('delta')

  columns = [column.strip() for column in key_columns.split(',')]
  where = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in columns])
  query = "UPDATE {0}.{1} t SET t.{2} = '{3}' WHERE NOT EXISTS (SELECT {4} FROM delta WHERE {5})".format(
    database_name, table_name, flag_name, flag_value, key_columns, where)
  print(query)
  spark.sql(query)
  
def merge_into_hive_table(df, database_name = None, table_name = None, key_columns = None, overwrite = None, filter_column = None, filter_value = None, auto_merge = True):
  print("MERGE INTO HIVE TABLE")
  
  if database_name is None:
    database_name = params.get('database_name').lower()
    
  if table_name is None:
    table_name = params.get('table_name').lower()
    
  if key_columns is None:  
    key_columns = params.get('key_columns')
    
  if overwrite is None:  
    overwrite = params.get('overwrite', False)  
  
  if auto_merge:
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    print("SCHEMA EVOLUTION ON")
    
  spark.sql("USE {0}".format(database_name))
  df.createOrReplaceTempView('delta')
  
  # join
  columns = [column.strip() for column in key_columns.split(',')]  
  join_str = ' AND '.join(["t.{0} = delta.{0}".format(column) for column in columns])  

  # filter columns
  if filter_column is not None and filter_value is not None:
    filter_str = "t.{0} = '{1}'".format(filter_column, filter_value)
    join_str = join_str + " AND " + filter_str
  
  # overwrite subset
  if overwrite and filter_column is not None and filter_value is not None:    
    query = "DELETE FROM {0} t WHERE {1}".format(table_name, filter_str)  
    print(query)
    spark.sql(query)
  
  query = "REFRESH TABLE {0}".format(table_name)
  print(query)
  spark.sql(query)
  
  query = "MERGE INTO {0} t USING delta ON {1} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(table_name, join_str)
  print(query)
  spark.sql(query)
  
def register_hive_table(df, database_name = None, table_name = None, target_folder = None, partition_column = None, overwrite = None, optimize = False):
  
  validate_data_types(df)
  
  if database_name is None:
    database_name = params.get('database_name').lower()
    
  if table_name is None:
    table_name = params.get('table_name').lower()
  
  if target_folder is None:
    target_folder = params.get('target_folder')
  
  if partition_column is None:
    partition_column = params.get('partition_column', None)
  
  if overwrite is None:
    overwrite = params.get('overwrite', False)
  
  full_table_name = database_name + '.' + table_name
  file_path = DATALAKE_ENDPOINT + target_folder + '/' + table_name + '.par' 
  
  spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(database_name))
  spark.sql("USE {0}".format(database_name))  

  table_exists = hive_table_exists(full_table_name)  
  
  if (not table_exists) or (table_exists and overwrite):
    message = "INIT TABLE %s (%s)" % (full_table_name, file_path)
    print(message)

    empty_df = spark.createDataFrame([], df.schema)    
    cmd = empty_df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("path", file_path) \
      .option("overwriteSchema", "true")

    if partition_column is not None:
      cmd.partitionBy(partition_column)      
   
    cmd.saveAsTable(table_name)
  
  if (not table_exists):
    configure_hive_table(full_table_name)
  
  if optimize:    
    optimize_hive_table(full_table_name)
    
def validate_data_types(df):
  for data_type in df.dtypes:
    if data_type[1] == 'null':
      message = 'Column {0} has NULL data type.'.format(data_type[0])
      raise Exception(message)

# COMMAND ----------



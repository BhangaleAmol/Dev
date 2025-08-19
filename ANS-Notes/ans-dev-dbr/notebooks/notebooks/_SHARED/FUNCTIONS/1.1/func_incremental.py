# Databricks notebook source
def get_incr_col_max_value(df, incremental_column = None, incremental_type = None):    
  
  if incremental_column is None:
    incremental_column = params.get('incremental_column') 
  
  if incremental_type is None:
    incremental_type = params.get('incremental_type', "timestamp") 
  
  default_value = '1900-01-01T00:00:00' if (incremental_type == 'timestamp') else 0
  
  if df is None:
    return default_value 
  
  if incremental_column is None:
    return default_value
  
  else:
    if incremental_column not in df.columns:
      raise Exception("Column " + incremental_column + " not present in the dataset")
    
    if str(df.schema[incremental_column].dataType) == 'TimestampType':
      
      max_value = (
        df.select(f.date_format(f.max(f.col(incremental_column)), 'yyyy-MM-dd HH:mm:ss').alias("max"))
        .limit(1).collect()[0].max
      )      
    
    elif str(df.schema[incremental_column].dataType) == 'DateType':
      
      max_value = (
        df.select(f.date_format(f.max(f.col(incremental_column)), 'yyyy-MM-dd').alias("max"))
        .limit(1).collect()[0].max
      )      
    
    else:
      max_value = (
        df.select(f.max(f.col(incremental_column)).alias("max"))
        .limit(1).collect()[0].max
      )
          
    if max_value is None:
      max_value = default_value
    
    return str(max_value)
  
def load_main_dataset(source_table, incremental_column = None, cutoff_value = None, incremental = None, sampling = None):
  
  if incremental is None:
    incremental = params.get('incremental', False) 
  
  if incremental_column is None:
    incremental_column = params.get('incremental_column') 
  
  if cutoff_value is None:
    cutoff_value = params.get('cutoff_value')
  
  if sampling is None:
    sampling = params.get('sampling', False)
  
  if incremental and (incremental_column is None or cutoff_value is None):
    raise Exception("No incremental column or cutoff value.")
  
  if incremental:
    query = "SELECT * FROM {0} WHERE {1} > '{2}'".format(source_table, incremental_column, cutoff_value)
    df = spark.sql(query)    
    print(query)    
  else:
    df = spark.table('{0}'.format(source_table))
    
  if sampling:
    df = df.limit(10)
  
  print("{0} number of rows: {1}".format(source_table, df.count()))
  return df

# COMMAND ----------

def get_cutoff_value(source_table = None, database_name = None, table_name = None, 
  incremental = None, incremental_type = None, prune_days = None, date_format = None):  
  
  if database_name is None:
    database_name = params.get('database_name') 
  
  if table_name is None:
    table_name = params.get('table_name') 
  
  if incremental is None:
    incremental = params.get('incremental', False)
  
  if incremental_type is None:
    incremental_type = params.get('incremental_type', "timestamp")

  if prune_days is None:
    prune_days = params.get('prune_days', 30)

  if date_format is None:
    date_format = params.get('date_format', "%Y-%m-%d")
  
  record = {}
  record = get_timestamp_record(database_name, table_name, source_table)
  
  # not incremental
  cutoff_value = '1900-01-01T00:00:00'
  
  # timestamp
  if incremental and incremental_type == 'timestamp':
    
    max_datetime = record["MAX_DATETIME"]
    max_datetime_dt = dt.datetime.strptime(max_datetime, '%Y-%m-%dT%H:%M:%SZ')

    cutoff_datetime_dt = dt.datetime.strptime('1900-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    if max_datetime != '1900-01-01T00:00:00Z' and prune_days is not None:
      cutoff_datetime_dt = max_datetime_dt + dt.timedelta(days=-prune_days)

    cutoff_value = cutoff_datetime_dt.strftime(date_format)
    cutoff_value = cutoff_value.replace('T', ' ')
  
  # sequence
  if incremental and incremental_type == 'sequence':
    max_sequence = record["MAX_SEQUENCE"]
    cutoff_value = max_sequence  
  
  return cutoff_value

def get_default_config_record(partition_key, row_key):
  return {
        'PartitionKey': partition_key,
        'RowKey': row_key,
        'MAX_DATETIME': '1900-01-01T00:00:00Z',
        'MAX_DATETIME@odata.type': 'Edm.DateTime',
        'RUN_DATETIME': '1900-01-01T00:00:00Z',
        'RUN_DATETIME@odata.type': 'Edm.DateTime',
        'MAX_SEQUENCE': 0,
        'MAX_SEQUENCE@odata.type': 'Edm.Int32'
    }

def get_run_datetime():
  return dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def get_table_config():
  storage_name = "edmans{0}config001".format(ENV_NAME)
  scope_name = "edm-ans-{0}-dbr-scope".format(ENV_NAME)
  sas_token =  dbutils.secrets.get(scope = scope_name, key = 'ls-azu-config-table-sas-key')
  return {'storage_name': storage_name, 'table_name': 'edmtimestamps', 'token': sas_token}

def get_timestamp_record(database_name, table_name, source_name = None):
  config = get_table_config()
  
  partition_key = database_name.lower()
  row_key = table_name if source_name is None else "{0} [{1}]".format(table_name, source_name)
  row_key = row_key.lower()
  
  data = {'PartitionKey': partition_key,'RowKey': row_key}
  record = get_config_record(config, data)
  
  if not record:
    default_record = get_default_config_record(partition_key, row_key)
    update_config_record(config, default_record)
    record = {**record, **default_record}
  return record

def update_cutoff_value(cutoff_value, source_table = None):
  
  test_run = params.get('test_run', False)    
  if test_run:
    print('TEST RUN')
    return
  
  config = get_table_config()
  
  database_name = params.get('database_name')
  table_name = params.get('table_name')
  incremental_type = params.get('incremental_type', 'timestamp')  
  
  partition_key = database_name.lower()
  row_key = table_name if source_table is None else "{0} [{1}]".format(table_name, source_table)
  row_key = row_key.lower()
  
  record = {}
  record = get_timestamp_record(database_name, table_name, source_table)
  
  data = {}
  data['PartitionKey'] = partition_key
  data['RowKey'] = row_key
  
  if cutoff_value is not None and incremental_type == "timestamp":
    data['MAX_DATETIME'] = cutoff_value.replace(' ', 'T')
    data['MAX_DATETIME@odata.type'] = 'Edm.DateTime'
  
  if cutoff_value is not None and incremental_type == "sequence":
    data['MAX_SEQUENCE'] = cutoff_value
    data['MAX_SEQUENCE@odata.type'] = 'Edm.Int32'
  
  update_config_record(config, data)
  
def update_run_datetime(run_datetime, database_name = None, table_name = None, source_table = None):
  
  test_run = params.get('test_run', False)    
  if test_run:
    print('TEST RUN')
    return
  
  config = get_table_config()
  
  if database_name is None:
    database_name = params.get('database_name')
    
  if table_name is None:
    table_name = params.get('table_name')
  
  partition_key = database_name.lower()
  row_key = table_name if source_table is None else "{0} [{1}]".format(table_name, source_table)
  row_key = row_key.lower()
  
  record = {}
  record = get_timestamp_record(database_name, table_name, source_table)
  
  data = {}
  data['PartitionKey'] = partition_key
  data['RowKey'] = row_key
  data['RUN_DATETIME'] = run_datetime
  data['RUN_DATETIME@odata.type']: 'Edm.DateTime'  
  
  update_config_record(config, data)

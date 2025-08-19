# Databricks notebook source
def check_datatype_mismatch(schema_1, schema_2):
  result = False
  
  fields_in_2_not_1 = set(schema_2) - set(schema_1)  
  if len(fields_in_2_not_1) > 0:  
    
      schema_d_1 = {}
      for f in schema_1.fields:
        schema_d_1[f.name] = f.dataType 
      
      fields_in_2_not_1_dict = {}
      for f in fields_in_2_not_1:
        fields_in_2_not_1_dict[f.name] = f.dataType
      
      for k, v in fields_in_2_not_1_dict.items():
        if k in schema_d_1.keys() and schema_d_1[k] != v:
          result = True        

  return result

def move_bad_data(good_schema, root_path, file_paths, bad_data_folder):

  for file_path in file_paths:
    schema = spark.read.format('parquet').load(file_path).schema
    
    # deviceId issue    
    if str(schema["deviceId"].dataType) == 'IntegerType':
      print(f'removing file with deviceId int: {file_path}')
      dbutils.fs.rm(file_path)
      continue
    
    # schema issue
    if check_datatype_mismatch(good_schema, schema):
      file_name = os.path.basename(file_path)
      
      bad_data_path = file_path.replace(root_path, bad_data_folder)
      print(f'schema mismatch moving to: {bad_data_path}')
      dbutils.fs.mv(file_path, bad_data_path, True)

# COMMAND ----------

def t_add_missing_columns(columns):
  def inner(df):
    missing_columns = set(columns).difference(df.columns)
    for col in missing_columns:
      print(f'adding missing {col} column')
      df = df.withColumn(col, f.lit(None))
    return df
  return inner

def t_cast_column_type(column, datatype):
  def inner(df):
    if column in df.columns:
      df = df.withColumn(column, f.col(column).cast(datatype).alias(column))
    return df
  return inner

def t_cast_types():
  def inner(df):    
    return (
      df
      .transform(t_cast_column_type('dt', 'date'))
      .transform(t_cast_column_type('durationSecs', 'integer'))
      .transform(t_cast_column_type('durationSecs1', 'integer'))
      .transform(t_cast_column_type('fle_ext_count_fast', 'integer'))
      .transform(t_cast_column_type('fle_ext_count_slow', 'integer'))
      .transform(t_cast_column_type('fle_ext_dur', 'integer'))
      .transform(t_cast_column_type('haptic_violation_count', 'integer'))
      .transform(t_cast_column_type('idle_dur', 'integer'))
      .transform(t_cast_column_type('payloadCountFinal', 'integer'))
      .transform(t_cast_column_type('payloadCountInit', 'integer'))
      .transform(t_cast_column_type('rad_uln_count_slow', 'integer'))
      .transform(t_cast_column_type('rad_uln_count_fast', 'integer'))         
      .transform(t_cast_column_type('rad_uln_dur', 'integer'))
      .transform(t_cast_column_type('rollAngle', 'integer'))
      .transform(t_cast_column_type('sup_pro_count_slow', 'integer'))
      .transform(t_cast_column_type('sup_pro_count_fast', 'integer'))
      .transform(t_cast_column_type('sup_pro_dur', 'integer'))
      .transform(t_cast_column_type('thumbsUpHaptic', 'double'))
      .transform(t_cast_column_type('thumbsUpHaptic_count', 'long'))
      .transform(t_cast_column_type('thumbsUpThreshold', 'double'))
      .transform(t_cast_column_type('thumbsUpVibration', 'integer'))
      .transform(t_cast_column_type('thumbsUpWindowSize', 'double'))
      .transform(t_cast_column_type('timedHaptic', 'double'))
      .transform(t_cast_column_type('timedHaptic_count', 'long'))
      .transform(t_cast_column_type('timedHapticWindowSize', 'double'))
      .transform(t_cast_column_type('timeStampStart', 'timestamp'))
      .transform(t_cast_column_type('timeStampEnd', 'timestamp'))
      .transform(t_cast_column_type('tot_dur', 'integer'))
      .transform(t_cast_column_type('walk_dur', 'integer'))  
      .withColumn('localTImeStampEnd', f.to_timestamp(f.col('localTImeStampEnd'), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias('localTImeStampEnd'))
      .withColumn('localTImeStampStart', f.to_timestamp(f.col('localTImeStampStart'), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias('localTImeStampStart'))
    )
  return inner

def t_unify_data_types():
  def inner(df):
    return (
      df
      .transform(t_cast_column_type('rollAngle', 'string'))
      .transform(t_cast_column_type('thumbsUpThreshold', 'string'))
      .transform(t_cast_column_type('thumbsUpVibration', 'string'))
      .transform(t_cast_column_type('thumbsUpWindowSize', 'string'))
    )
  return inner

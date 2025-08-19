# Databricks notebook source
def alter_hive_table_schema(table_name, schema):
  
  location = get_hive_table_location(table_name)
  
  spark.table(table_name).write.format('delta').mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(table_name + '_tmp')
    
  df = spark.table(table_name + '_tmp')
  df2 = df.transform(apply_schema(schema))  
  df2.write.format('delta').mode("overwrite") \
    .option("overwriteSchema", "true").option("path", location) \
    .saveAsTable(table_name)
  
  spark.sql("DROP TABLE {0}".format(table_name + '_tmp'))
  spark.sql("REFRESH TABLE {0}".format(table_name)) 

def export_table_schema(table_name):
  schema_dict = get_hive_table_schema(table_name)
  table_path = get_hive_table_location(table_name)
  folder_path = table_path.split('core.windows.net')[1]

  data_source_no = table_path.split('@')[1].split('.')[0][-3:]
  data_source = f'datalake{data_source_no}'

  return json.dumps({
    'schema': schema_dict,
    'table_path': table_path,
    'data_source': data_source,
    'folder_path': folder_path
  })  
  
def get_dataset_schema(table_name, source_name = None, debug = False):
  database_name = table_name.split('.')[0].lower()
  table_name = table_name.split('.')[1].lower()
  
  lst = [database_name, table_name]
  if source_name is not None:
    lst.append(source_name)
  
  file_name = ".".join(lst)  
  file_path = "/notebooks/_SCHEMA/{0}".format(file_name)
  
  if debug:
    print("LOADING SCHEMA DEFINITION FROM " + file_path)
  
  schema = dbutils.notebook.run(file_path, 60)
  schema = schema.replace("'", "\"")
  return json.loads(schema)
  
  schema = dbutils.notebook.run(file_path, 60)
  schema = schema.replace("'", "\"")
  return json.loads(schema)
  
def is_table_schema_matching(table_name, schema): 
  table = spark.table(table_name)
  df = table.transform(apply_schema(schema))
  return (table.schema == df.schema)

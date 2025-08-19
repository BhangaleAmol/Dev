# Databricks notebook source
import json, os

# COMMAND ----------

# EXPORT TABLE META
schema_dict = get_table_schema(table_name)
table_path = get_table_location(table_name)
folder_path = table_path.split('core.windows.net')[1]

data_source_no = table_path.split('@')[1].split('.')[0][-3:]
data_source = f'datalake{data_source_no}'

result = json.dumps({
  'schema': schema_dict,
  'table_path': table_path,
  'data_source': data_source,
  'folder_path': folder_path
})

endpoint_name = get_endpoint_name(metadata_container, metadata_storage)
file_path = f'{endpoint_name}{metadata_folder}/{table_name}.json'.lower()
dbutils.fs.put(file_path, result, True)
print(f'metadata saved to: {file_path}')

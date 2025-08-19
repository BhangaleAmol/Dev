# Databricks notebook source
import json

# COMMAND ----------

def export_table_schema(table_name):
  schema_dict = get_table_schema(table_name)
  table_path = get_table_location(table_name)
  folder_path = table_path.split('core.windows.net')[1]

  data_source_no = table_path.split('@')[1].split('.')[0][-3:]
  data_source = f'datalake{data_source_no}'

  return json.dumps({
    'schema': schema_dict,
    'table_path': table_path,
    'data_source': data_source,
    'folder_path': folder_path
  })

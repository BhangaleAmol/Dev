# Databricks notebook source
import re

def get_endpoint_name(container_name = 'datalake', storage_name = 'edmans{env}data001'):
  env_name = os.getenv('ENV_NAME')
  storage_name = storage_name.replace('{env}', env_name)  
  return f'abfss://{container_name}@{storage_name}.dfs.core.windows.net' 

def get_latest_file_name(source_folder, file_name, options = {}):
  
  container_name = options.get('container_name', 'datalake')
  storage_name = options.get('storage_name', 'edmans{env}data001')
  endpoint_name = get_endpoint_name(container_name, storage_name)
  
  folder_path = f'{endpoint_name}{source_folder}'
  file_prefix = file_name.split('*')[0]
  data_files = dbutils.fs.ls(folder_path)
  
  timestamps = []

  for f in data_files:
    if file_prefix in f[1]:
      timestamp = re.findall(r'\d{14}', f[1])   
      timestamps.append(timestamp)
  
  if not timestamps:
    return False
  
  max_timestamp = max(timestamps)[0]
  return file_name.replace('*', f'_{max_timestamp}')   

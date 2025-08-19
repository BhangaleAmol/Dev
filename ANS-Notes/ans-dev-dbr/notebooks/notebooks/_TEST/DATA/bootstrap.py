# Databricks notebook source
import unittest, json
from parameterized import parameterized

# COMMAND ----------

def get_databases():  
  return [row.databaseName for row in spark.sql('SHOW DATABASES').collect()]

def get_tables(database_name):
  spark.sql(f'USE {database_name}')
  data = spark.sql('SHOW TABLES').select('tableName').collect()
  return [database_name + '.' + row.tableName for row in data if row.tableName[-5:] != 'delta'] 

def get_test_has_fk_record_args(table_details, table_name, key):
  source_names = table_details[table_name]['sources']
  return [(f"{t[0].split('.')[1]}_{s}".lower(), t[0], s) 
          for s in source_names for t in table_details.items() if key in t[1]['fk']]

def get_test_has_no_nk_duplicates_args(table_details):
  result = []
  for t in table_details.items():  
    if 'nk' not in t[1]:
      continue    
    for s in t[1]['sources']:
      result.append((f"{t[0].split('.')[1]}_{s}".lower(), t[0], t[1]['nk'], s))
  return result

def get_test_fk_is_not_null_args(table_details):
  result = []
  for t in table_details.items():  
    if 'fk' not in t[1]:
      continue    
    for s in t[1]['sources']:
      result.append((f"{t[0].split('.')[1]}_{s}".lower(), t[0], t[1]['fk'], s))
  return result

def get_views(database_name):
  spark.sql(f'USE {database_name}')
  data = spark.sql('SHOW VIEWS').select('viewName').collect()
  return [database_name + '.' + row.viewName for row in data]

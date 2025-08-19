# Databricks notebook source
import requests, os

# COMMAND ----------

def create_work_item(title, type):
  code = dbutils.secrets.get(scope = 'edm-scope', key = 'ls-azu-func-devops-key')
  url = f'https://edm-ans-devops.azurewebsites.net/api/create_work_item?code={code}'
  
  data = {
    "itemtitle": title,
    "itemtype": type
  }
  response = requests.post(url, json = data)
  print(response.content)

def create_work_items(results):
  
  envName = os.getenv('ENV_NAME')  
  if envName != 'prod':   
    return  
  
  for database, tests in results.items():
    for test in tests:
      test = test.split(' ')[0]      
      title = f"BUG: [{database}] {test}"
      type = 'bug'
      print(f'Creating work item ({title})')
      create_work_item(title, type)

# COMMAND ----------



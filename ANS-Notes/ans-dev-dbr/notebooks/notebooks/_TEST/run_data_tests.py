# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

import json

# COMMAND ----------

# PARAMETERS
work_item = get_input_param('work_item', 'bool', False)
database_name = get_input_param('database_name', 'list', ['all'])
send_notification = get_input_param('send_notification', 'bool', False)
throw_exception = get_input_param('throw_exception', 'bool', False)

# COMMAND ----------

if database_name == ['all']:
  database_name = [
    'g_apac_sup', 'g_call_emea', 'g_call_us','g_emea_cs', 'g_fin_qv', 'g_gpi', 
    'g_gsc_sup', 'g_lac', 'g_na_sm', 'g_na_cs', 'g_na_tm','g_quality', 
    's_callsystems', 's_core', 's_manufacturing', 's_sales', 's_service', 
    's_supplychain', 's_trademanagement', 'pdh','ebs'
  ]

# COMMAND ----------

# RUN TESTS
total_result = {}
hive_databases = get_databases()
total_failures = 0
for database in database_name:
  if database in hive_databases:
    result = json.loads(dbutils.notebook.run(f"./DATA/test_{database}", 3 * 3600))
    print("Database {0}, tests failed: {1}".format(database, len(result['failures'])))
    total_result[database] = result['failures']
    
    if not result['successfull']:
      total_failures = total_failures + len(result['failures'])

# COMMAND ----------

# SHOW FAILURES
for database, tests in total_result.items():
  for test in tests:
    print(f'{database} {test}')

# COMMAND ----------

# CREATE WORK ITEM
if total_failures > 0 and work_item:
  create_work_items(total_result)

# COMMAND ----------

# SEND NOTIFICATION
if total_failures > 0 and send_notification:
  send_mail_data_quality_test_failure(total_result)

# COMMAND ----------

# THROW EXCEPTION
if total_failures > 0 and throw_exception:
  raise Exception(f"{total_failures} test(s) failed.")  

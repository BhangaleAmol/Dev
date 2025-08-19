# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

import json

# COMMAND ----------

send_notification = get_input_param('send_notification', 'bool', False)
throw_exception = get_input_param('throw_exception', 'bool', False)

# COMMAND ----------

total_failures = 0
result = json.loads(dbutils.notebook.run("./FUNCTIONS/test_func_transformation", 3600))
print("tests failed: {0}".format(len(result['failures'])))
if not result['successfull']:
  total_failures = total_failures + len(result['failures'])
  print(result['failures'])

# COMMAND ----------

# SEND NOTIFICATION
if total_failures > 0 and send_notification:
  send_mail_unit_test_failure(result['failures'])

# COMMAND ----------

# THROW EXCEPTION
if total_failures > 0 and throw_exception:
  raise Exception(f"{total_failures} test(s) failed.")  

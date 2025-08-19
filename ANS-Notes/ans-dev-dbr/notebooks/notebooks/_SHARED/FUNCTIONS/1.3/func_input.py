# Databricks notebook source
import json

# COMMAND ----------

def clean_input_param(field_value, field_type = 'string', default_value = None):  
  
  if field_value is None or field_value == '':
    field_value = default_value
  
  if (type(field_value).__name__ == "str"):
    field_value = field_value.strip()
  
  if field_value is not None:
    field_value = _cast_input_type(field_value, field_type)
  
  return field_value

def get_input_param(field_name, field_type = 'string', default_value = None, json_field = 'config'): 
  dbutils.widgets.text(json_field, "","") 
  json_value = dbutils.widgets.get(json_field)
   
  if json_value is None or json_value == '':
    field_value = default_value
  else:
    dict_value = json.loads(json_value)
    field_value = dict_value.get(field_name)
  
  field_value = clean_input_param(field_value, field_type, default_value)
  _print_input_param(field_name, field_value)
  return field_value

# COMMAND ----------

def _cast_input_type(value, data_type = "string"):  
  if value == '' or value is None:
    return None
  
  value = {
    'string': lambda x: str(x), 
    'int': lambda x: int(x),
    'bool': lambda x: str(x).lower() in ("yes", "true", "1"),
    'list': lambda x: list(x)
  }[data_type](value)
    
  return value

def _print_input_param(field_name, field_value):
  field_type = type(field_value).__name__
  if field_type == 'str':
    message = '{0} = "{1}" # [{2}]'.format(field_name, str(field_value), field_type)
  else:
    message = '{0} = {1} # [{2}]'.format(field_name, str(field_value), field_type)
  print(message)

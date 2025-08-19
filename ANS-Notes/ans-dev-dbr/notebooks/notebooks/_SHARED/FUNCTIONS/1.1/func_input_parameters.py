# Databricks notebook source
def cast_input_type(value, data_type = "string"):  
  if value == '' or value is None:
    return None
  
  value = {
    'string': lambda x: str(x), 
    'int': lambda x: int(x),
    'bool': lambda x: str(x).lower() in ("yes", "true", "1")
  }[data_type](value)
    
  return value

def format_default_params(input_dict):
  
  data_types = {
    "incremental": "bool", 
    "overwrite": "bool", 
    "prune_days": "int",
    "sampling": "bool",
    "test_run": "bool"
  }
  
  default_values = {
    "cutoff_value": "1900-01-01T00:00:00", 
    "incremental": False, 
    "overwrite": False, 
    "prune_days": 30,
    "sampling": False,
    "test_run": False, 
  }
  
  input_dict['run_datetime'] = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  
  for field_name, field_value in input_dict.items():
    if field_name in data_types.keys():
      input_dict[field_name] = cast_input_type(field_value, data_types[field_name])

    if field_value is None:
      if field_name in default_values.keys():
        input_dict[field_name] = default_values[field_name]

def get_input_params(field_list):  
  
  result_dict = {}
  for field_name in field_list:  
    dbutils.widgets.text(field_name, "","") 
    field_value = dbutils.widgets.get(field_name).strip()    
    result_dict[field_name] = None if field_value == '' else field_value 
    
  return result_dict

def get_input_param_json(field_name):
  
  dbutils.widgets.text(field_name, "","")
  field_value_json = dbutils.widgets.get(field_name).strip()
   
  if field_value_json == '':
    return {}
  
  field_value_dict = json.loads(field_value_json)    
  result_dict = {}
  for key, value in field_value_dict.items():
    value = value.strip() if (type(value).__name__ == "str") else value
    if value == '': value = None
    result_dict[key.lower()] = value
      
  return result_dict

def get_param(params, param_name, param_type = None, default_value = None):
  param_value = params.get(param_name)
  
  if param_value is None: 
    params[param_name] = default_value
    
  if param_type is not None:
    params[param_name] = cast_input_type(params[param_name], param_type)  
  
  return params[param_name]

def print_dict(input_dict):
  if input_dict is None:
    return None  
  for name, value in input_dict.items():
    print_input(name, value)

def print_input(input_name, param_value, param_name = None):  
  param_name = input_name if param_name is None else param_name
  param_type = type(param_value).__name__
  param_value = str(param_value)
  
  if param_type in ['bool', 'int', 'NoneType']:    
    message = '{0} = set_param(params, "{1}", {2}) # [{3}]'.format(param_name, input_name, param_value, param_type)
  else:
    message = '{0} = set_param(params, "{1}", "{2}") # [{3}]'.format(param_name, input_name, param_value, param_type)
    
  print(message)
  
def set_param(params, param_name, param_value):  
  params[param_name] = param_value
  return params[param_name]

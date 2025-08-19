# Databricks notebook source
import json
import requests

# COMMAND ----------

def get_config_record(config, data):  
    data_str = json.dumps(data)
    data_obj = json.loads(data_str)
    partition_key = data_obj["PartitionKey"].lower()
    row_key = data_obj["RowKey"].lower()

    header = {'Content-type':'application/json', 'Accept':'application/json'} 
    table_url = "https://{0}.table.core.windows.net/{1}(PartitionKey='{2}',RowKey='{3}'){4}".format(
      config['storage_name'], config['table_name'], partition_key, row_key, config['token'])
    
    attempts = 5
    for count in range(attempts):
                try:
                    response = requests.get(table_url, headers=header,timeout=300)
                    response_obj = json.loads(response.text)
                except Exception as e:
                    if count == (attempts - 1):
                        raise(e)
                    time.sleep(60)
                    continue
                else:
                    break

    if 'odata.error' in response_obj:
      return {}
    
    if(response.status_code == 403):
      raise Exception(response.text)

    return json.loads(response.text)

def update_config_record(config, data): 

    record = get_config_record(config, data)      
    partition_key = data["PartitionKey"]
    row_key = data["RowKey"]

    data_str = json.dumps(data)  
    data_obj = json.loads(data_str) 
    new_record = json.dumps({**record, **data_obj})

    header = {'Content-type':'application/json', 'Accept':'application/json'} 
    table_url = "https://{0}.table.core.windows.net/{1}(PartitionKey='{2}',RowKey='{3}'){4}".format(
      config['storage_name'], config['table_name'], partition_key, row_key, config['token'])

    try:
        response = requests.put(table_url, headers=header, data=new_record,timeout=300)
        if(response.status_code==403):
          raise Exception(response.text)
    except  Exception as e:
          Print('Timeout after 300 seconds')
          notebook_data = {
            'notebook_name': NOTEBOOK_NAME,
          }
          send_mail_update_config_file_fail(notebook_data)      

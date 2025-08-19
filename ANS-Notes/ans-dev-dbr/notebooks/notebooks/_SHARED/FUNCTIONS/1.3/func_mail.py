# Databricks notebook source
import os, requests

# COMMAND ----------

def send_mail_data_quality_test_failure(results):
  
  message = '<table><tr><th>database</th><th>test</th></tr>'  
  test_number = 0
  for database, tests in results.items():
    test_number = test_number + len(tests)
    for test in tests:
      message += f"<tr><td>{database}</td><td>{test}</td></tr>"
  message += '</table>'
  
  title = f"[{ENV_NAME.upper()}] Found ({test_number}) data quality test failures"  
  
  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url)
  
def send_mail_duplicate_records_found(results):  
  title = f"[{ENV_NAME.upper()}] {results['source_name']} - ({results['notebook_name']}) - Found ({results['duplicates_count']}) duplicate records"  
  message = f"notebook path: {results['notebook_path']}<br>"
  message += f"target name: {results['target_name']}<br><br>"
  message += 'keys for first 50 duplicates:<br><br>'  
  message += results['duplicates_sample'].toPandas().to_html()

  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url)

def send_mail_pdh_25k_records_found(results):
  title = f"[{ENV_NAME.upper()}] Found 25k records in PDH source files"  
  message = "\r\n".join(results)
  
  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url)
  
def send_mail_unit_test_failure(failures): 
  title = f"[{ENV_NAME.upper()}] Found ({len(failures)}) unit test failures"  
  message = "\r\n".join(failures)
  
  url = get_secret("la-edm-ans-send-mail") 
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url) 
  
def send_mail_update_config_file_fail(results):  
  title = f"[{ENV_NAME.upper()}] Update Config Fail - ({results['notebook_name']})" 

  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": title
  }  
  send_mail(data, url)
  
def send_mail_row_count_mismatch(results):  
  title = f"[{ENV_NAME.upper()}] Row count mismatch for {results['target_name']} table" 
  
  message = f"table keys count: {results['table_keys_count']}<br>"
  message += f"full keys count: {results['full_keys_count']}"
    
  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url)  
  
def send_mail(data, url):
  response = requests.post(url, json = data)

# COMMAND ----------



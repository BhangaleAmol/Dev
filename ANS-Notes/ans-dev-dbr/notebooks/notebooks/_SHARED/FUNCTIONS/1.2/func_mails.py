# Databricks notebook source
def mail_gpd_records_not_matching(site, sp_html, db_html, list_name):
     
  title = "GPD - {0} Pipeline Error - Records not matching".format(site)
  site_url = "https://ansellhealthcare.sharepoint.com/sites/Team-GlobalProduction/Lists/{0}/AllItems.aspx".format(list_name)
   
  message = """
    <p><b>No match for Sharepoint records</b></p>
    <p>{0}</p>
    <p>Sharepoint</p>
    {1}
    <p>Database</p>
    {2}
  """.format(site_url, sp_html, db_html)
  
  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edm_gpd_notifications@ansell.com", 
    "title": title, 
    "message": message
  }  
  send_mail(data, url) 

def mail_gpd_records_not_updated(site, max_date, list_name):
  
  title = "GPD - {0} Pipeline Error - SharePoint records not updated".format(site)
  site_url = "https://ansellhealthcare.sharepoint.com/sites/Team-GlobalProduction/Lists/{0}/AllItems.aspx".format(list_name)
  
  message = """
    <p><b>Latest record date: {0}</b></p>
    <p>{1}</p>
  """.format(max_date, site_url)
  
  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edm_gpd_notifications@ansell.com", 
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
  
def send_mail_update_config_file_fail(results):  
  title = f"[{ENV_NAME.upper()}] Update Config Fail - ({results['notebook_name']})" 

  url = get_secret("la-edm-ans-send-mail")  
  data = {
    "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms", 
    "title": title, 
    "message": title
  }  
  send_mail(data, url)

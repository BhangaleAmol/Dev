# Databricks notebook source
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.mgmt.datafactory.operations import *

from datetime import datetime, timedelta, timezone
import time

# COMMAND ----------

def get_adf_client():
  credentials = ClientSecretCredential(TENANT_ID, SVC_EDM_DBR_ID, SVC_EDM_DBR_SECRET)
  return DataFactoryManagementClient(credentials, SUBCRIPTION_ID)

def start_adf_pipeline(adf_client, pipeline_name, parameters={}, options={}):
  
  check_status = options.get('check_status', False)

  response = adf_client.pipelines.create_run(
    RESOURCE_GROUP, 
    DATA_FACTORY,
    pipeline_name,
    parameters=parameters)
  
  if check_status:
    time.sleep(60)
    if not _pipeline_succeeded(adf_client, response.run_id):
      raise Exception(pipeline_name + " failed.")
  
  return response.run_id

# COMMAND ----------

def _pipeline_succeeded(adf_client, run_id):     
    
  while True:

    adf_client.pipeline_runs.get(RESOURCE_GROUP, DATA_FACTORY, run_id)

    filter_params = RunFilterParameters(
      last_updated_after = datetime.now(timezone.utc) - timedelta(1), 
      last_updated_before = datetime.now(timezone.utc) + timedelta(1))

    query_response = adf_client.activity_runs.query_by_pipeline_run(
      RESOURCE_GROUP, 
      DATA_FACTORY, 
      run_id, 
      filter_params)

    status = [action.status for action in query_response.value]  
    if any(s == "InProgress" for s in status):
      time.sleep(60)
    else:      
      if all(s == "Succeeded" for s in status):
        return True
      else:
        return False

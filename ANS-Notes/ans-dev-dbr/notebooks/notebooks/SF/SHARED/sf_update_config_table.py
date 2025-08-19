# Databricks notebook source
import os
envName = os.getenv('ENV_NAME')
secretScope = "edm-ans-{0}-dbr-scope".format(envName)
sasToken = dbutils.secrets.get(scope = secretScope, key = "ls-azu-datalake-table-sas-key")

# COMMAND ----------

dbutils.widgets.text("table_row", "","")
dbutils.widgets.get("table_row")
tableRow = getArgument("table_row")

# COMMAND ----------

print(tableRow)

# COMMAND ----------

import json
from datetime import datetime

tableRowJson = json.loads(tableRow)

dateTimeObj = datetime.now()
dateTimeStr = dateTimeObj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

tableRowJson["FULL_LAST_EXTRACT_DATE@odata.type"] = "Edm.DateTime"
tableRowJson["INCR_LAST_EXTRACT_DATE@odata.type"] = "Edm.DateTime"

if tableRowJson["INCREMENTAL"] == True:
  fullDateTimeObj = datetime.strptime(tableRowJson["FULL_LAST_EXTRACT_DATE"], "%Y-%m-%dT%H:%M:%S%z")
  fullDateTimeStr = fullDateTimeObj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
  tableRowJson["FULL_LAST_EXTRACT_DATE"] = fullDateTimeStr
  tableRowJson["INCR_LAST_EXTRACT_DATE"] = dateTimeStr
else:
  tableRowJson["FULL_LAST_EXTRACT_DATE"] = dateTimeStr
  tableRowJson["INCR_LAST_EXTRACT_DATE"] = dateTimeStr

tableRowStr = json.dumps(tableRowJson)

# COMMAND ----------

import requests

storageName = "edmans{0}data001".format(envName)
headerContent = {'Content-type':'application/json', 'Accept':'application/json'}

partitionKey = tableRowJson["PartitionKey"]
rowKey = tableRowJson["RowKey"]
tableUrl = "https://" + storageName + ".table.core.windows.net/salesforce" + "(PartitionKey='" + partitionKey + "',RowKey='" + rowKey + "')" + sasToken

response = requests.put(tableUrl, headers=headerContent, data=tableRowStr)
if(response.status_code==403):
  raise Exception(response.text)

print(response.text)

# Databricks notebook source
import os

ENV_NAME = os.getenv('ENV_NAME')
SCOPE_NAME = "edm-ans-{0}-dbr-scope".format(ENV_NAME)
STORAGE_NAME = "edmans{0}data001".format(ENV_NAME)
DATALAKE_ENDPOINT = 'abfss://datalake@edmans{0}data001.dfs.core.windows.net'.format(ENV_NAME)
CFG_STORAGE_NAME = "edmans{0}config001".format(ENV_NAME)
CFG_TABLE_NAME = 'edmtimestamps'
TENANT_ID = 'e49ea3fe-87f8-44df-a7ba-2131f4fb91f1'
SVC_EDM_DBR_ID = dbutils.secrets.get(scope = SCOPE_NAME, key = "svc-edm-dbr-id")
SVC_EDM_DBR_SECRET = dbutils.secrets.get(scope = SCOPE_NAME, key = "svc-edm-dbr-secret")
SUBCRIPTION_ID = 'a64abb5d-8b1a-45bf-9eca-7096de5d4fac'
RESOURCE_GROUP = 'edm-ans-{}-rg'.format(ENV_NAME)
DATA_FACTORY = 'edm-ans-{}-df'.format(ENV_NAME)
NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
NOTEBOOK_NAME = os.path.basename(NOTEBOOK_PATH)

# COMMAND ----------

print('###GLOBAL VARIABLES###')
print(f'ENV_NAME: {ENV_NAME}')
print(f'STORAGE_NAME: {STORAGE_NAME}')
print(f'CFG_STORAGE_NAME: {CFG_STORAGE_NAME}')
# print(f'DATALAKE_ENDPOINT: {DATALAKE_ENDPOINT}')
print(f'RESOURCE_GROUP: {RESOURCE_GROUP}')
print(f'DATA_FACTORY: {DATA_FACTORY}')
print(f'NOTEBOOK_PATH: {NOTEBOOK_PATH}')
print(f'NOTEBOOK_NAME: {NOTEBOOK_NAME}')

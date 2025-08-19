# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

# LOAD LIBRARIES

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.mgmt.datafactory.operations import *


# COMMAND ----------

# MAGIC %run ./class_adf_controller

# COMMAND ----------

adf_controller = AdfController()
adf_controller.set_credentials(TENANT_ID, SVC_EDM_DBR_ID, SVC_EDM_DBR_SECRET)
adf_controller.set_client(SUBCRIPTION_ID, RESOURCE_GROUP, DATA_FACTORY)

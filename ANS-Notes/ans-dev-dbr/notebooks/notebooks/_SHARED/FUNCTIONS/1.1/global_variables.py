# Databricks notebook source
ENV_NAME = os.getenv('ENV_NAME')
STORAGE_NAME = "edmans{0}data001".format(ENV_NAME)
SCOPE_NAME = "edm-ans-{0}-dbr-scope".format(ENV_NAME)
DATALAKE_ENDPOINT = 'abfss://datalake@edmans{0}data001.dfs.core.windows.net'.format(ENV_NAME)

# Databricks notebook source
config = [
{
    'PartitionKey': '1',
    'RowKey': '1',
    'ACTIVE': True,
    'HANDLE_DELETE': False,
    'INCREMENTAL': False,
    'KEY_COLUMNS': '_Row',
    'PUBLISH': True,
    'SCHEMA_NAME': 'dbo',
    'TABLE_NAME': 'Mfg_ITMMAST'
},
{
    'PartitionKey': '1',
    'RowKey': '2',
    'ACTIVE': True,
    'HANDLE_DELETE': False,
    'INCREMENTAL': False,
    'KEY_COLUMNS': '_Row',
    'PUBLISH': True,
    'SCHEMA_NAME': 'dbo',
    'TABLE_NAME': 'Mfg_SUBITEM'
},
{
   'PartitionKey': '1',
    'RowKey': '3',
    'ACTIVE': True,
    'HANDLE_DELETE': False,
    'INCREMENTAL': False,
    'KEY_COLUMNS': '_Row',
    'PUBLISH': True,
    'SCHEMA_NAME': 'dbo',
    'TABLE_NAME': 'Mfg_QTY'
},
{
    'PartitionKey': '1',
    'RowKey': '4',
    'ACTIVE': True,
    'HANDLE_DELETE': False,
    'INCREMENTAL': False,
    'KEY_COLUMNS': '_Row',
    'PUBLISH': True,
    'SCHEMA_NAME': 'dbo',
    'TABLE_NAME': 'Mfg_ORDMAST'
}
]

# COMMAND ----------



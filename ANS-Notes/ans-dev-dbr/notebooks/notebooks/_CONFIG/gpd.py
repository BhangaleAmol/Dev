# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'BKK_PRODUCTION_DATA',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'BKK_PRODUCTION_DATA'
}, 
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'BKKProductsList',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'BKK_PRODUCTS_LIST'
}, 
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'MELAKA_PRODUCTION_DATA',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'MLK_PRODUCTION_DATA'
}, 
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'XIAMEN_PRODUCTION_DATA',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'XIA_PRODUCTION_DATA'
}, 
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'XiamenProductsList',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'XIA_PRODUCTS_LIST'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'Modified',
	'KEY_COLUMNS': 'Id',
	'LIST_NAME': 'MelakaProductsList',
	'PARTITION_COLUMN': 'Created',
	'PRUNE_DAYS': 30,
	'PUBLISH': True,
	'TABLE_NAME': 'MLK_PRODUCTS_LIST'
}]


# COMMAND ----------



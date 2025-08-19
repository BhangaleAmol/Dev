# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'SM_SALES_ORDERS',
	'PUBLISH': True,
	'TABLE_NAME': 'SM_SALES_ORDERS'
},
  {
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'SM_SALES_SHIPPING',
	'PUBLISH': True,
	'TABLE_NAME': 'SM_SALES_SHIPPING'
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'SM_SALES_BACKORDERS',
	'PUBLISH': True,
	'TABLE_NAME': 'SM_SALES_BACKORDERS'
},
  {
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'SM_SALES_INVOICES',
	'PUBLISH': True,
	'TABLE_NAME': 'SM_SALES_INVOICES'
}
]

# COMMAND ----------



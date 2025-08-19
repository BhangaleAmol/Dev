# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_AGENT',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_AGENT'
},
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_COMPANY',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_COMPANY'
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_ENTRYPOINT',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_ENTRYPOINT'
},
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_QUEUE',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_QUEUE'
},
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_SITE',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_SITE'
},
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'DIM_TEAM',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_TEAM'
},
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'FACT_AGENT_ACTIVITY',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_AGENT_ACTIVITY'
},
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'FACT_CUSTOMER_SESSION',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_CUSTOMER_SESSION'
},
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'FACT_CUSTOMER_ACTIVITY',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_CUSTOMER_ACTIVITY'
},  
]

# COMMAND ----------



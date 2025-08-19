# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'CLEAN_AARS',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_AARS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'CLEAN_ASRS',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_ASRS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'CLEAN_CARS',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_CARS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'CLEAN_CSRS',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_CSRS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'CLEAN_SF_ACCOUNT',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_SF_ACCOUNT',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'CLEAN_SF_CONTACT',
	'PUBLISH': True,
	'TABLE_NAME': 'CLEAN_SF_CONTACT',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 3,
	'NOTEBOOK_NAME': 'COMPANY_BRIDGE',
	'PUBLISH': True,
	'TABLE_NAME': 'COMPANY_BRIDGE',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_AGENT',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_AGENT',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_COMPANY',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_COMPANY',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_ENTRYPOINT',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_ENTRYPOINT',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '11',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_QUEUE',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_QUEUE',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '12',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_SITE',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_SITE',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '13',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'DIM_TEAM',
	'PUBLISH': True,
	'TABLE_NAME': 'DIM_TEAM',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '14',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'FACT_AGENT_ACTIVITY',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_AGENT_ACTIVITY',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '15',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'FACT_CUSTOMER_SESSION',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_CUSTOMER_SESSION',
    'REGISTER': True
},
{
	'PartitionKey': '1',
	'RowKey': '16',
	'ACTIVE': True,
	'NOTEBOOK_LEVEL': 4,
	'NOTEBOOK_NAME': 'FACT_CUSTOMER_ACTIVITY',
	'PUBLISH': True,
	'TABLE_NAME': 'FACT_CUSTOMER_ACTIVITY',
    'REGISTER': True
}, 
]

# COMMAND ----------



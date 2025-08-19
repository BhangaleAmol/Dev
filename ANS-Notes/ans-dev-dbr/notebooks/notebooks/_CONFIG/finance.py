# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'GL_SEGMENTS_EBS',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'GL_SEGMENTS_EBS'
},
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'GL_ACCOUNTS_EBS',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'GL_ACCOUNTS_EBS'
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'AP_TRANSACTIONS_EBS',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'AP_TRANSACTIONS_EBS'
},
{
   'PartitionKey': '1',
   'RowKey': '4',
   'ACTIVE': True,
   'INCREMENTAL': True,
   'NOTEBOOK_LEVEL': 1,
   'NOTEBOOK_NAME': 'GL_TRANSACTIONS_EBS',
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'TABLE_NAME': 'GL_TRANSACTIONS_EBS'
}, 
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'GL_SEGMENTS_AGG',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'GL_SEGMENTS_AGG'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'GL_ACCOUNTS_AGG',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'GL_ACCOUNTS_AGG'
}, 
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'GL_TRANSACTIONS_AGG',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'GL_TRANSACTIONS_AGG'
}, 
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'AP_TRANSACTIONS_AGG',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'AP_TRANSACTIONS_AGG'
}, 
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'AR_TRANSACTIONS_EBS',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'AR_TRANSACTIONS_EBS'
}, 
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': True,
	'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 2,
	'NOTEBOOK_NAME': 'AR_TRANSACTIONS_AGG',
	'PRUNE_DAYS': 10,
	'PUBLISH': True,
	'TABLE_NAME': 'AR_TRANSACTIONS_AGG'
}]

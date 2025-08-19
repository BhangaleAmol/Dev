# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'runno',
	'INCREMENTAL_TYPE': 'sequence',
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_workorder_dt'
}, 
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_workorder_hd'
}, 
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_fpxreject'
}, 
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'runno',
	'INCREMENTAL_TYPE': 'sequence',
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_postprocessing_pp01_pp05'
}, 
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'runno',
	'INCREMENTAL_TYPE': 'sequence',
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_qa_hd'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_productmaster'
}, 
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
	'HANDLE_DELETE': False,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'runno',
	'INCREMENTAL_TYPE': 'sequence',
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_qa_dt'
}, 
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_pk_et_dt'
}, 
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
	'HANDLE_DELETE': False,
	'INCREMENTAL': True,
	'INCREMENTAL_COLUMN': 'runno',
	'INCREMENTAL_TYPE': 'sequence',
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_pk_isp_dt'
}, 
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_pk_et_hd'
}, 
{
	'PartitionKey': '1',
	'RowKey': '11',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_defect'
}, 
{
	'PartitionKey': '1',
	'RowKey': '12',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_supermarket_hd'
}, 
{
	'PartitionKey': '1',
	'RowKey': '13',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_supermarket_dt'
}, 
{
	'PartitionKey': '1',
	'RowKey': '14',
	'ACTIVE': True,
	'HANDLE_DELETE': True,
	'INCREMENTAL': False,
	'KEY_COLUMNS': 'runno',
	'PUBLISH': True,
	'SERVER': 'MLKSQL5',
	'TABLE_NAME': 'ans_pk_isp_hd'
}]


# COMMAND ----------



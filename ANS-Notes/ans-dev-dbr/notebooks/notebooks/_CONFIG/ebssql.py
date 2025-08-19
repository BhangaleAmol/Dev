# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_USER_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_CUSTOMER_ACCOUNT_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_CUSTOMER_LOC_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_EXCH_RATE_G'
}, 
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_FUND_CATEGORY_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_GL_ACCOUNT_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': False,
	'FACT': True,
	'TABLE_NAME': 'W_GL_OTHER_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_LEDGER_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_GL_SEGMENT_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': False,
	'FACT': True,
	'TABLE_NAME': 'W_INVENTORY_MONTHLY_BAL_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '11',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_XACT_SOURCE_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '12',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_INT_ORG_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '13',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_PRODUCT_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '14',
	'ACTIVE': False,
	'FACT': True,
	'TABLE_NAME': 'W_QV_DROP_SHIP_A'
}, 
{
	'PartitionKey': '1',
	'RowKey': '15',
	'ACTIVE': False,
	'FACT': True,
	'TABLE_NAME': 'W_TRADE_PROMOTION_ACCRUAL_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '16',
	'ACTIVE': True,
	'FACT': True,
	'TABLE_NAME': 'W_SALES_INVOICE_LINE_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '17',
	'ACTIVE': True,
	'FACT': True,
	'TABLE_NAME': 'W_SALES_ORDER_LINE_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '18',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_STATUS_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '19',
	'ACTIVE': False,
	'FACT': False,
	'TABLE_NAME': 'TIME_DIMENSION'
}, 
{
	'PartitionKey': '1',
	'RowKey': '20',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_XACT_TYPE_D'
}, 
{
	'PartitionKey': '1',
	'RowKey': '21',
	'ACTIVE': False,
	'FACT': False,
	'TABLE_NAME': 'W_STANDARD_COST_MONTHLY_G'
}, 
{
	'PartitionKey': '1',
	'RowKey': '22',
	'ACTIVE': False,
	'FACT': True,
	'TABLE_NAME': 'W_SALES_ORDER_CANCELLATIONS_F'
}, 
{
	'PartitionKey': '1',
	'RowKey': '23',
	'ACTIVE': True,
	'FACT': False,
	'TABLE_NAME': 'W_ALLOCATIONS_PER_ACCOUNT_A'
}]


# COMMAND ----------



# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_INTRANSIT_EXTRACT_FS',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_INTRANSIT_EXTRACT_FS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_INVENTORY_EXTRACT',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_INVENTORY_EXTRACT',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_INTRANSIT_DETAILS_F',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_INTRANSIT_DETAILS_F',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_PURCH_PRICE_VARIANCE_F',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_PURCH_PRICE_VARIANCE_F',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_INVENTORY_EXTRACT_F',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_INVENTORY_EXTRACT_F',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_12_MONTH_SALES',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_12_MONTH_SALES',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_12_MONTH_SALES_PAM',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_12_MONTH_SALES_PAM',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_INVENTORY',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_INVENTORY',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
    'INCREMENTAL': False,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_PO_DATES',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_PO_DATES',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_GROSS_CBO',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_GROSS_CBO',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '11',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_PWBI_AP_INVOICE_A',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_PWBI_AP_INVOICE_A',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '12',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_GLOBAL_INVENTORY_SHIP_A',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_GLOBAL_INVENTORY_SHIP_A',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '13',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'CUSTOMER_SHIPTO',
	'PUBLISH': True,
	'TABLE_NAME': 'CUSTOMER_SHIPTO',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '14',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_GLOBAL_INV_ORD_A',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_GLOBAL_INV_ORD_A',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '15',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_INVOICES_SHIPMENTS',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_INVOICES_SHIPMENTS',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '16',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_ORDER_DATA',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_ORDER_DATA',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '17',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_ORDER_DATA_FULL',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_ORDER_DATA_FULL',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '18',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'WC_QV_WIP_INVENTORY_A',
	'PUBLISH': True,
	'TABLE_NAME': 'WC_QV_WIP_INVENTORY_A',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '19',
	'ACTIVE': True,
    'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'ORDER_INTAKE',
	'PUBLISH': True,
	'TABLE_NAME': 'ORDER_INTAKE',
    'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '20',
	'ACTIVE': True,
  'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'TMB_INVENTORY',
	'PUBLISH': True,
	'TABLE_NAME': 'TMB_INVENTORY',
  'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '21',
	'ACTIVE': True,
  'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'TMB_OPEN_PO',
	'PUBLISH': True,
	'TABLE_NAME': 'TMB_OPEN_PO',
  'REGISTER': False
},
{
	'PartitionKey': '1',
	'RowKey': '22',
	'ACTIVE': True,
  'INCREMENTAL': True,
	'NOTEBOOK_LEVEL': 1,
	'NOTEBOOK_NAME': 'TMB_INTRANSIT',
	'PUBLISH': True,
	'TABLE_NAME': 'TMB_INTRANSIT',
  'REGISTER': False
}
]

# COMMAND ----------



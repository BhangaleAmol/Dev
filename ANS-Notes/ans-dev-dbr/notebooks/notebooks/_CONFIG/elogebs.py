# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,    
    'ENVIRONMENT': 'dev',
    'INCREMENTAL': False,
    'NOTEBOOK_NAME': 'LOGILITY_DROP_SHIP_UPDATE',
    'PRUNE_DAYS': '3',	
	'PUBLISH': True,
	'SOURCE_QUERY': "SELECT cast(EBS_SALES_ORDER as varchar(15)) EBS_SALES_ORDER, cast(SALES_ORDER_HEADER as varchar(15)) SALES_ORDER_HEADER, cast(SALES_ORDER_LINE as varchar(15)) SALES_ORDER_LINE, cast(LINE_ID as varchar(15)) LINE_ID, cast(ITEM_NUMBER as varchar(15)) ITEM_NUMBER, cast(FULFILMENT_DATE as date) FULFILMENT_DATE, cast(BATCH_ID as varchar(15)) BATCH_ID FROM g_logility.logility_drop_ship_ebs WHERE Processed = 'N'",
	'SOURCE_TABLE': 'g_logility.logility_drop_ship_ebs',
    'TABLE_NAME': 'XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',
    'TARGET_QUERY': 'select to_char(EBS_SALES_ORDER) EBS_SALES_ORDER ,to_char(Header_Id) as SALES_ORDER_HEADER,to_char(Sales_Order_Line) SALES_ORDER_LINE,to_char(Line_Id) LINE_ID,to_char(Item_Number) ITEM_NUMBER, to_date(Fulfilment_Date) FULFILMENT_DATE,to_char(Batch_Id) BATCH_ID from XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',   
    'TRIGGER_PROCESS': False,
    'TRIGGER_URL': 'http://uat-osb.ansell.com/ans/SO_LinesRelease_Service'
},
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': False,    
    'ENVIRONMENT': 'test',
    'INCREMENTAL': False,
    'NOTEBOOK_NAME': 'LOGILITY_DROP_SHIP_UPDATE',
    'PRUNE_DAYS': '3',	
	'PUBLISH': True,
	'SOURCE_QUERY': "SELECT cast(EBS_SALES_ORDER as varchar(15)) EBS_SALES_ORDER, cast(SALES_ORDER_HEADER as varchar(15)) SALES_ORDER_HEADER, cast(SALES_ORDER_LINE as varchar(15)) SALES_ORDER_LINE, cast(LINE_ID as varchar(15)) LINE_ID, cast(ITEM_NUMBER as varchar(15)) ITEM_NUMBER, cast(FULFILMENT_DATE as date) FULFILMENT_DATE, cast(BATCH_ID as varchar(15)) BATCH_ID FROM g_logility.logility_drop_ship_ebs WHERE Processed = 'N'",
	'SOURCE_TABLE': 'g_logility.logility_drop_ship_ebs',
    'TABLE_NAME': 'XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',
    'TARGET_QUERY': 'select to_char(EBS_SALES_ORDER) EBS_SALES_ORDER ,to_char(Header_Id) as SALES_ORDER_HEADER,to_char(Sales_Order_Line) SALES_ORDER_LINE,to_char(Line_Id) LINE_ID,to_char(Item_Number) ITEM_NUMBER, to_date(Fulfilment_Date) FULFILMENT_DATE,to_char(Batch_Id) BATCH_ID from XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',   
    'TRIGGER_PROCESS': False,
    'TRIGGER_URL': 'http://uat-osb.ansell.com/ans/SO_LinesRelease_Service'
},
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': False,    
    'ENVIRONMENT': 'prod',
    'INCREMENTAL': False,
    'NOTEBOOK_NAME': 'LOGILITY_DROP_SHIP_UPDATE',
    'PRUNE_DAYS': '3',	
	'PUBLISH': True,
	'SOURCE_QUERY': "SELECT cast(EBS_SALES_ORDER as varchar(15)) EBS_SALES_ORDER, cast(SALES_ORDER_HEADER as varchar(15)) SALES_ORDER_HEADER, cast(SALES_ORDER_LINE as varchar(15)) SALES_ORDER_LINE, cast(LINE_ID as varchar(15)) LINE_ID, cast(ITEM_NUMBER as varchar(15)) ITEM_NUMBER, cast(FULFILMENT_DATE as date) FULFILMENT_DATE, cast(BATCH_ID as varchar(15)) BATCH_ID FROM g_logility.logility_drop_ship_ebs WHERE Processed = 'N'",
	'SOURCE_TABLE': 'g_logility.logility_drop_ship_ebs',
    'TABLE_NAME': 'XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',
    'TARGET_QUERY': 'select to_char(EBS_SALES_ORDER) EBS_SALES_ORDER ,to_char(Header_Id) as SALES_ORDER_HEADER,to_char(Sales_Order_Line) SALES_ORDER_LINE,to_char(Line_Id) LINE_ID,to_char(Item_Number) ITEM_NUMBER, to_date(Fulfilment_Date) FULFILMENT_DATE,to_char(Batch_Id) BATCH_ID from XXASL.XXOM_LOGILITY_DROP_SHIP_TBL',   
    'TRIGGER_PROCESS': False,
    'TRIGGER_URL': 'http://prod-osb.ansell.com/ans/SO_LinesRelease_Service'
}
]

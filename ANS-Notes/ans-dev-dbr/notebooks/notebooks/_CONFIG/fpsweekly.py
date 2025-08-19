# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': False,
    'DELIMITER': ';',
    'FILE_EXTENSION': '.csv',
	'FILE_NAME': 'DS_SAP_Update_{datetime}',
	'FILE_SHARE': '\\\\EUAZUFPS1.ansell.com\\EDM',
    'GROUPINGS': False,
    'GROUPING_QUERY': '',
    'NOTEBOOK_NAME':'LOGILITY_DROP_SHIP_UPDATE',
    'PROCESS_NAME': 'log4sap',
	'PUBLISH': True,
	'QUERY': "SELECT SAP_Sales_Order, SAP_Sales_Order_Line, Item_Number, Fulfilment_Date FROM g_logility.logility_drop_ship_sap WHERE Processed = 'N'",
    'QUOTE_CHAR':'',
	'TABLE_NAME': 'g_logility.logility_drop_ship_sap',
    'TARGET_FOLDER': 'g_log_for_sap/drop_ship'
}]

# COMMAND ----------



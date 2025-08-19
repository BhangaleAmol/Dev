# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': False,
    'DELIMITER': ';',
    'FILE_EXTENSION': '.csv',
	'FILE_NAME': '{param_value2}-{datetime}-{param_value1}',
	'FILE_SHARE': '\\\\EUAZUFPS1.ansell.com\\EDM',
    'GROUPINGS': True,
    'GROUPING_QUERY': "SELECT DISTINCT groupingid as param_value1, oracle_user_id as param_value2  FROM s_logility.po_logility_sap WHERE Processed='N'",
    'NOTEBOOK_NAME':'LOGILITY_PO_UPDATE',
    'PROCESS_NAME': 'log4sap',
	'PUBLISH': True,
	'QUERY': "SELECT VENDOR,COMPANY_CODE,PLANT_CODE,PURCHASING_GROUP,LOCAL_CODE,[U/M],RETD,QUANTITY,PO_NUM  FROM s_logility.po_logility_sap  WHERE groupingid = '{param_value1}'",
    'QUOTE_CHAR':'',
	'TABLE_NAME': 's_logility.po_logility_sap',
    'TARGET_FOLDER': 'g_log_for_sap/sap_po'
}]

# COMMAND ----------



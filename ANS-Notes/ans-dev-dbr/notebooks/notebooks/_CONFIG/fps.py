# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': False,
    'DELIMITER':',',
	'FILE_NAME': 'cti_cars',
    'FILE_EXTENSION':'.csv',
	'FILE_SHARE': '\\\\stlfps1.ansell.com\\EDM',
    'GROUPINGS':False,
    'GROUPING_QUERY':'',
    'PROCESS_NAME':'cti',
	'PUBLISH': True,
	'QUERY': "SELECT * FROM cti.cars WHERE teamName__s =  'Team_NA-LAC_Inside_Sales'",
    'QUOTE_CHAR':'"',
	'TABLE_NAME': 'cti.cars',
    'TARGET_FOLDER': '/cti'
},
{
	'PartitionKey': '1',
    'RowKey': '2',
    'ACTIVE': False,
    'DELIMITER':',',
    'FILE_NAME': 'cti_csrs',
    'FILE_EXTENSION':'.csv',
    'FILE_SHARE': '\\\\stlfps1.ansell.com\\EDM',
    'GROUPINGS':False,
    'GROUPING_QUERY':'',
    'PROCESS_NAME':'cti',
    'PUBLISH': True,
    'QUERY': '',
    'QUOTE_CHAR':'"',
    'TABLE_NAME': 'cti.csrs',
    'TARGET_FOLDER': '/cti'
}]

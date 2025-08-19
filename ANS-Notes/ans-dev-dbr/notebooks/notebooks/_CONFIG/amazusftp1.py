# Databricks notebook source
config = [
{
    'PartitionKey': '1',
    'RowKey': '1',
    'ACTIVE': True,
    'FILE_NAME': 'Fx Rates.CSV',
    'FOLDER_NAME': 'QV/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'RateCode,RateMonth',
    'PUBLISH': True,
    'TABLE_NAME': 'FX_RATES'
},
{
    'PartitionKey': '1',
    'RowKey': '2',
    'ACTIVE': True,
    'FILE_NAME': 'Exclusion_List.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Exclusion_List'
},
{
    'PartitionKey': '1',
    'RowKey': '3',
    'ACTIVE': True,
    'FILE_NAME': 'WC_PROCON_ALL_CONTRACTS_A.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_PROCON_ALL_CONTRACTS_A'
},
{
    'PartitionKey': '1',
    'RowKey': '4',
    'ACTIVE': True,
    'FILE_NAME': 'WC_QV_POS_EXCLUDED_CUSTOMER_H.CSV',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_QV_POS_EXCLUDED_CUSTOMER_H'
},
{
    'PartitionKey': '1',
    'RowKey': '5',
    'ACTIVE': True,
    'FILE_NAME': 'WC_TM_US_ZIP_CODES_H.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_TM_US_ZIP_CODES_H'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': False,
	'FILE_NAME': 'time_dimension.csv',
	'FOLDER_NAME': 'QV/',
	'HANDLE_DELETE': False,
	'HEADER': True,
	'KEY_COLUMNS': 'dayID',
	'PUBLISH': True,
	'TABLE_NAME': 'time_dimension'
}, 
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': False,
	'FILE_NAME': 'Fx Rates Hist.CSV',
	'FOLDER_NAME': 'QV/',
	'HANDLE_DELETE': False,
	'HEADER': True,
	'KEY_COLUMNS': 'RateCode,RateMonth',
	'PUBLISH': True,
	'TABLE_NAME': 'FX_RATES_HIST'
}, 
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': False,
	'FILE_NAME': 'grainger_conversion_table.csv',
	'FOLDER_NAME': 'QV/',
	'HANDLE_DELETE': False,
	'HEADER': True,
	'KEY_COLUMNS': 'GraingerSKU',
	'PUBLISH': True,
	'TABLE_NAME': 'grainger_conversion_table'
},
{
    'PartitionKey': '1',
    'RowKey': '9',
    'ACTIVE': True,
    'FILE_NAME': 'Henri_Schein.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Henri_Schein'
},
{
    'PartitionKey': '1',
    'RowKey': '10',
    'ACTIVE': True,
    'FILE_NAME': 'MAN_ADJ.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'MAN_ADJ'
},
{
    'PartitionKey': '1',
    'RowKey': '11',
    'ACTIVE': True,
    'FILE_NAME': 'ScheinCAD.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'ScheinCAD'
},
{
    'PartitionKey': '1',
    'RowKey': '12',
    'ACTIVE': True,
    'FILE_NAME': 'NDC.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'NDC'
},
{
    'PartitionKey': '1',
    'RowKey': '13',
    'ACTIVE': True,
    'FILE_NAME': 'Grainger_POS.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Grainger_POS'
},
{
    'PartitionKey': '1',
    'RowKey': '14',
    'ACTIVE': True,
    'FILE_NAME': 'Final_Lookup_canada.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Final_Lookup_canada'
},
{
    'PartitionKey': '1',
    'RowKey': '15',
    'ACTIVE': True,
    'FILE_NAME': 'Airgas.csv',
    'FOLDER_NAME': 'Skybot/',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Airgas'
}
]


# COMMAND ----------



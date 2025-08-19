# Databricks notebook source
config = [
{
    'PartitionKey': '1',
    'RowKey': '1',
    'ACTIVE': True,
    'FILE_NAME': 'budget exchange rates.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'Currency,FiscalYear',
    'PUBLISH': True,
    'TABLE_NAME': 'FX_BUDGET_RATES'
},
{
    'PartitionKey': '1',
    'RowKey': '2',
    'ACTIVE': False,
    'FILE_NAME': 'Budget Mexico FY20.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'CustomerCode, ShipToDeliveryLocationID, ProductNumber, Month',
    'PUBLISH': True,
    'TABLE_NAME': 'Budget_Mexico'
},
{
    'PartitionKey': '1',
    'RowKey': '3',
    'ACTIVE': False,
    'FILE_NAME': 'Budget OLAC FY20.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'CustomerCode, ShipToDeliveryLocationID, ProductNumber, Month',
    'PUBLISH': True,
    'TABLE_NAME': 'Budget_OLAC'
},
{
    'PartitionKey': '1',
    'RowKey': '4',
    'ACTIVE': True,
    'FILE_NAME': 'Fx Rates Hist.CSV',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'RateCode,RateMonth',
    'PUBLISH': True,
    'TABLE_NAME': 'FX_RATES_HIST'
},
{
    'PartitionKey': '1',
    'RowKey': '5',
    'ACTIVE': True,
    'FILE_NAME': 'grainger_conversion_table.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'GraingerSKU',
    'PUBLISH': True,
    'TABLE_NAME': 'grainger_conversion_table'
},
{
    'PartitionKey': '1',
    'RowKey': '6',
    'ACTIVE': True,
    'FILE_NAME': 'Grainger_Data_Export_2019.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Grainger_2019'
},
{
    'PartitionKey': '1',
    'RowKey': '7',
    'ACTIVE': True,
    'FILE_NAME': 'Grainger_Data_Export_2020.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'Grainger_2020'
},
{
    'PartitionKey': '1',
    'RowKey': '8',
    'ACTIVE': True,
    'FILE_NAME': 'time_dimension.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': 'dayID',
    'PUBLISH': True,
    'TABLE_NAME': 'time_dimension'
},
{
    'PartitionKey': '1',
    'RowKey': '9',
    'ACTIVE': True,
    'FILE_NAME': 'WC_PROCON_ALL_CONTRACTS_A.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_PROCON_ALL_CONTRACTS_A'
},
{
    'PartitionKey': '1',
    'RowKey': '10',
    'ACTIVE': True,
    'FILE_NAME': 'WC_QV_POS_EXCLUDED_CUSTOMER_H.CSV',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_QV_POS_EXCLUDED_CUSTOMER_H'
},
{
    'PartitionKey': '1',
    'RowKey': '11',
    'ACTIVE': True,
    'FILE_NAME': 'WC_TM_US_ZIP_CODES_H.csv',
    'FOLDER_NAME': 'MISC',
    'HANDLE_DELETE': False,
    'HEADER': True,
    'KEY_COLUMNS': '',
    'PUBLISH': True,
    'TABLE_NAME': 'WC_TM_US_ZIP_CODES_H'
}
]

# COMMAND ----------



# Databricks notebook source
config = [
{
    'PartitionKey': '1',
    'RowKey': '1',
    'ACTIVE': True,
    'PUBLISH': True,
    'QUERY': 'SELECT account_number AS CustID, customer_name AS CustName, product_number AS ProductID, product_name AS ProductDescr, CAST(net_units AS DECIMAL(18,2)) AS InvoicedQtyInPieces, CAST(returns AS DECIMAL(18,2)) AS ReturnsQtyInPieces, invoice_Month AS InvoiceMonth,  CASE WHEN net_amt_usd IS NULL THEN CAST(0 AS DECIMAL(18,2)) ELSE CAST(net_amt_usd AS DECIMAL(18,2)) END AS InvoiceSalesAmount,  CASE WHEN return_amt_usd IS NULL THEN CAST(0 AS DECIMAL(18,2)) ELSE CAST(return_amt_usd AS DECIMAL(18,2)) END AS ReturnsAmount FROM {source_table} WHERE 1=1',
    'SOURCE_TABLE': 'g_fin_qv.WC_QV_12_MONTH_SALES',
    'TABLE_NAME': 'dl.DL12MonthSales'
}
]

# COMMAND ----------



# Databricks notebook source
config = [
{
	'PartitionKey': '1',
	'RowKey': '1',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_customer',
	'TABLE_NAME': 'DIM_CUSTOMER'
}, 
{
	'PartitionKey': '1',
	'RowKey': '2',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_engineer',
	'TABLE_NAME': 'DIM_ENGINEER'
}, 
{
	'PartitionKey': '1',
	'RowKey': '3',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_entity',
	'TABLE_NAME': 'DIM_ENTITY'
}, 
{
	'PartitionKey': '1',
	'RowKey': '4',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_support_group',
	'TABLE_NAME': 'DIM_SUPPORT_GROUP'
}, 
{
	'PartitionKey': '1',
	'RowKey': '5',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_ticket',
	'TABLE_NAME': 'DIM_TICKET'
}, 
{
	'PartitionKey': '1',
	'RowKey': '6',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_ticket_category',
	'TABLE_NAME': 'DIM_TICKET_CATEGORY'
}, 
{
	'PartitionKey': '1',
	'RowKey': '7',
	'ACTIVE': True,
	'DIMENSION': False,
	'NOTEBOOK_NAME': 'glpi_fact_ticket_status',
	'TABLE_NAME': 'FACT_TICKET_STATUS'
}, 
{
	'PartitionKey': '1',
	'RowKey': '8',
	'ACTIVE': True,
	'DIMENSION': False,
	'NOTEBOOK_NAME': 'glpi_map_engineer_group',
	'TABLE_NAME': 'MAP_ENGINEER_GROUP'
}, 
{
	'PartitionKey': '1',
	'RowKey': '9',
	'ACTIVE': True,
	'DIMENSION': False,
	'NOTEBOOK_NAME': 'glpi_map_ticket_item',
	'TABLE_NAME': 'MAP_TICKET_ITEM'
}, 
{
	'PartitionKey': '1',
	'RowKey': '10',
	'ACTIVE': True,
	'DIMENSION': True,
	'NOTEBOOK_NAME': 'glpi_dim_item',
	'TABLE_NAME': 'DIM_ITEM'
}]


# COMMAND ----------



# Databricks notebook source
config = [    
    {
        'PartitionKey': 'tembo_master_pipeline',
        'RowKey': 'pipeline_start',        
        'RECIPIENT': 'anslogilitydlyrptexc@ansell.com'
    },
    {
        'PartitionKey': 'tembo_master_pipeline',
        'RowKey': 'pipeline_success',        
        'RECIPIENT': 'anslogilitydlyrptexc@ansell.com'
    },
    {
        'PartitionKey': 'tembo_master_pipeline',
        'RowKey': 'pipeline_failed',        
        'RECIPIENT': 'anslogilitydlyrptexc@ansell.com'
    },
    {
        'PartitionKey': 'natm_master_pipeline',
        'RowKey': 'pipeline_success',        
        'RECIPIENT': 'grp_trademanagement_NA@ansell.com'
    },
    {
        'PartitionKey': 'cor_master_pipeline',
        'RowKey': 'pipeline_failed',        
        'RECIPIENT': 'businessintelligenceglobal@ansell.com'
    },
    {
        'PartitionKey': 'sup_master_pipeline',
        'RowKey': 'pipeline_failed',        
        'RECIPIENT': 'businessintelligenceglobal@ansell.com'
    },
    {
        'PartitionKey': 'finqv_master_pipeline',
        'RowKey': 'pipeline_failed',        
        'RECIPIENT': 'businessintelligenceglobal@ansell.com'
    }
]

# COMMAND ----------



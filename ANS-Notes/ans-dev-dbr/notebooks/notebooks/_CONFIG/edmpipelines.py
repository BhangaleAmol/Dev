# Databricks notebook source
config = [
    #   cal_master_pipeline
    {
        'PartitionKey': 'cal_master_pipeline',
        'RowKey': 'ctiv2_master_pipeline',        
        'TRIGGER': False
    },
    #   callus_master_pipeline
    {
        'PartitionKey': 'callus_master_pipeline',
        'RowKey': 'cal_master_pipeline',        
        'TRIGGER': False
    },
    #   callemea_master_pipeline
    {
        'PartitionKey': 'callemea_master_pipeline',
        'RowKey': 'cal_master_pipeline',        
        'TRIGGER': False
    },
    #   cor_master_pipeline    
    {
        'PartitionKey': 'cor_master_pipeline',
        'RowKey': 'col_master_pipeline',        
        'TRIGGER': False,
        'GROUP': 'lac'
    },
    {
        'PartitionKey': 'cor_master_pipeline',
        'RowKey': 'ebsv2_master_pipeline',        
        'TRIGGER': False
    },    
    {
        'PartitionKey': 'cor_master_pipeline',
        'RowKey': 'tot_master_pipeline',        
        'TRIGGER': False,
        'GROUP': 'lac'
    },    
    #   gpi_master_pipeline
    {
        'PartitionKey': 'gpi_master_pipeline',
        'RowKey': 'svc_master_pipeline',        
        'TRIGGER': False
    },
    #   mnf_master_pipeline
    {
        'PartitionKey': 'mnf_master_pipeline',
        'RowKey': 'gpd_master_pipeline',        
        'TRIGGER': False
    },
    #   natm_master_pipeline
    {
        'PartitionKey': 'natm_master_pipeline',
        'RowKey': 'tdm_master_pipeline',        
        'TRIGGER': False
    },
    #   sup_master_pipeline    
    {
        'PartitionKey': 'sup_master_pipeline',
        'RowKey': 'col_master_pipeline',        
        'TRIGGER': False,
        'GROUP': 'lac'
    },    
    {
        'PartitionKey': 'sup_master_pipeline',
        'RowKey': 'ebsv2_master_pipeline',        
        'TRIGGER': False
    },    
    {
        'PartitionKey': 'sup_master_pipeline',
        'RowKey': 'tot_master_pipeline',        
        'TRIGGER': False,
        'GROUP': 'lac'
    },
    #   svc_master_pipeline
    {
        'PartitionKey': 'svc_master_pipeline',
        'RowKey': 'com_master_pipeline',        
        'TRIGGER': False
    },
    {
        'PartitionKey': 'svc_master_pipeline',
        'RowKey': 'twd_master_pipeline',        
        'TRIGGER': False
    },
    #   tdm_master_pipeline
    {
        'PartitionKey': 'tdm_master_pipeline',
        'RowKey': 'tdm_trigger_pipeline',        
        'TRIGGER': False,        
    },
    {
        'PartitionKey': 'tdm_master_pipeline',
        'RowKey': 'cor_master_pipeline',        
        'TRIGGER': False,        
    },    
    {
        'PartitionKey': 'tdm_master_pipeline',
        'RowKey': 'ebsv2_master_pipeline',        
        'TRIGGER': False
    },
    {
        'PartitionKey': 'ebs_master_pipeline',
        'RowKey': 'ebsv2_master_pipeline',  
        'TRIGGER': False
    },
    {
        'PartitionKey': 'ma_master_pipeline',
        'RowKey': 'cor_master_pipeline',  
        'TRIGGER': False
    },
    {
        'PartitionKey': 'ma_master_pipeline',
        'RowKey': 'sup_master_pipeline',  
        'TRIGGER': False
    },       
    {
      'PartitionKey': 'tembo_master_pipeline',
      'RowKey': 'cor_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'tembo_master_pipeline',
      'RowKey': 'sup_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'tembo_master_pipeline',
      'RowKey': 'tembo_trigger_pipeline',        
      'TRIGGER': False
    },
    # G_FIN_QV - finqv_master_pipeline
    {
      'PartitionKey': 'finqv_master_pipeline',
      'RowKey': 'ebsv2_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'finqv_master_pipeline',
      'RowKey': 'cor_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 1
    },
    {
      'PartitionKey': 'finqv_master_pipeline',
      'RowKey': 'sup_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 1
    },
    {
      'PartitionKey': 'tsys_master_pipeline',
      'RowKey': 'glpiv2_master_pipeline',    
      'TRIGGER': False
    },
    {
      'PartitionKey': 'fin_master_pipeline',
      'RowKey': 'ebsv2_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'slog_master_pipeline',
      'RowKey': 'logftp_master_pipeline',        
      'TRIGGER': False
    },    
    {
      'PartitionKey': 'slogord_master_pipeline',
      'RowKey': 'logord_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'slogweekly_master_pipeline',
      'RowKey': 'logftpweekly_master_pipeline',        
      'TRIGGER': False
    },
	{
      'PartitionKey': 'glog_master_pipeline',
      'RowKey': 'slog_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'glogweekly_master_pipeline',
      'RowKey': 'slogweekly_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'comind_master_pipeline',
      'RowKey': 'ebsv2_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'elog_master_pipeline',
      'RowKey': 'glog_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'elogord_master_pipeline',
      'RowKey': 'slogord_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'elog_master_pipeline',
      'RowKey': 'elog_trigger_pipeline',        
      'TRIGGER': False
    },    
    {
      'PartitionKey': 'fpsord_master_pipeline',
      'RowKey': 'slogord_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'fpsweekly_master_pipeline',
      'RowKey': 'glogweekly_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'elogebs_master_pipeline',
      'RowKey': 'glogweekly_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'eiforzdev_master_pipeline',
      'RowKey': 'swg_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'eiforztest_master_pipeline',
      'RowKey': 'swg_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'eiforzprod_master_pipeline',
      'RowKey': 'swg_master_pipeline',        
      'TRIGGER': False
    },
    # G_GSC_SUP - gscsup_master_pipeline
    {
      'PartitionKey': 'gscsup_master_pipeline',
      'RowKey': 'ebsv2_master_pipeline',        
      'TRIGGER': False
    },
    {
      'PartitionKey': 'gscsup_master_pipeline',
      'RowKey': 'cor_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 1
    },
    {
      'PartitionKey': 'gscsup_master_pipeline',
      'RowKey': 'sup_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 1
    },
    {
      'PartitionKey': 'upi_master_pipeline',
      'RowKey': 'cor_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 4
    },
    {
      'PartitionKey': 'upi_master_pipeline',
      'RowKey': 'sup_master_pipeline',        
      'TRIGGER': False,
      'HORIZON': 4
    }
]

# COMMAND ----------



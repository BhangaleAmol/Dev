# Databricks notebook source
config = [
 {
   'PartitionKey': '1',
   'RowKey': '1',
   'ACTIVE': True,
   'ENVIRONMENT': 'dev',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforzdev_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 }, 
 {
   'PartitionKey': '1',
   'RowKey': '2',
   'ACTIVE': True,
   'ENVIRONMENT': 'dev',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforztest_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 },
  {
   'PartitionKey': '1',
   'RowKey': '3',
   'ACTIVE': True,
   'ENVIRONMENT': 'dev',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforzprod_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 },
  {
   'PartitionKey': '1',
   'RowKey': '4',
   'ACTIVE': True,
   'ENVIRONMENT': 'test',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforzdev_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 }, 
 {
   'PartitionKey': '1',
   'RowKey': '5',
   'ACTIVE': True,
   'ENVIRONMENT': 'test',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforztest_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 },
  {
   'PartitionKey': '1',
   'RowKey': '6',
   'ACTIVE': True,
   'ENVIRONMENT': 'test',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata_test',
   'PIPELINE_NAME': 'eiforzprod_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 },
  {
   'PartitionKey': '1',
   'RowKey': '7',
   'ACTIVE': True,
   'ENVIRONMENT': 'prod',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata',
   'PIPELINE_NAME': 'eiforzdev_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 }, 
 {
   'PartitionKey': '1',
   'RowKey': '8',
   'ACTIVE': True,
   'ENVIRONMENT': 'prod',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata',
   'PIPELINE_NAME': 'eiforztest_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 },
  {
   'PartitionKey': '1',
   'RowKey': '9',
   'ACTIVE': True,
   'ENVIRONMENT': 'prod',
   'INCREMENTAL': True,   
   'PUBLISH': True,
   'SOURCE_TABLE': 'swg.ansellgloveprodenrichdata_v',
   'TABLE_NAME': 'data_ansellgloveprodenrichdata',
   'PIPELINE_NAME': 'eiforzprod_wrapper_pipeline',
   'QUERY': 'SELECT deviceId,fle_ext_count_slow,fle_ext_count_fast,rad_uln_count_slow,rad_uln_count_fast,sup_pro_count_slow,sup_pro_count_fast,fle_ext_dur,rad_uln_dur,sup_pro_dur,idle_dur,walk_dur,tot_dur,wrist_score,batchId,runId,userId,organizationId,jobFunctionId,locationId,hand,timeStampStart,timeStampEnd,localTImeStampStart,localTImeStampEnd,payloadCountInit,payloadCountFinal,durationSecs,durationSecs1,haptic_violation_count,rollAngle,thumbsUpVibration,thumbsUpThreshold,thumbsUpWindowSize,_modified, thumbsUpHaptic, thumbsUpHaptic_count, timedHaptic, timedHaptic_count, timedHapticWindowSize FROM {source_table} WHERE 1=1' 
 }
]

# COMMAND ----------



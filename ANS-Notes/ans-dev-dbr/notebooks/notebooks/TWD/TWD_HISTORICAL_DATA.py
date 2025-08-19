# Databricks notebook source
#need to bring in 4 sheets:
#1.Masterlist by Lot
#2.Masterlist by Defect
#3.Masterlist by Record ID
#4. Plant Response Time

# COMMAND ----------

# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_xlsx

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/TWD/raw_data/delta_data'
target_folder = target_folder or '/datalake/TWD/raw_data/full_data'
database_name = database_name or "twd"
file_name = file_name or "Data_for_Global_Complaint_Dashboard.xlsx"
sheet_name = sheet_name or 'Masterlist by Lot'
table_name = table_name or "Masterlistbylot"
header = header or True

print(source_folder)
print(target_folder)
print(file_name)
print(table_name)

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_xlsx_params()

# COMMAND ----------

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = '/dbfs/mnt/datalake{0}/{1}'.format(source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

import pandas as pd
MAIN_PD = pd.read_excel(source_file_path, header = 0, sheet_name = sheet_name, engine='openpyxl')
MAIN_PD = MAIN_PD.astype(str)
MAIN_PD = MAIN_PD.drop(MAIN_PD.filter(regex='Unnamed:').columns, axis=1)

# COMMAND ----------

MASTERLISTBYLOT = spark.createDataFrame(MAIN_PD)
MASTERLISTBYLOT_F = (
  MASTERLISTBYLOT
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(attach_surrogate_key(columns = #'TWGBU,Month,Date,BatchLot,RecordID,CurrentState,Subdivision,Region,Country,OriginatingMfg,AssignedTo,Originator,ProductBrand,ProductStyle,ProductCategory,ProductFamily,ProductGroup,ShortDescription,IsClosed,Count,Helper,ProductBrandCorrected,SBU,SBU2,GBU,Closed,Source', name='_ID'))
'RecordID,BatchLot', name='_ID'))  
)

display(MASTERLISTBYLOT_F)

# COMMAND ----------

# VALIDATE NK - SHOW DUPLICATES
show_duplicate_rows(MASTERLISTBYLOT_F, ['_ID'])

# COMMAND ----------

# DELETE FLAG
MASTERLISTBYLOT_F = MASTERLISTBYLOT_F.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

valid_count_rows(MASTERLISTBYLOT_F, "_ID", 'MASTERLISTBYLOT_F')

# COMMAND ----------

# PERSIST DATA
merge_to_delta(MASTERLISTBYLOT_F, table_name, target_folder, overwrite)

# COMMAND ----------

source_folder = None
target_folder = None
database_name = None
file_name = None
sheet_name = None
table_name = None

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/TWD/raw_data/delta_data'
target_folder = target_folder or '/datalake/TWD/raw_data/full_data'
database_name = database_name or "twd"
file_name = file_name or "Data_for_Global_Complaint_Dashboard.xlsx"
sheet_name = sheet_name or 'Masterlist by Defect'
table_name = table_name or "Masterlistbydefect"
header = header or True

print(source_folder)
print(target_folder)
print(file_name)
print(table_name)

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_xlsx_params()

# COMMAND ----------

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = '/dbfs/mnt/datalake{0}/{1}'.format(source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

import pandas as pd
MAIN_PD = pd.read_excel(source_file_path, header = 0, sheet_name = sheet_name, engine='openpyxl')
MAIN_PD = MAIN_PD.astype(str)
MAIN_PD = MAIN_PD.drop(MAIN_PD.filter(regex='Unnamed:').columns, axis=1)

# COMMAND ----------

MASTERLISTBYDEFECT = spark.createDataFrame(MAIN_PD)
MASTERLISTBYDEFECT_F = (
  MASTERLISTBYDEFECT
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(attach_surrogate_key(columns = #'TWGBU,Month,Date,RecordID,ComplaintType,Subdivision,Country,Region,BatchLot,OriginatingMfg,ProductBrand,ProductStyle,ProductCategory,ProductFamily,ProductGroup,AssignedTo,COUNT,Helper,ProductBrandCorrected,SBU,GBU,SBU2,Source,Defect', name='_ID')) 
'RecordID,ComplaintType,ProductStyle,BatchLot,Defect, Helper', name='_ID'))  
)

display(MASTERLISTBYDEFECT_F)

# COMMAND ----------

# DELETE FLAG
MASTERLISTBYDEFECT_F = MASTERLISTBYDEFECT_F.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

# VALIDATE NK - SHOW DUPLICATES
show_duplicate_rows(MASTERLISTBYDEFECT_F, ['_ID'])

# COMMAND ----------

valid_count_rows(MASTERLISTBYDEFECT_F, "_ID", 'MASTERLISTBYDEFECT_F')

# COMMAND ----------

# PERSIST DATA
merge_to_delta(MASTERLISTBYDEFECT_F, table_name, target_folder, overwrite)

# COMMAND ----------

source_folder = None
target_folder = None
database_name = None
file_name = None
sheet_name = None
table_name = None

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/TWD/raw_data/delta_data'
target_folder = target_folder or '/datalake/TWD/raw_data/full_data'
database_name = database_name or "twd"
file_name = file_name or "Data_for_Global_Complaint_Dashboard.xlsx"
sheet_name = sheet_name or 'Masterlist by Record ID'
table_name = table_name or "Masterlistbyrecordid"
header = header or True

print(source_folder)
print(target_folder)
print(file_name)
print(table_name)

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_xlsx_params()

# COMMAND ----------

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = '/dbfs/mnt/datalake{0}/{1}'.format(source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

import pandas as pd
MAIN_PD = pd.read_excel(source_file_path, header = 0, sheet_name = sheet_name, engine='openpyxl')
MAIN_PD = MAIN_PD.astype(str)
MAIN_PD = MAIN_PD.drop(MAIN_PD.filter(regex='Unnamed:').columns, axis=1)

# COMMAND ----------

MASTERLISTBYRECORDID = spark.createDataFrame(MAIN_PD)
MASTERLISTBYRECORDID_F = (
  MASTERLISTBYRECORDID
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(attach_surrogate_key(columns = #'RecordID,Month,MonthClosed,DateCreated,TWGBU,Region,Subdivision,Country,OriginatingMfg,ComplaintType,AssignedTo,ProductBrand,ProductStyle,ProductCategory,ProductFamily,ProductGroup,ShortDescription,Distributor,CurrentState,DaysInCurrentState,IsPastDue,DaysPastDue,IsClosed,DaysOpenOriginalClosure,Investigation,Opened,PendingApproval,PendingComplaintAdministratorApproval,PendingRouting,ReviewandAssessment,RootCauseAnalysis,PlantRespTime,CAProcessTime,TotalResponseTime,Count,Defect,Helper,ProductBrandCorrected,SBU,GBU,SBU2,Source,DistributorCorrected,Status,Closed', name='_ID'))  
'RecordID', name='_ID'))  
)

display(MASTERLISTBYRECORDID_F)

# COMMAND ----------

# DELETE FLAG
MASTERLISTBYRECORDID_F = MASTERLISTBYRECORDID_F.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

valid_count_rows(MASTERLISTBYRECORDID_F, "_ID", 'MASTERLISTBYRECORDID_F')
# init_load = init_hive_table(MASTERLISTBYRECORDID_F, database_name, table_name, file_path = target_file_path, mode ="overwrite")

# COMMAND ----------

# PERSIST DATA
merge_to_delta(MASTERLISTBYRECORDID_F, table_name, target_folder, overwrite)

# COMMAND ----------

source_folder = None
target_folder = None
database_name = None
file_name = None
sheet_name = None
table_name = None

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/TWD/raw_data/delta_data'
target_folder = target_folder or '/datalake/TWD/raw_data/full_data'
database_name = database_name or "twd"
file_name = file_name or "Data_for_Global_Complaint_Dashboard.xlsx"
sheet_name = sheet_name or 'Plant Response Time'
table_name = table_name or "Plantresponsetime"
header = header or True

print(source_folder)
print(target_folder)
print(file_name)
print(table_name)

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_xlsx_params()

# COMMAND ----------

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = '/dbfs/mnt/datalake{0}/{1}'.format(source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

import pandas as pd
MAIN_PD = pd.read_excel(source_file_path, header = 0, sheet_name = sheet_name, engine='openpyxl')
MAIN_PD = MAIN_PD.astype(str)
MAIN_PD = MAIN_PD.drop(MAIN_PD.filter(regex='Unnamed:').columns, axis=1)
# MAIN_PD.head()

# COMMAND ----------

PLANTRESPONSETIME = spark.createDataFrame(MAIN_PD)
PLANTRESPONSETIME_F = (
  PLANTRESPONSETIME
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(attach_surrogate_key(columns = #'RecordId,TWGBU,Country,Subdivision,Region,OriginatingMfg,MonthCreated,MonthClosed,QApproval,InvestigationQA,Opened,PendingpprovalQA,PendingComplaintAdministratorApproval,PendingRouting,ReviewandAssessment,RootCauseAnalysisQA,PlantResponse,CAResponse,TotalResponseTime,SBU,SBU2,GBU,Source,QapprovalFYQ,ClosedQtr,ClosedFY', name='_ID'))  
'RecordId', name='_ID'))  
)

display(PLANTRESPONSETIME_F)

# COMMAND ----------

# DELETE FLAG
PLANTRESPONSETIME_F = PLANTRESPONSETIME_F.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

valid_count_rows(PLANTRESPONSETIME_F, "_ID")

# COMMAND ----------

# PERSIST DATA
merge_to_delta(PLANTRESPONSETIME_F, table_name, target_folder, overwrite)
merge_into_hive_table(PLANTRESPONSETIME_F, table_name, key_columns = '_ID')

# COMMAND ----------



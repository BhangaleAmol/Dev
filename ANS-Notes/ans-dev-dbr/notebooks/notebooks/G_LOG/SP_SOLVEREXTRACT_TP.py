# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['Item','Location','Solver_Period','generationDate'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'sp_solverextract_tp')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name,table_name,None)
target_table_agg = table_name[:table_name.rfind("_")] + '_agg'
currentuser = str(spark.sql("select current_user()").collect()[0][0])
currenttime = datetime.now()
source_table = 'logftp.logility_sp_cp_extract'
viewName = 'logility_sp_cp_extract'

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)
  print(cutoff_value)
  logility_sp_cp_extract = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_sp_cp_extract = load_full_dataset(source_table)

logility_sp_cp_extract.createOrReplaceTempView(viewName)
# logility_sp_cp_extract.display()

# COMMAND ----------

# DBTITLE 1,Demand
main = spark.sql("""
SELECT 
ITEM_ID as Item,
LOC_ID as Location,
SLVR_PRD as Solver_Period,
date_format(to_date(replace(SLVR_DATE,':000000000',''),'yyyyMMdd'),'MM/dd/yyyy') as Solver_Date,
PRD_TYPE as Period_Type_Weekly, 
cast(SS_QTY as int) as Safety_Stock,
cast(HLD_COST_VALUE as numeric(15,5)) as Holding_Cost_Value,
cast(XFER_IN_MIN_QTY as int) as Transfer_in_min_Quantity,
cast(AVAIL_INV_QTY as int) as Available_Inventory_Quantity,
cast(ADJ_INV_QTY as int) as Adjusted_Inventory_Quantity,
cast(FIRM_PROD_QTY as int) as Firm_Production_Quantity,
cast(PLND_XFER_OUT_QTY as int) as Planned_Transfer_Out_Quantity,
cast(FIRM_XFER_OUT_QTY as int) as Firmed_Transfer_Out_Quantity,
cast(CUST_ONTIME_ORD_QTY as int) as CustomerOrder_On_time_Quantity,
cast(CUST_LATE_ORD_QTY as int) as CustomerOrder_Late_Quantity,
cast(FULFILL_LATE_QTY as int) as Late_Fulfilled_Quantity,
cast(DMD_FULFILL_QTY as int) as Demand_Fulfilled_Quantity_Commited_Plan,
cast(FIRM_PURCH_QTY as int) as Firm_Purchase_Quantity,
cast(NET_FCST_QTY as int) as Net_Forecast,
cast(FIRM_DMD_QTY as int) as Open_Customer_Orders,
cast(PLND_PROD_QTY as int) as Planned_Production_Quantity,
cast(PLND_PURCH_QTY as int) as Planned_Purchase_Quantity,
cast(FIRM_XFER_INTRNST_QTY as int) as Firm_Intransit_Quantity,
cast(FIRM_XFER_ONORD_QTY as int) as Firm_PO_Quantity ,
cast(KIT_COMPT_USG_QTY as int) as Kit_Component_Usage_Quantity,
cast(DWELL_QTY as int) as Dwell_Qty,
cast(PLND_XFER_IN_QTY as int) as Planned_Transfer_in_Quantity,
cast(NET_INV_QTY as int) as Net_Inventory_Quantity,
cast(ATP_QTY as int) as ATP_Quantity,
cast(SUBST_USG_QTY as int) as Substitute_Usage_Quantity,
cast(SUBST_QTY as int) as Substitute_Quantity,
cast(RETN_PURCH_QTY as int) as Return_PO_Quantity,
cast(PROMO_LIFT_QTY as int) as Promotional_Lift_Quantity,
cast(ALLCD_INV_QTY as int) as Allocated_Inventory_Quantity,
--cast(VM_ADJ_PROD_ORD_QTY as int) as Vendor_Min_Production_Quantity, --removed from extract on 02/24
cast(VM_ADJ_PURCH_ORD_QTY as int) as Vendor_Min_Purchase_Quantity,
cast(VM_USR_ADJ_PROD_ORD_QTY as int) as User_Adj_Vendor_Min_Production_Quantity,
cast(VM_USR_ADJ_PURCH_ORD_QTY as int) as User_Adj_Vendor_Min_Purchase_Quantity,
cast(EXPIR_INV_QTY as int) as Expired_Inventory_Quantity,
cast(SHPBL_INV_QTY as int) as Shippable_Inventory,
GenerationDate,
current_timestamp() as _MODIFIED
FROM 
logility_sp_cp_extract
""")
main.display()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main, key_columns)

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
append_into_table(main, target_table, {"incremental_column":"GenerationDate"})

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, '_MODIFIED')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)

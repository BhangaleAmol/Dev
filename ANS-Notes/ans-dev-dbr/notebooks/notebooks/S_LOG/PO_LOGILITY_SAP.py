# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

ENV_NAME = os.getenv('ENV_NAME')
ENV_NAME = ENV_NAME.upper()
ENV_NAME

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['GroupingID','LOCAL_CODE'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'po_logility_sap')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/logility/temp_data')
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
source = "LOG"

# COMMAND ----------

# DBTITLE 1,Read Source Data
# LOAD DATASETS
if incremental:
  po_cutoff_value = get_cutoff_value("logftp.logility_sp_purchase_orders_export", None, prune_days)
  to_cutoff_value = get_cutoff_value("logftp.logility_sp_transfer_orders_export", None, prune_days)
  print(po_cutoff_value,to_cutoff_value)
  source_purchase_df = load_incremental_dataset("logftp.logility_sp_purchase_orders_export", '_MODIFIED', po_cutoff_value)
  source_transfer_df = load_incremental_dataset("logftp.logility_sp_transfer_orders_export", '_MODIFIED', to_cutoff_value)  
else:
  source_purchase_df = load_full_dataset("logftp.logility_sp_purchase_orders_export")
  source_transfer_df = load_full_dataset("logftp.logility_sp_transfer_orders_export")

source_df = source_purchase_df.union(source_transfer_df)  
source_df.createOrReplaceTempView('logility_sp_purchase_orders_export')
# source_df.display()

# COMMAND ----------

pogrouping=spark.sql("""
WITH cte
(
       SELECT item,
              location_dc,
              source_location,
              to_date(replace(need_by_date,':000000000',''),'yyyyMMdd') need_by_date,
              quantity,
              uom,
              logility_user_id,
              planner_id,
              order_id ,
              int(month(to_date(replace(need_by_date,':000000000',''),'yyyyMMdd'))) mnth,
              _SOURCEFILENAME
       FROM   logility_sp_purchase_orders_export 
       WHERE     source_location not like 'DC%'
                 AND location_dc not like 'DS%'                 
)
,cte1
(
         SELECT   md5(concat_ws(location_dc,source_location,logility_user_id,mnth)) as groupingid,
                  --rank() OVER(ORDER BY location_dc,source_location,logility_user_id,mnth) groupingid,
                  item,
                  location_dc,
                  source_location,
                  need_by_date ,
                  quantity,
                  uom,
                  logility_user_id,
                  mnth,
                  date_part('YEAR',need_by_date)
                           || '-'
                           || date_part('MON',need_by_date) yymm ,
                  date_part('DAY',need_by_date)             dd,
                  planner_id,
                  order_id,
                  _SOURCEFILENAME
         FROM     cte 
) 
,cte2
(
       SELECT groupingid,
              item,
              location_dc,
              source_location,
              yymm,
              dd,
              uom,
              logility_user_id,
              quantity,
              planner_id,
              order_id,
              _SOURCEFILENAME
       FROM   cte1 
)
,cte3
(
         SELECT   groupingid,
                  yymm ,
                  max(dd) maxday
         FROM     cte2
         GROUP BY groupingid,
                  yymm 
) 
,cte4
(
       SELECT cte2.groupingid,
              cte2.item,
              cte2.location_dc,
              cte2.source_location,
              cte2.yymm
                     || '-'
                     || cte3.maxday need_by_date,
              cte2.quantity,
              cte2.uom,
              cte2.logility_user_id,
              cte2.planner_id,
              cte2.order_id,
              cte2._SOURCEFILENAME
       FROM   cte2
       JOIN   cte3
       ON     cte2.groupingid = cte3.groupingid
       AND    cte2.yymm = cte3.yymm 
)
SELECT   groupingid,
         item,
         location_dc,
         source_location,
         need_by_date,
         Sum(quantity) Quantity,
         uom,
         planner_id,
         order_id,
         logility_user_id,
         _SOURCEFILENAME
FROM     cte4
GROUP BY groupingid,
         item,
         location_dc,
         source_location,
         need_by_date,
         uom,
         planner_id,
         order_id,
         logility_user_id,
         _SOURCEFILENAME
""")
# pogrouping.display()
pogrouping.createOrReplaceTempView('pogrouping')

# COMMAND ----------

pogroups=spark.sql("""
select po.groupingid,
po.item,
po.location_dc,
po.source_location,
po.need_by_date,
po.Quantity,
case when po.uom = 'PIECE' then 'PC' 
     when po.uom = 'PAIR' then 'PR'
else
    po.uom
end as uom,
po.planner_id,
po.order_id,
po.logility_user_id,
po._SOURCEFILENAME,
lm.InventoryHoldingERP from pogrouping po left join spt.locationmaster lm on lm.GlobalLocationCode = po.Location_DC
WHERE InventoryHoldingERP = 'SAP'
""")

# pogroups.display()
pogroups.createOrReplaceTempView('pogroups')

# COMMAND ----------

main=spark.sql("""
with cte(  
  SELECT 'AAL1' as ERPLocCode,'C50' as COMPANY_CODE
  UNION ALL
  SELECT 'AAL2','C50' 
  UNION ALL
  SELECT 'AAL3','C50' 
  UNION ALL
  SELECT 'AJN1','C52' 
  UNION ALL
  SELECT 'ANV1','C20' 
  UNION ALL
  SELECT 'ANV2','C20' 
  UNION ALL
  SELECT 'ANV3','C20' 
  UNION ALL
  SELECT 'NLT1','C32'  
)  

select distinct GroupingID,SAPGOODSSUPPLIERCODE VENDOR, cte.COMPANY_CODE, replace(po.Location_DC,'DC','') PLANT_CODE, 
CASE WHEN prod.GBU = 'INDUSTRIAL'  then 'IND'  
WHEN prod.GBU = 'MEDICAL' then 'MED'
WHEN prod.GBU = 'SINGLE USE' then 'SU'
ELSE null END as PURCHASING_GROUP,
po.ITEM LOCAL_CODE, 
UOM `U/M`, 
date_format(Need_By_Date, 'dd.MM.y') RETD, 
Quantity QUANTITY, cast(null as string) PO_NUM,
case when user.login IN ('DOTJUPC','RAMMVER1') then 'BRUDLEF1' else user.login end as ORACLE_USER_ID, --user.login BRUDLEF1
po._SOURCEFILENAME,
'N' as Processed
from pogroups po
left join cte on cte.ERPLocCode = replace(po.Location_DC,'DC','')
left join spt.locationmaster lm on lm.GlobalLocationCode = po.Source_Location
left join s_core.product_agg prod on prod.e2eproductCode = po.ITEM and _SOURCE='PDH'
left join (select replace(internalEmailAddress, substr(internalEmailAddress, charindex('@', internalEmailAddress)), '') internalEmailAddress,login from s_core.user_agg where _SOURCE = 'EBS') user on user.internalEmailAddress = po.Logility_User_ID 
""")

# main.display()
main.createOrReplaceTempView('po_sap')

# COMMAND ----------

if not table_exists(target_table) or overwrite==True:
  print("SAP PO Table not exists")
  query = "SELECT * FROM po_sap"
  main_f = spark.sql(query)
  overwrite=True   

else:
  print("SAP PO Table exists")
  query = f""" 
SELECT *
FROM   po_sap a
WHERE  NOT EXISTS (SELECT groupingid
                   FROM   {target_table} b
                   WHERE  a.groupingid = b.groupingid and 
                          a.LOCAL_CODE = b.LOCAL_CODE
                          ) 
    """
  main_f = spark.sql(query)
  overwrite=False
  
print(main_f.count())
main_f.createOrReplaceTempView('main_f')

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(main, target_table, key_columns, options = {'auto_merge': True})

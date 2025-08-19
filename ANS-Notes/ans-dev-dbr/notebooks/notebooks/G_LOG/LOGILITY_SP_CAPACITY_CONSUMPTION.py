# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['LOC_ID','PROD_RSRC_ID','ITEM_ID','PLND_PROD_DATE','PLND_ORD_QTY','GenerationDate'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'logility_sp_capacity_consumption')
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
currenttime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
source_table = 'logftp.logility_sp_production_order_export' 
viewName = 'logility_sp_production_order_export'

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)
  print(cutoff_value)
  logility_sp_prod_order_extract = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_sp_prod_order_extract = load_full_dataset(source_table)

logility_sp_prod_order_extract.createOrReplaceTempView(viewName)
# logility_sp_prod_order_extract.display()

# COMMAND ----------

main = spark.sql(f"""
WITH cte
(
         select   location_id,
                  production_resource,
                  max(IF(rnk=1,nvl(secondary_resource,''),'')) secondaryres,
                  max(IF(rnk=2,nvl(secondary_resource,''),'')) secsecondaryres                  
         FROM    (
                           SELECT   *,
                                    dense_rank() OVER (PARTITION BY Production_Resource ORDER BY secondary_resource) rnk
                           FROM     g_tembo.secondary_resource_link
                           )
         GROUP BY location_id, production_resource
)
,srl
(
select location_id,production_resource,
case when secondaryres=='' and secsecondaryres <> '' then secsecondaryres else secondaryres end as secondaryres,
case when secondaryres=='' and secsecondaryres <> '' then null else secsecondaryres end as secsecondaryres
from cte 
)
SELECT DISTINCT LOC_ID,
          PROD_RSRC_ID,
          srl.secondaryres    AS SecondaryRes,
          srl.secsecondaryres AS SecSecondaryRes,
          ITEM_ID,
          to_date(replace(PLND_PROD_DATE,':000000000',''),'yyyyMMdd') as PLND_PROD_DATE,
          PLND_ORD_QTY,
          nvl(itemloc.units_per_hour,0.0)                   AS Units_per_hours_primary,
          cast(nvl(plnd_ord_qty*itemloc.units_per_hour,0.0) as decimal(38,31))      AS Capacity_consumed_primary,
          nvl(itemloc_sec.units_per_hour,0.0)               AS Units_per_hours_secondary,
          cast(nvl(plnd_ord_qty*itemloc_sec.units_per_hour,0.0) as decimal(38,31))  AS Capacity_consumed_secondary,
          nvl(itemloc_ssec.units_per_hour,0.0)              AS Units_per_hours_sec_secondary,
          cast(nvl(plnd_ord_qty*itemloc_ssec.units_per_hour,0.0) as decimal(38,31)) AS Capacity_consumed_sec_secondary,
          GenerationDate,
          cast('{currenttime}' as timestamp) AS _MODIFIED
FROM      {viewName} prdorder
LEFT JOIN srl ON prdorder.prod_rsrc_id = srl.production_resource AND prdorder.loc_id = srl.location_id
LEFT JOIN g_tembo.sps_itemloc_prod_conv itemloc ON itemloc.item = prdorder.item_id AND itemloc.production_resource = prdorder.prod_rsrc_id
LEFT JOIN g_tembo.sps_itemloc_prod_conv itemloc_sec ON itemloc_sec.item = prdorder.item_id AND itemloc_sec.production_resource = srl.secondaryres
LEFT JOIN g_tembo.sps_itemloc_prod_conv itemloc_ssec ON itemloc_ssec.item = prdorder.item_id AND itemloc_ssec.production_resource = srl.secsecondaryres
WHERE     LEFT(loc_id,2) IN ('PL', 'EM')
UNION ALL
SELECT LOC_ID,
       PROD_RSRC_ID,
       NULL AS SecondaryRes,
       NULL AS SecSecondaryRes,
       ITEM_ID,
      to_date(replace(PLND_PROD_DATE,':000000000',''),'yyyyMMdd') as PLND_PROD_DATE,
       PLND_ORD_QTY,
       NULL AS Units_per_hours_primary,
       NULL AS Capacity_consumed_primary,
       NULL AS Units_per_hours_secondary,
       NULL AS Capacity_consumed_secondary,
       NULL AS Units_per_hours_sec_secondary,
       NULL AS Capacity_consumed_sec_secondary,
       GenerationDate,
       cast('{currenttime}' as timestamp) AS _MODIFIED
FROM   {viewName} prdorder
WHERE  LEFT(loc_id,2) NOT IN ('PL', 'EM')


""")
# main.display()
main.createOrReplaceTempView("main")

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
append_into_table(main, target_table, {"incremental_column":"GenerationDate"})

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, '_MODIFIED')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)

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
key_columns = get_input_param('key_columns', 'list', default_value = ['GroupingID','ITEM','NEW_DATE'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 5)
table_name = get_input_param('table_name', default_value = 'po_logility_ebs')
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

ebsProduct = spark.sql("""SELECT itemId,e2eproductcode,ansstduom,primaryuomconv,
                         CASE
                         WHEN ansstduom IN ('PAIR','PR') THEN primaryuomconv/2
                         WHEN ansstduom IN ('PIECE','PC') THEN primaryuomconv
                        END AS primaryuomconv_der,
                        CASE
                               WHEN ansstduom IN ('PAIR','PR') THEN 'PR'
                               WHEN ansstduom IN ('PIECE','PC') THEN 'PC'
                               ELSE ansstduom
                        END AS uom
                      FROM s_core.product_agg WHERE _source='EBS'""")
ebsProduct.createOrReplaceTempView('ebsProduct')
# ebsProduct.display()

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
              case when Shipment_ID is null then Order_ID else Shipment_ID end ShipmentGrp,
              _SOURCEFILENAME

       FROM   logility_sp_purchase_orders_export
        WHERE     source_location not like 'DC%'
                 AND location_dc not like 'DS%'
) 
,cte1
(
         SELECT   md5(concat_ws(location_dc,source_location,logility_user_id,ShipmentGrp)) as groupingid,
                  --rank() OVER(ORDER BY location_dc,source_location,logility_user_id,ShipmentGrp) groupingid,         
                  item,
                  location_dc,
                  source_location,
                  need_by_date,
                  quantity,
                  uom,
                  logility_user_id,
                  planner_id,
                  order_id,
                  _SOURCEFILENAME
         FROM     cte ) 
,cte2
(
       SELECT groupingid,              
              max(need_by_date) need_by_date                          
       FROM   cte1 
       GROUP BY groupingid               
)

SELECT   cte1.groupingid,
         cte1.item,
         cte1.location_dc,
         cte1.source_location,
         cte2.need_by_date,
         Sum(cte1.quantity) Quantity,
         cte1.uom,
         cte1.planner_id,
         cte1.order_id,
         cte1.logility_user_id,
         cte1._SOURCEFILENAME
FROM     cte1
INNER JOIN cte2 on cte1.groupingid = cte2.groupingid
GROUP BY cte1.groupingid,
         cte1.item,
         cte1.location_dc,
         cte1.source_location,
         cte2.need_by_date,
         cte1.uom,
         cte1.planner_id,
         cte1.order_id,
         cte1.logility_user_id,
         cte1._SOURCEFILENAME
""")
# pogrouping.display()
pogrouping.createOrReplaceTempView('pogrouping')

# COMMAND ----------

pogrouping1=spark.sql("""
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
lm.InventoryHoldingERP 
from 
pogrouping po left join spt.locationmaster lm on lm.GlobalLocationCode = po.Location_DC
WHERE InventoryHoldingERP = 'EBS'
""")

# pogrouping1.display()
pogrouping1.createOrReplaceTempView('pogrouping1')

# COMMAND ----------

po_vendors=spark.sql("""
select msi.segment1 as Item, asu.vendor_name, asu.vendor_id, asa.vendor_site_id, asa.vendor_site_code  
        FROM ebs.PO_HEADERS_ALL POH  
        JOIN       ebs.PO_LINES_ALL PLL on POH.PO_HEADER_ID = PLL.PO_HEADER_ID           
        LEFT JOIN  ebs.mtl_system_items_b msi ON PLL.item_id = msi.inventory_item_id 
        --LEFT JOIN  ebs.mtl_parameters mtlp on msi.organization_id = mtlp.organization_id
        LEFT JOIN  ebs.ap_suppliers asu ON asu.vendor_id = POH.vendor_id
        LEFT JOIN  ebs.ap_supplier_sites_all asa ON POH.vendor_id = asa.vendor_id and POH.vendor_site_id = asa.vendor_site_id        
        WHERE  msi.organization_id = 124
        AND    POH.type_lookup_code = 'BLANKET'
        AND    nvl(POH.cancel_flag, 'N') <> 'Y' AND nvl(PLL.cancel_flag,'N') <> 'Y'
        AND CURRENT_TIMESTAMP() BETWEEN POH.start_date AND nvl(POH.end_date,'2999-01-01')
"""
)

# po_vendors.display()
po_vendors.createOrReplaceTempView('po_vendors')

# COMMAND ----------

main=spark.sql("""
WITH cte
(
                select distinct groupingid,
                                replace(location_dc,'DC','') org_code,
                                po.item,
                                'Y'                                                  for_release,
                                'Planned Order'                                      order_type_code,
                                date_format(need_by_date,'MM/dd/y')                  new_date,
                                cast(round(quantity/pr.primaryuomconv_der) as int) AS new_quantity,
                                cast(NULL AS string)                                 po_num,
                                case when source_location like 'PL%' or source_location like 'EM%' then cast(null as string) 
                                else source_location end as  source_org,
                                supp.vendor_name  as source_supplier,
                                supp.vendor_site_code as source_supplier_site                                
                                ,USER.login AS oracle_user_id
                                ,_SOURCEFILENAME,
                                RANK() OVER (PARTITION BY po.groupingid,location_dc,po.item,supp.vendor_id ORDER BY supp.vendor_site_id) rnk
                FROM       pogrouping1 po
                INNER JOIN      spt.locationmaster lm
                ON              lm.globallocationcode = po.source_location
                INNER JOIN      ebsProduct pr ON pr.e2eproductcode = po.item AND pr.uom = po.uom
                LEFT JOIN      po_vendors supp
                ON              supp.vendor_id = lm.vendor_id and supp.item = po.item                
                LEFT JOIN
                                (
                                       SELECT replace(internalemailaddress, substr(internalemailaddress, charindex('@', internalemailaddress)), '') internalemailaddress,
                                              login
                                       FROM   s_core.user_agg
                                       WHERE  _source = 'EBS') USER
                ON              USER.internalemailaddress = po.logility_user_id                
)
SELECT   
groupingid,
         org_code,
         item,
         for_release,
         order_type_code,
         new_date,
         Sum(new_quantity) NEW_QUANTITY,
         po_num,
         source_org,
         source_supplier,
         source_supplier_site,
         oracle_user_id,
         _SOURCEFILENAME,
         'N' as Processed
FROM cte  
WHERE rnk =1 and oracle_user_id is not null
GROUP BY groupingid,
         org_code,
         item,
         for_release,
         order_type_code,
         new_date,
         po_num,
         source_org,
         source_supplier,
         source_supplier_site,
         oracle_user_id,
         _SOURCEFILENAME,
         Processed
""")

# main.display()
main.createOrReplaceTempView('po_ebs')

# COMMAND ----------

if not table_exists(target_table) or overwrite==True:
  print("EBS PO Table not exists")
  query = "SELECT * FROM po_ebs"
  main_f = spark.sql(query)
  overwrite=True   

else:
  print("EBS PO Table exists")
  query = f""" 
SELECT *
FROM   po_ebs a
WHERE  NOT EXISTS (SELECT groupingid
                   FROM   {target_table} b
                   WHERE  a.groupingid = b.groupingid                           
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
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# Databricks notebook source
# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

ENV_NAME = os.getenv('ENV_NAME')
ENV_NAME = ENV_NAME.upper()
ENV_NAME

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['SAP_Sales_Order','SAP_Sales_Order_Line'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 10)
table_name = get_input_param('table_name', default_value = 'logility_drop_ship_sap')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name,table_name,None)
target_table_agg = table_name[:table_name.rfind("_")] + '_agg'
currentuser = str(spark.sql("select current_user()").collect()[0][0])
currenttime = datetime.now()
source = "LOG"
source_table = 'logftp.logility_sp_customer_orders_export'

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)  
  print(cutoff_value)  
  source_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  source_df = load_full_dataset(source_table)

source_df.createOrReplaceTempView('logility_sp_customer_orders_export')
# source_df.display()

# COMMAND ----------

drop_ship = spark.sql("""                
with cte(
select CO_Order_Number,max(to_date(replace(CO_Fulfilment_Date,':000000000',''),'yyyyMMdd')) CO_Fulfilment_Date from logility_sp_customer_orders_export group by CO_Order_Number
  )
select ds.Item,
ds.Location,
ds.Customer,
ds.CO_Order_Number,
ds.CO_Line_Number,
so.salesorderid,
so.salesorderdetailid,
to_date(replace(ds.CO_Requested_Date,':000000000',''),'yyyyMMdd') CO_Requested_Date,
cte.CO_Fulfilment_Date,
ds.OrderQuantity,
COALESCE(so._SOURCE,sh._SOURCE) as _SOURCE
from logility_sp_customer_orders_export ds 
join cte on ds.CO_Order_Number=cte.CO_Order_Number
left join (select ordernumber, salesorderid,_SOURCE from s_supplychain.sales_order_headers_sap) sh on ds.CO_Order_Number=sh.ordernumber
left join (select ordernumber,cast(linenumber as int) linenumber,salesorderid,salesorderdetailid,orderLineStatus,_SOURCE from s_supplychain.sales_order_lines_sap) so on ds.CO_Order_Number=so.ordernumber and ds.CO_Line_Number=so.linenumber   --linenumber || '.' || sequencenumber
""")
# drop_ship.display()
drop_ship.createOrReplaceTempView('drop_ship')

# COMMAND ----------

# DBTITLE 1,Get unique rows from VBEP
vbep = spark.sql("""
with cte(
select VBELN,POSNR,BANFN,ETENR,rank() OVER(PARTITION BY VBELN,POSNR ORDER BY ETENR) rnk from sapp01.vbep
)
select VBELN,POSNR,BANFN from cte where rnk=1
""")
vbep.createOrReplaceTempView('vbep')
# vbep.display()

# COMMAND ----------

# DBTITLE 1,Get unique rows from EBAN
eban = spark.sql("""
with cte(
select BANFN,FRGKZ,FRGST,rank() OVER(PARTITION BY BANFN ORDER BY BNFPO) rnk from sapp01.eban
)
select BANFN,FRGKZ,FRGST from cte where rnk=1
""")
eban.createOrReplaceTempView('eban')
# eban.display()

# COMMAND ----------

LOGILITY_DROP_SHIP_SAP = spark.sql(""" 
select 
CO_Order_Number SAP_Sales_Order,
CO_Line_Number SAP_Sales_Order_Line,
Item Item_Number,
CO_Fulfilment_Date Fulfilment_Date,
vbep.BANFN as PReqNumber,
eban.FRGKZ as BlockIndicator,
eban.FRGST as HoldType,
'N' as Processed
from
drop_ship
left join vbep on vbep.VBELN = CO_Order_Number and vbep.POSNR = CO_Line_Number
left join eban on eban.BANFN = vbep.BANFN
WHERE _SOURCE = 'SAP'
and CO_Fulfilment_Date is not null
""")

LOGILITY_DROP_SHIP_SAP.createOrReplaceTempView('LOGILITY_DROP_SHIP_SAP')
# LOGILITY_DROP_SHIP_SAP.display()

# COMMAND ----------

if not table_exists(target_table) or overwrite==True:
  print("SAP Table do not exists")
  query = "SELECT * FROM LOGILITY_DROP_SHIP_SAP"
  overwrite=True   

else:
  print("SAP Table exists")
  query = f""" 
SELECT *
FROM   LOGILITY_DROP_SHIP_SAP a
WHERE  NOT EXISTS (SELECT SAP_Sales_Order,
                          SAP_Sales_Order_Line
                   FROM   {target_table} b
                   WHERE  a.SAP_Sales_Order = b.SAP_Sales_Order
                          AND a.SAP_Sales_Order_Line = b.SAP_Sales_Order_Line) 
    """
  overwrite=False

main = spark.sql(query)
print(main.count())
main.createOrReplaceTempView('main')
# main.display()

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(main, target_table, key_columns)

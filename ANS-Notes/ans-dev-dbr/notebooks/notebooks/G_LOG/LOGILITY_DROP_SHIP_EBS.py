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
key_columns = get_input_param('key_columns', 'list', default_value = ['EBS_Sales_Order','Line_Id','Batch_Id'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 10)
table_name = get_input_param('table_name', default_value = 'logility_drop_ship_ebs')
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
ol.salesorderid,
ol.salesorderdetailid,
to_date(replace(ds.CO_Requested_Date,':000000000',''),'yyyyMMdd') CO_Requested_Date,
cte.CO_Fulfilment_Date,
ds.OrderQuantity,
ol._SOURCE,ol.orderLineStatus
from logility_sp_customer_orders_export ds 
join cte on ds.CO_Order_Number=cte.CO_Order_Number
left join s_supplychain.sales_order_headers_ebs oh on ds.CO_Order_Number=oh.ordernumber
left join s_supplychain.sales_order_lines_ebs ol on ds.CO_Order_Number=ol.ordernumber and ds.CO_Line_Number=ol.linenumber and ol.productcode = ds.Item
where ol.orderLineStatus not in ('CANCELLED','CLOSED')
and (nvl(oh.orderHoldType,'NoHolds') in ('Logility Hold','NoHolds') and ol.orderLineHoldType = 'Logility Hold')
""")
# drop_ship.display()
drop_ship.createOrReplaceTempView('drop_ship')

# COMMAND ----------

LOGILITY_DROP_SHIP_EBS = spark.sql(""" 
select 
CO_Order_Number EBS_Sales_Order,
salesorderid Sales_Order_Header,
CO_Line_Number Sales_Order_Line,
salesorderdetailid Line_Id,
Item Item_Number,
CO_Fulfilment_Date,
case when dayofweek(CO_Fulfilment_Date)=1 then date_add(CO_Fulfilment_Date,1) --day is Sunday
when dayofweek(CO_Fulfilment_Date)=7 then date_add(CO_Fulfilment_Date,2) --day is Sat
else CO_Fulfilment_Date
end as Fulfilment_Date,
date_format(current_date,'yyMMdd') Batch_Id,
'N' as Processed
from
drop_ship
WHERE _SOURCE = 'EBS'
and CO_Fulfilment_Date is not null
""")

LOGILITY_DROP_SHIP_EBS.createOrReplaceTempView('LOGILITY_DROP_SHIP_EBS')
# LOGILITY_DROP_SHIP_EBS.display()

# COMMAND ----------

# DBTITLE 1,Load into EBS target table
if not table_exists(target_table) or overwrite==True:
  print("EBS Table not exists")
  query = "SELECT EBS_Sales_Order,Sales_Order_Header,Sales_Order_Line,Line_Id,Item_Number,Fulfilment_Date,Batch_Id,Processed FROM LOGILITY_DROP_SHIP_EBS"
  main = spark.sql(query)
  overwrite=True   

else:
  print("EBS Table exists")
  query = f""" 
SELECT EBS_Sales_Order,Sales_Order_Header,Sales_Order_Line,Line_Id,Item_Number,Fulfilment_Date,Batch_Id,Processed
FROM   LOGILITY_DROP_SHIP_EBS a
WHERE  NOT EXISTS (SELECT EBS_Sales_Order,
                          Line_Id
                   FROM   {target_table} b
                   WHERE  a.EBS_Sales_Order = b.EBS_Sales_Order 
                          AND a.Line_Id = b.Line_Id
                          ) 
    """
  main = spark.sql(query)
  overwrite=False

print(main.count())
main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main, key_columns)

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite}) 
append_into_table(main, target_table)

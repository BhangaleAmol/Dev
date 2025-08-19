# Databricks notebook source
# EXTEND DATAFRAME OBJECT
def transform(self, f):
  return f(self)
DataFrame.transform = transform

# COMMAND ----------

from pyspark.sql.functions import expr
def map_ebs_transaction_type_group(column):
  def inner(df):
      df = df.withColumn(column,expr("CASE  " + 
                                       " when transactionTypeGroup like 'AIT_DIRECT SHIPMENT%' THEN 'Direct AIT Shipment' " +
                                       " when transactionTypeGroup like '%DIRECT SHIPMENT%' THEN 'Direct Shipment' " + 
                                       " when transactionTypeGroup like '%CONSIGNMENT%' THEN 'Consignment' " + 
                                       " when transactionTypeGroup like '%CREDIT MEMO%' THEN 'Credit Memo' " + 
                                       " when transactionTypeGroup like '%DROP SHIPMENT%' THEN 'Drop Shipment' " + 
                                       " when transactionTypeGroup like '%FREE OF CHARGE%' THEN 'Free Of Charge' " + 
                                       " when transactionTypeGroup like '%GTO%' THEN 'GTO' " + 
                                       " when transactionTypeGroup like '%INTERCOMPANY%' THEN 'Intercompany' " + 
                                       " when transactionTypeGroup like '%REPLACEMENT%' THEN 'Replacement' " + 
                                       " when transactionTypeGroup like '%SAMPLE RETURN%' THEN 'Sample Return' " + 
                                       " when transactionTypeGroup like '%RETURN%' THEN 'Return' " + 
                                       " when transactionTypeGroup like '%SAMPLE%' THEN 'Sample' " + 
                                       " when transactionTypeGroup like '%SERVICE%' THEN 'Service' " + 
                                       " when transactionTypeGroup like '%STANDARD%' THEN 'Standard' " + 
                                       " when transactionTypeGroup like '%CUSTOMER_SAFETY_STOCK%' THEN 'Safety Stock' " + 
                                       " when transactionTypeGroup like '%BLANKET SALES AGREEMENT%' then 'Blanket Sales Agreement' " + 
                                       " when transactionTypeGroup like '%BROKEN_CASE%' then 'Broken Case' " + 
                                       " when transactionTypeGroup like '%DIRECT_BILLING%' then 'Direct Billing Order' " + 
                                       " when transactionTypeGroup like '%DONATION%' then 'Donation Order' " + 
                                       " when transactionTypeGroup like '%INTERNAL%' then 'Internal Order' " + 
                                       " when transactionTypeGroup like '%MANUAL PRICEALLOWED%' then 'Manual PriceAllowed Order' " + 
                                       " when transactionTypeGroup like '%PREPAYMENT%' then 'Prepayment Order' " + 
                                       " when transactionTypeGroup like '%PRICE RELATED%' then 'Price related Order' " + 
                                       " when transactionTypeGroup like '%OVERSHIPMENT%' then 'Overshipment Adjustment' " + 
                                       " when transactionTypeGroup like '%CONSIGNMENT%' then 'Consignment' " + 
                                       " when transactionTypeGroup like '%DEBIT_MEMO%' then 'Debit Memo Order' " + 
                                       " when transactionTypeGroup like '%CREDIT_MEMO%' then 'Credit Memo Order' " + 
                                       " when transactionTypeGroup like '%SAFETY STOCK%' then 'Safety Stock' " + 
                                       " when transactionTypeGroup like '%RMA%' then 'RMA Order' " + 
                                       " when transactionTypeGroup like '%LOCAL_DROP_SHIP%' then 'Local Drop Ship' " + 
                                       " when transactionTypeGroup like '%NO_RECEIPT%' then 'No Receipt' " + 
                                       " when transactionTypeGroup like '%NO_SALES%' then 'No Sales' " + 
                                       " when transactionTypeGroup like '%BORDER_LINE%' then 'Border Line' " + 
                                       " when transactionTypeGroup like '%ENDUSER%' then 'Enduser Order' " + 
                                       " when transactionTypeGroup like '%E_COMMERCE ORDER%' then 'E-Commerce Order' " + 
                                       " when transactionTypeGroup like '%DROPSHIP%' then 'DropShip Customer' " + 
                                       " when transactionTypeGroup like '%RAW_MATERIAL_PROCURMENT%' then 'Raw Material Procurement' " + 
                                       " when transactionTypeGroup like '%DEMO BILL%' then 'Demo Bill Only' " + 
                                       " when transactionTypeGroup like '%SALES AGREEMENT%' then 'Sales Agreement' " + 
                                       " when transactionTypeGroup like '%DEMO_ORDER%' then 'Demo Order' " + 
                                       " when transactionTypeGroup like '%EVALUATION%' then 'Evaluation Order' " + 
                                       " when transactionTypeGroup like '%RM%' then 'RM Order' " + 
                                       " when transactionTypeGroup like '%WEBSTORE%' then 'Webstore Order' " + 
                                       " when transactionTypeGroup like '%CUST_SAF_STK%' then 'Customer Safety Stock' " + 
                                       " else transactionTypeGroup   " + 
                                     " end"))
      return df
  return inner

# COMMAND ----------

def map_e2eItemDesc(lookupDF,column):
  def inner(df):
    
    df = df.withColumn(column,f.regexp_replace(column,',5','.5'))\
           .withColumn(column,f.regexp_replace(column,',0',''))\
           .withColumn(column,f.regexp_replace(column,'[;)(*&^%$#@!©<>,’”/"]',""))\
           .withColumn(column,f.regexp_replace(column,"[\\r|\\n]",""))\
           .withColumn(column,f.trim(column))\
           .withColumn(column,f.when(f.length(f.col(column)) > 36, f.regexp_replace(column,"Vend Pack|Vending Pack|VEND PACK","VP")).otherwise(f.col(column)))\
           .withColumn(column,f.when(f.length(f.col(column)) > 36, f.regexp_replace(column,"Size|size|SIZE",""))\
                               .otherwise(f.col(column)))\
           .withColumn(column,f.regexp_replace(column,"\s+"," "))
    
    for keyvalue, cvalue in lookupDF.collect():
      df = df.withColumn(column,f.when(f.length(f.col(column)) > 36, f.trim(f.regexp_replace(column,keyvalue,cvalue))).otherwise(f.col(column)))\
             .withColumn(column,f.regexp_replace(column,"\s+"," "))
           
    df = df.withColumn(column,f.when(f.length(f.col(column)) > 36, f.trim(f.col(column).substr(0,36))).otherwise(f.col(column)))
      
    return df
  return inner

# COMMAND ----------



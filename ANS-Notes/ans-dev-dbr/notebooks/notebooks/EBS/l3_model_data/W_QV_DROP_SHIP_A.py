# Databricks notebook source
# MAGIC %run ../SHARED/bootstrap

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# Get parameters
dbutils.widgets.text("source_folder", "","")
sourceFolder = getArgument("source_folder")
sourceFolder = '/datalake/EBS/raw_data/delta_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("source_folder_full", "","")
sourceFolderFull = getArgument("source_folder_full")
sourceFolderFull = '/datalake/EBS/raw_data/full_data' if sourceFolderFull == '' else sourceFolderFull

dbutils.widgets.text("target_folder", "","")
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/EBS/stage_data' if targetFolder == '' else targetFolder

dbutils.widgets.text("table_name", "","")
tableName = getArgument("table_name")
tableName = 'W_QV_DROP_SHIP_A' if tableName == '' else tableName

# COMMAND ----------

# Create paths
sourceFolderUrl = DATALAKE_ENDPOINT  + sourceFolder + '/'
targetFileUrl = DATALAKE_ENDPOINT +  targetFolder + '/' + tableName + '.par'

print(sourceFolderUrl)
print(targetFileUrl)

# COMMAND ----------

# INCREMENTAL DATASET
sourceFileUrl = sourceFolderUrl + 'apps.ams_categories_b.par'
AMS_CATEGORIES_B = spark.read.parquet(sourceFileUrl)
AMS_CATEGORIES_B.createOrReplaceTempView("AMS_CATEGORIES_B")

# FULL DATASETS
OE_ORDER_LINES_ALL = spark.table('ebs.oe_order_lines_all')
OE_ORDER_LINES_ALL.createOrReplaceTempView("OE_ORDER_LINES_ALL")

OE_ORDER_HEADERS_ALL = spark.table('ebs.oe_order_headers_all')
OE_ORDER_HEADERS_ALL.createOrReplaceTempView("OE_ORDER_HEADERS_ALL")

OE_ORDER_HOLDS_ALL = spark.table('ebs.oe_order_holds_all')
OE_ORDER_HOLDS_ALL.createOrReplaceTempView("OE_ORDER_HOLDS_ALL")

OE_DROP_SHIP_SOURCES = spark.table('ebs.oe_drop_ship_sources')
OE_DROP_SHIP_SOURCES.createOrReplaceTempView("OE_DROP_SHIP_SOURCES")

HZ_CUST_ACCOUNTS = spark.table('ebs.hz_cust_accounts')
HZ_CUST_ACCOUNTS.createOrReplaceTempView("HZ_CUST_ACCOUNTS")

OE_HOLD_SOURCES_ALL = spark.table('ebs.oe_hold_sources_all')
OE_HOLD_SOURCES_ALL.createOrReplaceTempView("OE_HOLD_SOURCES_ALL")

OE_HOLD_DEFINITIONS = spark.table('ebs.oe_hold_definitions')
OE_HOLD_DEFINITIONS.createOrReplaceTempView("OE_HOLD_DEFINITIONS")

HZ_CUST_ACCT_SITES_ALL = spark.table('ebs.hz_cust_acct_sites_all')
HZ_CUST_ACCT_SITES_ALL.createOrReplaceTempView("HZ_CUST_ACCT_SITES_ALL")

HZ_PARTIES = spark.table('ebs.hz_parties')
HZ_PARTIES.createOrReplaceTempView("HZ_PARTIES")

MTL_PARAMETERS = spark.table('ebs.mtl_parameters')
MTL_PARAMETERS.createOrReplaceTempView("MTL_PARAMETERS")

HR_ORGANIZATION_UNITS = spark.table('ebs.hr_organization_units')
HR_ORGANIZATION_UNITS.createOrReplaceTempView("HR_ORGANIZATION_UNITS")

MTL_RESERVATIONS = spark.table('ebs.mtl_reservations')
MTL_RESERVATIONS.createOrReplaceTempView("MTL_RESERVATIONS")

HZ_PARTY_SITES = spark.table('ebs.hz_party_sites')
HZ_PARTY_SITES.createOrReplaceTempView("HZ_PARTY_SITES")

HZ_LOCATIONS = spark.table('ebs.hz_locations')
HZ_LOCATIONS.createOrReplaceTempView("HZ_LOCATIONS")

HZ_CUST_SITE_USES_ALL = spark.table('ebs.hz_cust_site_uses_all')
HZ_CUST_SITE_USES_ALL.createOrReplaceTempView("HZ_CUST_SITE_USES_ALL")

PO_LINE_LOCATIONS_ALL = spark.table('ebs.po_line_locations_all')
PO_LINE_LOCATIONS_ALL.createOrReplaceTempView("PO_LINE_LOCATIONS_ALL")

OE_TRANSACTION_TYPES_TL = spark.table('ebs.oe_transaction_types_tl')
OE_TRANSACTION_TYPES_TL.createOrReplaceTempView("OE_TRANSACTION_TYPES_TL")

PO_HEADERS_ALL = spark.table('ebs.po_headers_all')
PO_HEADERS_ALL.createOrReplaceTempView("PO_HEADERS_ALL")

MTL_SYSTEM_ITEMS_B = spark.table('ebs.mtl_system_items_b')
MTL_SYSTEM_ITEMS_B.createOrReplaceTempView("MTL_SYSTEM_ITEMS_B")

RA_CUSTOMER_TRX_LINES_ALL = spark.table('ebs.ra_customer_trx_lines_all')
RA_CUSTOMER_TRX_LINES_ALL.createOrReplaceTempView("RA_CUSTOMER_TRX_LINES_ALL")

RA_CUSTOMER_TRX_ALL = spark.table('ebs.ra_customer_trx_all')
RA_CUSTOMER_TRX_ALL.createOrReplaceTempView("RA_CUSTOMER_TRX_ALL")

RCV_SHIPMENT_LINES = spark.table('ebs.rcv_shipment_lines')
RCV_SHIPMENT_LINES.createOrReplaceTempView("RCV_SHIPMENT_LINES")

RCV_SHIPMENT_HEADERS = spark.table('ebs.rcv_shipment_headers')
RCV_SHIPMENT_HEADERS.createOrReplaceTempView("RCV_SHIPMENT_HEADERS")

XX_INV_UTI_CONTRAINER = spark.table('ebs.xx_inv_uti_contrainer')
XX_INV_UTI_CONTRAINER.createOrReplaceTempView("XX_INV_UTI_CONTRAINER")

XX_INV_ACS_CONTAINER = spark.table('ebs.xx_inv_acs_container')
XX_INV_ACS_CONTAINER.createOrReplaceTempView("XX_INV_ACS_CONTAINER")

# COMMAND ----------

from pyspark import StorageLevel

# COMMAND ----------

RSH_FIRST_CETD = spark.sql("""
  SELECT 
    COALESCE(rsl.secondary_quantity_shipped, -999) secondary_quantity,rsl.last_update_date,
    ods.header_id,
    ods.line_id, 
    MAX(DATE_FORMAT(rsh.expected_receipt_date, 'yyyy-MMM-dd')) rsh_first_cetd
  FROM OE_DROP_SHIP_SOURCES ods
  INNER JOIN RCV_SHIPMENT_LINES rsl ON rsl.po_header_id = ods.po_header_id and rsl.po_line_id  = ods.po_line_id
  LEFT JOIN RCV_SHIPMENT_HEADERS rsh ON rsh.shipment_header_id =  rsl.shipment_header_id
  LEFT JOIN PO_HEADERS_ALL pha ON pha.po_header_id = rsl.po_header_id
  GROUP BY
    COALESCE(rsl.secondary_quantity_shipped, -999),
    rsl.last_update_date,
    ods.header_id,
    ods.line_id
""")

RSH_FIRST_CETD.createOrReplaceTempView("RSH_FIRST_CETD")
RSH_FIRST_CETD.persist(StorageLevel.DISK_ONLY)
RSH_FIRST_CETD.count()

# COMMAND ----------

PLL_FIRST_CETD = spark.sql("""
  SELECT
    ods.line_id,
    MAX(DATE_FORMAT(pll.promised_date, 'yyyy-MMM-dd')) pll_first_cetd
  FROM OE_DROP_SHIP_SOURCES ods
  INNER JOIN PO_LINE_LOCATIONS_ALL pll ON pll.po_line_id = ods.po_line_id AND pll.line_location_id = ods.line_location_id
  GROUP BY ods.line_id
""")

PLL_FIRST_CETD.createOrReplaceTempView("PLL_FIRST_CETD")
PLL_FIRST_CETD.persist(StorageLevel.DISK_ONLY)
PLL_FIRST_CETD.count()

# COMMAND ----------

AMNT_DUE = spark.sql("""
  SELECT
    rct.org_id,
    rct.BILL_TO_CUSTOMER_ID,
    rctl.interface_line_attribute6,
    rctl.interface_line_attribute1,
    MAX(rct.trx_number) AR_INVOICE_NUM, 
    MAX(DATE_FORMAT(rct.trx_date, 'yyyy-MMM-dd')) AR_INVOICE_Date,
    SUM(rctl.QUANTITY_INVOICED * COALESCE(rctl.UNIT_SELLING_PRICE, 0)) quantity_val
  FROM 
    RA_CUSTOMER_TRX_LINES_ALL rctl, 
    RA_CUSTOMER_TRX_ALL rct
  WHERE 
    rct.org_id = rctl.org_id 
    AND rct.customer_trx_id = rctl.customer_trx_id
    AND rct.interface_header_attribute1 = rctl.interface_line_attribute1
  GROUP BY
    rct.org_id,
    rct.BILL_TO_CUSTOMER_ID,
    rctl.interface_line_attribute6,
    rctl.interface_line_attribute1
""")

AMNT_DUE.createOrReplaceTempView("AMNT_DUE")
AMNT_DUE.persist(StorageLevel.DISK_ONLY)
AMNT_DUE.count()

# COMMAND ----------

LATEST_CARRIER_DATE = spark.sql("""
  SELECT 
    odss.line_id,
    COALESCE(dc_date, egd_date) LATEST_CARRIER_DATE,
    ROW_NUMBER() OVER(PARTITION BY odss.line_id ORDER BY COALESCE(dc_date, egd_date)) rownumber
   FROM XX_INV_ACS_CONTAINER xiac
   INNER JOIN RCV_SHIPMENT_HEADERS rsh ON xiac.container = COALESCE (rsh.attribute1, '~!@#$%^^&*()_+_=-')
   INNER JOIN RCV_SHIPMENT_LINES rsl ON rsh.shipment_header_id = rsl.shipment_header_id
   INNER JOIN OE_DROP_SHIP_SOURCES odss ON rsl.po_line_id = odss.po_line_id
""")

LATEST_CARRIER_DATE.createOrReplaceTempView("LATEST_CARRIER_DATE")
LATEST_CARRIER_DATE.persist(StorageLevel.DISK_ONLY)
LATEST_CARRIER_DATE.count()      

# COMMAND ----------

PO_NUM = spark.sql("""
  SELECT
    ods.line_id,
    MAX(poh.segment1) po_num
  FROM OE_DROP_SHIP_SOURCES ods
  INNER JOIN PO_HEADERS_ALL poh ON ods.po_header_id = poh.po_header_id
  GROUP BY ods.line_id
""")

PO_NUM.createOrReplaceTempView("PO_NUM")
PO_NUM.persist(StorageLevel.DISK_ONLY)
PO_NUM.count()  

# COMMAND ----------

W_QV_DROP_SHIP_A = spark.sql("""
  SELECT 
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') CREATED_ON_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') CHANGED_ON_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') INSERT_DT,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd hh:mm:ss.SSS') UPDATE_DT,
    'EBS' DATASOURCE_NUM_ID,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddhhmmssSSS') LOAD_BATCH_ID,
    'N' DELETE_FLG,
    OEH.ORDER_NUMBER ORDER_ID,
    CONCAT(INT(COALESCE(OEL.LINE_NUMBER, '')), '.', INT(COALESCE(OEL.SHIPMENT_NUMBER, ''))) LINE,
    OEL.LINE_ID,
    OEL.HEADER_ID,
    MSI.SEGMENT1 ITEM,
    MSI.DESCRIPTION ITEM_DESCRIPTION,
    OEL.ORDERED_QUANTITY ORDERED_QTY,
    OEL.SHIPPED_QUANTITY SHIPPED_QTY,
    OEL.SHIPPING_QUANTITY_UOM SHIPPED_UOM,
    OEL.ORDER_QUANTITY_UOM ORDERED_QTY_UOM,
    PO_NUM.PO_NUM,
    MP.ORGANIZATION_CODE ORG,
    OEL.FLOW_STATUS_CODE STATUS,
    HCA.ACCOUNT_NAME CUSTOMER,
    OEH.CUST_PO_NUMBER,
    HL.CITY BILL_TO_CITY,
    HL.STATE BILL_TO_STATE,
    HL1.ADDRESS1 SHIP_TO_NAME,
    HL1.CITY SHIP_TO_CITY,
    HL1.STATE SHIP_TO_STATE,
    OEL.ORDERED_ITEM ORDERED_ITEM,
    OEL.UNIT_LIST_PRICE * OEL.ORDERED_QUANTITY ORDERED_QTY_TOTAL_PRICE,
    OEH.ORG_ID ORG_ID,
    AMNT_DUE.QUANTITY_VAL AMT_INVOICED,
    AMNT_DUE.AR_INVOICE_NUM AR_INVOICE_NUM,
    AMNT_DUE.AR_INVOICE_DATE AR_INVOICE_DATE,
    "Null" CONTAINER_NUM,
    "Null" ETA_DATE,
    --cat2.segment1 GBU,
    CASE 
      WHEN (OEL.UNIT_LIST_PRICE * OEL.ORDERED_QUANTITY) - AMNT_DUE.QUANTITY_VAL = 0 THEN 'Null'
      ELSE (OEL.UNIT_LIST_PRICE * OEL.ORDERED_QUANTITY) - AMNT_DUE.QUANTITY_VAL 
    END AMNT_DUE,
    COALESCE(RFC.RSH_FIRST_CETD, PFC.PLL_FIRST_CETD) FIRST_CETD,
    DATE_FORMAT(OEL.REQUEST_DATE, 'yyyy-MMM-dd') REQUEST_DATE,
    DATE_FORMAT(OEL.SCHEDULE_SHIP_DATE, 'yyyy-MMM-dd') SCHEDULE_SHIP_DATE,
    HL.ADDRESS1 BILL_TO_NAME,
    (OEL.PRICING_QUANTITY * OEL.UNIT_SELLING_PRICE) EXTENDED_PRICE, 
    OEL.REVREC_EXPIRATION_DAYS ACCEPTANCE_EXPIRE_DAYS,
    LCD.LATEST_CARRIER_DATE LATEST_CARRIER_DATE,
    (OEL.UNIT_SELLING_PRICE * OEL.ORDERED_QUANTITY) LINE_TOTAL
  FROM OE_ORDER_LINES_ALL OEL
  INNER JOIN OE_ORDER_HEADERS_ALL OEH ON OEH.HEADER_ID = OEL.HEADER_ID AND OEH.ORG_ID = OEL.ORG_ID
  INNER JOIN HZ_CUST_SITE_USES_ALL HSU ON HSU.SITE_USE_ID = OEL.INVOICE_TO_ORG_ID 
  INNER JOIN HZ_CUST_ACCT_SITES_ALL HCS ON HCS.CUST_ACCT_SITE_ID = HSU.CUST_ACCT_SITE_ID
  INNER JOIN HZ_CUST_ACCOUNTS HCA ON HCA.CUST_ACCOUNT_ID = HCS.CUST_ACCOUNT_ID
  INNER JOIN HZ_PARTIES HP ON HP.PARTY_ID = HCA.PARTY_ID
  INNER JOIN HZ_PARTY_SITES HPS ON HPS.PARTY_SITE_ID = HCS.PARTY_SITE_ID
  INNER JOIN HZ_LOCATIONS HL ON HL.LOCATION_ID = HPS.LOCATION_ID
  INNER JOIN OE_TRANSACTION_TYPES_TL OTT ON OTT.TRANSACTION_TYPE_ID = OEH.ORDER_TYPE_ID
  INNER JOIN HZ_CUST_SITE_USES_ALL HSU1 ON HSU1.SITE_USE_ID =  OEL.SHIP_TO_ORG_ID
  INNER JOIN HZ_CUST_ACCT_SITES_ALL HCS1 ON HCS1.CUST_ACCT_SITE_ID = HSU1.CUST_ACCT_SITE_ID
  INNER JOIN HZ_PARTY_SITES HPS1 ON HPS1.PARTY_SITE_ID = HCS1.PARTY_SITE_ID
  INNER JOIN HZ_LOCATIONS HL1 ON HL1.LOCATION_ID = HPS1.LOCATION_ID
  INNER JOIN MTL_SYSTEM_ITEMS_B MSI ON MSI.ORGANIZATION_ID = OEL.SHIP_FROM_ORG_ID AND MSI.INVENTORY_ITEM_ID = OEL.INVENTORY_ITEM_ID
  INNER JOIN MTL_PARAMETERS MP ON MP.ORGANIZATION_ID = MSI.ORGANIZATION_ID
  INNER JOIN HZ_CUST_ACCOUNTS HCA1 ON HCA1.CUST_ACCOUNT_ID = OEH.SOLD_TO_ORG_ID
  LEFT JOIN AMNT_DUE ON 
    AMNT_DUE.ORG_ID = OEL.ORG_ID 
    AND AMNT_DUE.BILL_TO_CUSTOMER_ID = OEH.SOLD_TO_ORG_ID 
    AND AMNT_DUE.INTERFACE_LINE_ATTRIBUTE6 = INT(OEL.LINE_ID)  
    AND AMNT_DUE.INTERFACE_LINE_ATTRIBUTE1 = INT(OEH.ORDER_NUMBER)  
  --LEFT JOIN MTL_ITEM_CATEGORIES_V cat2 ON 
  --  cat2.inventory_item_id = msi.inventory_item_id
  --  AND cat2.organization_id = mp.ORGANIZATION_id
  --  AND cat2.CATEGORY_SET_NAME = 'Division and Transfer Price'
  --  AND msi.INVENTORY_ITEM_STATUS_CODE = 'Active'
  --  AND msi.ITEM_TYPE IN ('FINISHED GOODS', 'ACCESSORIES')
  LEFT JOIN LATEST_CARRIER_DATE LCD ON LCD.LINE_ID = OEL.LINE_ID AND LCD.ROWNUMBER = 1
  LEFT JOIN RSH_FIRST_CETD RFC ON 
    RFC.SECONDARY_QUANTITY = COALESCE (OEL.FULFILLED_QUANTITY2, -999) 
    AND RFC.LAST_UPDATE_DATE > OEL.CREATION_DATE
    AND OEL.FULFILLMENT_DATE > RFC.LAST_UPDATE_DATE
    AND RFC.HEADER_ID = OEL.HEADER_ID
    AND RFC.LINE_ID = OEL.LINE_ID
  LEFT JOIN PLL_FIRST_CETD PFC ON PFC.LINE_ID = OEL.LINE_ID
  LEFT JOIN PO_NUM ON PO_NUM.LINE_ID = OEL.LINE_ID
  WHERE
    COALESCE(OEL.SOURCE_TYPE_CODE, '@') = 'EXTERNAL'
    AND OTT.LANGUAGE = 'US'
    AND HSU.SITE_USE_CODE = 'BILL_TO'
    AND HSU1.SITE_USE_CODE = 'SHIP_TO'
 -- ORDER BY 
 --   hca.account_name,
 --   oeh.cust_po_number,
 --   oeh.order_number,
 --   oel.line_number,
 --   oel.shipment_number
""")

W_QV_DROP_SHIP_A.createOrReplaceTempView("W_QV_DROP_SHIP_A")
W_QV_DROP_SHIP_A.cache()
W_QV_DROP_SHIP_A.count()

# COMMAND ----------

W_QV_DROP_SHIP_A.coalesce(10).write.format("parquet").mode("overwrite").save(targetFileUrl)

# COMMAND ----------

W_QV_DROP_SHIP_A.unpersist()

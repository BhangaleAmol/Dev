# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('ebs.mtl_onhand_quantities_detail')
main_inc_int = load_full_dataset('ebs.rcv_shipment_headers')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)
  main_inc_int = main_inc_int.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('onhand_quantities_detail_inc')
main_inc_int.createOrReplaceTempView('rcv_shipment_headers_inc')

# COMMAND ----------

TMP_MRPN = spark.sql("""
SELECT INVENTORY_ITEM_ID, 
  MAX(CROSS_REFERENCE) MRPN 
FROM EBS.MTL_CROSS_REFERENCES
WHERE CROSS_REFERENCE_TYPE = 'Legacy MRPN'
      AND ORGANIZATION_ID = 124
GROUP BY INVENTORY_ITEM_ID
""")

TMP_MRPN.createOrReplaceTempView("TMP_MRPN")

# COMMAND ----------

TMP_EXPIRATION_DATES = spark.sql("""
SELECT 
    MLN.INVENTORY_ITEM_ID,
    MLN.ORGANIZATION_ID,
    MLN.LOT_NUMBER,
    MAX(MLN.EXPIRATION_DATE)  EXPIRATION_DATE
  FROM 
    EBS.MTL_LOT_NUMBERS MLN
  GROUP BY 
      MLN.ORGANIZATION_ID,
      MLN.INVENTORY_ITEM_ID,
      MLN.LOT_NUMBER
""")
TMP_EXPIRATION_DATES.createOrReplaceTempView("TMP_EXPIRATION_DATES")

# COMMAND ----------

TMP_SGA_COST = spark.sql("""
SELECT 
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  LEGAL_ENTITY_ID,
  START_DATE,
  END_DATE,
  CMPNT_COST
FROM 
  EBS.CM_CMPT_DTL
  INNER JOIN EBS.GMF_PERIOD_STATUSES
    ON CM_CMPT_DTL.PERIOD_ID = GMF_PERIOD_STATUSES.PERIOD_ID
      AND CM_CMPT_DTL.COST_TYPE_ID = GMF_PERIOD_STATUSES.COST_TYPE_ID
WHERE COST_ANALYSIS_CODE = 'SGA'
  AND COST_CMPNTCLS_ID = 21 
""")
TMP_SGA_COST.createOrReplaceTempView("TMP_SGA_COST")

# COMMAND ----------

TMP_SEE_THRU_COST = spark.sql("""
SELECT 
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  LEGAL_ENTITY_ID,
  START_DATE,
  END_DATE,
  sum(CASE WHEN CM_CMPT_MST_B.COST_CMPNTCLS_CODE NOT IN (
        'DUTY',
        'DUTY-ADJ',
        'MEDICALTAX',
        'INFREIGHT',
        'INFREIGHT-ADJ',
        'SGA',
        'MUPADJ',
        'MARK UP',
        'SGA-ADJ'
      ) THEN CM_CMPT_DTL.CMPNT_COST END) STD_COST,
      sum(CASE WHEN CM_CMPT_MST_B.COST_CMPNTCLS_CODE IN ('INFREIGHT', 'INFREIGHT-ADJ') THEN CM_CMPT_DTL.CMPNT_COST END) FREIGHT_COST,
      sum(CASE WHEN CM_CMPT_MST_B.COST_CMPNTCLS_CODE  IN ('DUTY', 'DUTY-ADJ', 'MEDICALTAX') THEN CM_CMPT_DTL.CMPNT_COST END) DUTY_COST,
      sum(CASE WHEN CM_CMPT_MST_B.COST_CMPNTCLS_CODE  IN ('SGA', 'MUPADJ', 'MARK UP', 'SGA-ADJ') THEN CM_CMPT_DTL.CMPNT_COST END) MARKUP_COST
FROM 
  EBS.CM_CMPT_DTL
  INNER JOIN EBS.CM_CMPT_MST_B ON CM_CMPT_DTL.COST_CMPNTCLS_ID = CM_CMPT_MST_B.COST_CMPNTCLS_ID
  INNER JOIN EBS.GMF_PERIOD_STATUSES
    ON CM_CMPT_DTL.PERIOD_ID = GMF_PERIOD_STATUSES.PERIOD_ID
      AND CM_CMPT_DTL.COST_TYPE_ID = GMF_PERIOD_STATUSES.COST_TYPE_ID
      and CM_CMPT_DTL.COST_TYPE_ID = 1000
      GROUP BY INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  LEGAL_ENTITY_ID,
  START_DATE,
  END_DATE

""")
TMP_SEE_THRU_COST.createOrReplaceTempView("TMP_SEE_THRU_COST")

# COMMAND ----------

EXCH_RATE = spark.sql("""
 SELECT 
    GL_DAILY_RATES.FROM_CURRENCY, 
    GL_DAILY_RATES.TO_CURRENCY, 
    GL_DAILY_RATES.CONVERSION_TYPE, 
    CAST(DATE_FORMAT(GL_DAILY_RATES.CONVERSION_DATE,"yyyyMMdd") AS INT) CONVERSION_DATE,
    GL_DAILY_RATES.FROM_CURRENCY FROM_CURCY_CD,
    GL_DAILY_RATES.TO_CURRENCY TO_CURCY_CD,
    GL_DAILY_RATES.CONVERSION_TYPE RATE_TYPE,
    GL_DAILY_RATES.CONVERSION_DATE START_DT,
    DATE_ADD(GL_DAILY_RATES.CONVERSION_DATE,1) END_DT,
    GL_DAILY_RATES.CONVERSION_RATE EXCH_RATE,
    GL_DAILY_RATES.CONVERSION_DATE EXCH_DT
  FROM 
    EBS.GL_DAILY_RATES
    WHERE GL_DAILY_RATES.FROM_CURRENCY = 'USD'
    AND  UPPER(GL_DAILY_RATES.CONVERSION_TYPE) = 'CORPORATE'
""")

EXCH_RATE.createOrReplaceTempView("EXCH_RATE")

# COMMAND ----------

TEMBO_AVAIL_SUBINVENTORY_CODE_LKP = spark.sql("""
SELECT DISTINCT
    KEY_VALUE 
FROM SMARTSHEETS.EDM_Control_Table 
WHERE table_Id = 'TEMBO_AVAILABLE_SUBINVENTORY_CODES' 
""")
TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.createOrReplaceTempView("TEMBO_AVAIL_SUBINVENTORY_CODE_LKP")

# COMMAND ----------

main_intransit = spark.sql("""
SELECT
  REPLACE(STRING(INT(MAX(rlt.created_by))), ",", "") createdBy,
  MAX(rlt.creation_date) createdOn,
  REPLACE(STRING(INT(MAX(rlt.last_updated_by))), ",", "") modifiedBy,
  MAX(rlt.last_update_date) modifiedOn,
  CAST(
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP
  ) insertedOn,
  CAST(
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP
  ) updatedOn,
  SUM(case
    when (
      upper(RSL.Unit_of_measure) in ('PAIR')
      and msib.ATTRIBUTE15 = 'PR'
    )
    or (
      upper(RSL.Unit_of_measure) in ('PIECE')
      and msib.ATTRIBUTE15 = 'PC'
    ) then rsl.quantity_shipped - rsl.quantity_received
    when upper(RSL.Unit_of_measure) in ('CA', 'CASE')
    and msib.PRIMARY_UOM_CODE = 'CA' then round(
      (rsl.quantity_shipped - rsl.quantity_received) * msib.PUOM2STDUOM,
      2
    )
    when upper(RSL.Unit_of_measure) in ('CA', 'CASE')
    and msib.ATTRIBUTE15 = 'PR' then round(
      (rsl.quantity_shipped - rsl.quantity_received) * msib.PUOMCONV * 2,
      2
    )
    when upper(RSL.Unit_of_measure) in ('CA', 'CASE')
    and msib.ATTRIBUTE15 = 'PR' then round(
      (rsl.quantity_shipped - rsl.quantity_received) * msib.PUOMCONV,
      2
    )
    when (
      upper(RSL.Unit_of_measure) in ('PIECE')
      and msib.ATTRIBUTE15 = 'PR'
    ) then (rsl.quantity_shipped - rsl.quantity_received) / 2
  end ) ansStdQty,
  0 ansStdQtyAvailableToReserve,
  0 ansStdQtyAvailableToTransact,
  msib.ATTRIBUTE15 ansStdUomCode,
  LED.CURRENCY_CODE currency,
  '' dateReceived,
  msib.DUAL_UOM_CONTROL dualUOMControl,
  CASE WHEN LED.CURRENCY_CODE = 'USD' THEN 1 ELSE EXCH_RATE.EXCH_RATE END exchangeRate,
  UPPER(DATE.periodName) glPeriodCode,
  '' holdStatus,
  LAST_DAY(CURRENT_DATE -1) inventoryDate,
  REPLACE(STRING(INT(rsh.ship_to_org_id)), ",", "") inventoryWarehouseID,
  REPLACE(STRING(INT(rsl.item_id)), ",", "") itemId,
  CASE
    WHEN msib.LOT_CONTROL_CODE = 1 THEN 'No Control'
    WHEN msib.LOT_CONTROL_CODE = 2 THEN 'Full Control'
    ELSE 'N/A'
  END lotControlFlag,
  lot.EXPIRATION_DATE lotExpirationDate,
  rlt.lot_num lotNumber,
  NULL lotStatus,
  MAX(rlt.transaction_date) manufacturingDate,
  TMP_MRPN.MRPN,
  REPLACE(STRING(INT(HR_OPERATING_UNITS.ORGANIZATION_ID)), ",", "") owningBusinessUnitId,
  msib.ATTRIBUTE8 originId,
  SUM(case
    when (
      upper(RSL.Unit_of_measure) in ('CA', 'CASE')
      and msib.PRIMARY_UOM_CODE = 'CA'
    )
    or (
      upper(RSL.Unit_of_measure) in ('PAIR')
      and msib.PRIMARY_UOM_CODE = 'PR'
    )
    or (
      upper(RSL.Unit_of_measure) in ('PIECE')
      and msib.PRIMARY_UOM_CODE = 'PC'
    ) then round(rsl.quantity_shipped - rsl.quantity_received, 2)
    when (
      upper(RSL.Unit_of_measure) in ('CA', 'CASE')
      and msib.ATTRIBUTE15 = 'PR'
    ) then round(
      (rsl.quantity_shipped - rsl.quantity_received) / msib.PUOM2STDUOM,
      2
    )
    when (
      upper(RSL.Unit_of_measure) in ('PAIR')
      and msib.PRIMARY_UOM_CODE = 'CA'
    ) then round(
      (rsl.quantity_shipped - rsl.quantity_received) / msib.PUOMCONV * 2,
      2
    )
    when (
      upper(RSL.Unit_of_measure) in ('PIECE')
      and msib.PRIMARY_UOM_CODE = 'CA'
    ) then round(
      (rsl.quantity_shipped - rsl.quantity_received) / msib.PUOMCONV,
      2
    )
  end) primaryQty,
  0    primaryQtyAvailableToTransact,
  msib.PRIMARY_UOM_CODE primaryUOMCode,
  msib.segment1 productCode,
  0 secondaryQty,
  0 secondaryQtyAvailableToTransact,
  msib.SECONDARY_UOM_CODE secondaryUOMCode,
  COALESCE(TMP_SGA_COST.CMPNT_COST, 0) sgaCostPerUnitPrimary,
  msib.SHELF_LIFE_DAYS shelfLifeDays,
  COALESCE(TMP_COST.STD_COST, 0) + COALESCE(TMP_COST.DUTY_COST, 0) + COALESCE(TMP_COST.FREIGHT_COST, 0) stdCostPerUnitPrimary,
  'IN TRANSIT' subInventoryCode,
  COALESCE(GIC.ACCTG_COST, 0) totalCost
from
  rcv_shipment_headers_inc rsh
  join ebs.rcv_shipment_lines rsl on rsh.shipment_header_id = rsl.shipment_header_id
  join ebs.rcv_lot_transactions rlt on Rsl.Shipment_Line_Id = Rlt.Shipment_Line_Id
  join ebs.mtl_system_items_b msib on rsl.item_id = msib.inventory_item_id
  LEFT OUTER JOIN TMP_MRPN ON rsl.item_id = TMP_MRPN.INVENTORY_ITEM_ID
  join ebs.HR_ORGANIZATION_INFORMATION HOI on HOI.ORGANIZATION_ID = RSH.ORGANIZATION_ID
  join ebs.GL_LEDGERS LED on LED.LEDGER_ID = HOI.ORG_INFORMATION1
  join EBS.HR_OPERATING_UNITS on HOI.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
  join EBS.GL_SETS_OF_BOOKS on HR_OPERATING_UNITS.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  
   LEFT OUTER JOIN EXCH_RATE EXCH_RATE
    ON GL_SETS_OF_BOOKS.CURRENCY_CODE = EXCH_RATE.TO_CURCY_CD
      AND CURRENT_DATE-1  >= EXCH_RATE.START_DT
      AND CURRENT_DATE-1  < EXCH_RATE.END_DT
  LEFT JOIN S_CORE.DATE DATE ON DATE.DAYID = DATE_FORMAT(LAST_DAY(CURRENT_DATE -1), 'yyyyMMdd')
  left join EBS.MTL_LOT_NUMBERS LOT on rsl.item_id = lot.inventory_item_id
  and rsh.ship_to_org_id = lot.organization_id
  and rlt.lot_num = lot.Lot_Number
  LEFT OUTER JOIN TMP_SGA_COST ON rsh.ship_to_org_id = TMP_SGA_COST.ORGANIZATION_ID
  AND rsl.item_id = TMP_SGA_COST.INVENTORY_ITEM_ID
  AND CURRENT_DATE -1 BETWEEN TMP_SGA_COST.START_DATE
  AND TMP_SGA_COST.END_DATE
  LEFT OUTER JOIN TMP_SEE_THRU_COST TMP_COST ON rsh.ship_to_org_id = TMP_COST.ORGANIZATION_ID
  AND rsl.item_id = TMP_COST.INVENTORY_ITEM_ID
  AND CURRENT_DATE -1 BETWEEN TMP_COST.START_DATE
  AND TMP_COST.END_DATE
  LEFT JOIN EBS.GL_ITEM_CST GIC ON rsh.ship_to_org_id = GIC.ORGANIZATION_ID
  AND rsl.item_id = GIC.INVENTORY_ITEM_ID
  AND CURRENT_DATE -1 BETWEEN GIC.START_DATE
  AND GIC.END_DATE
  
  
where
  1 = 1
  And Rlt.Lot_Transaction_Type = 'SHIPMENT'
  AND rsl.shipment_line_status_code IN ('EXPECTED', 'PARTIALLY RECEIVED')
  and msib.organization_id = 124
  GROUP BY
  msib.ATTRIBUTE15 ,
  LED.CURRENCY_CODE ,
  msib.DUAL_UOM_CONTROL ,
  EXCH_RATE.EXCH_RATE ,
  UPPER(DATE.periodName) ,
  REPLACE(STRING(INT(rsh.ship_to_org_id)), ",", "") ,
  REPLACE(STRING(INT(rsl.item_id)), ",", "") ,
  CASE
    WHEN msib.LOT_CONTROL_CODE = 1 THEN 'No Control'
    WHEN msib.LOT_CONTROL_CODE = 2 THEN 'Full Control'
    ELSE 'N/A'
  END ,
  lot.EXPIRATION_DATE ,
  rlt.lot_num ,
  --rsl.shipment_line_status_code ,
  --rlt.transaction_date ,
  TMP_MRPN.MRPN,
  HR_OPERATING_UNITS.ORGANIZATION_ID ,
  msib.ATTRIBUTE8 ,
  msib.PRIMARY_UOM_CODE ,
  msib.segment1 ,
  msib.SECONDARY_UOM_CODE ,
  COALESCE(TMP_SGA_COST.CMPNT_COST, 0) ,
  msib.SHELF_LIFE_DAYS ,
  COALESCE(TMP_COST.STD_COST, 0) + COALESCE(TMP_COST.DUTY_COST, 0) + COALESCE(TMP_COST.FREIGHT_COST, 0) ,
  COALESCE(GIC.ACCTG_COST, 0) 
  """)
main_intransit.createOrReplaceTempView("main_intransit")

# COMMAND ----------

main_inventory = spark.sql("""
SELECT   
  0                                           createdBy,
  MIN(INVENTORY.CREATION_DATE)                createdOn,
  0                                           modifiedBy,
  MAX(INVENTORY.LAST_UPDATE_DATE)             modifiedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)     insertedOn,
  CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') as TIMESTAMP)     updatedOn,
  SUM(CASE  WHEN INVENTORY.IS_CONSIGNED=2 and  cast(MTL_SYSTEM_ITEMS.PUOM2STDUOM as decimal(22,7)) > 0
         THEN INVENTORY.PRIMARY_TRANSACTION_QUANTITY * MTL_SYSTEM_ITEMS.PUOM2STDUOM
         ELSE 0
  END)                                        ansStdQty,
  NULL                                        AS ansStdQtyAvailableToReserve,
  SUM(CASE  
          WHEN INVENTORY.IS_CONSIGNED=2 and  cast(MTL_SYSTEM_ITEMS.PUOM2STDUOM as decimal(22,7)) > 0
          THEN 
              CASE 
                WHEN INVENTORY.SUBINVENTORY_CODE = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
                THEN INVENTORY.PRIMARY_TRANSACTION_QUANTITY * MTL_SYSTEM_ITEMS.PUOM2STDUOM
                ELSE 0
              END 
          ELSE 0
      END)                                        AS ansStdQtyAvailableToTransact,
  MTL_SYSTEM_ITEMS.ATTRIBUTE15                ansStdUomCode,
  GL_SETS_OF_BOOKS.CURRENCY_CODE              currency,
  MAX(DATE(INVENTORY.DATE_RECEIVED))          dateReceived,
  MTL_SYSTEM_ITEMS.DUAL_UOM_CONTROL           dualUOMControl,
  CASE WHEN GL_SETS_OF_BOOKS.CURRENCY_CODE = 'USD' THEN 1 ELSE EXCH_RATE.EXCH_RATE END   exchangeRate,
  UPPER(DATE.periodName)            glPeriodCode,
  CASE WHEN TMP_EXPIRATION_DATES.EXPIRATION_DATE is not null 
     AND TMP_EXPIRATION_DATES.EXPIRATION_DATE <= add_months(current_date, 6)
         THEN 'Hold'
  END                                         holdStatus,
  LAST_DAY(CURRENT_DATE-1)                    inventoryDate, 
  REPLACE(STRING(INT(INVENTORY.ORGANIZATION_ID   )), ",", "")    inventoryWarehouseID,
  REPLACE(STRING(INT(INVENTORY.INVENTORY_ITEM_ID )), ",", "")    itemId,
  CASE 
      WHEN MTL_SYSTEM_ITEMS.LOT_CONTROL_CODE = 1 
        THEN 'No Control' 
      WHEN MTL_SYSTEM_ITEMS.LOT_CONTROL_CODE = 2 
        THEN 'Full Control' 
      ELSE 'N/A'
  END                                         lotControlFlag,
  TMP_EXPIRATION_DATES.EXPIRATION_DATE        lotExpirationDate,
  COALESCE(INVENTORY.LOT_NUMBER,'No Lot Number') lotNumber,
  NULL                                        AS lotStatus,
  DATE_ADD(TMP_EXPIRATION_DATES.EXPIRATION_DATE,  - int(shelf_life_days) )      
                                              manufacturingDate,
  TMP_MRPN.MRPN                               MRPN,
  REPLACE(STRING(INT(HR_OPERATING_UNITS.ORGANIZATION_ID)), ",", "")     owningBusinessUnitId,
  MTL_SYSTEM_ITEMS.ATTRIBUTE8                 originId,
  SUM(CASE  WHEN INVENTORY.IS_CONSIGNED=2
        THEN INVENTORY.PRIMARY_TRANSACTION_QUANTITY
        ELSE 0
  END)                                        primaryQty,
  SUM(CASE  
          WHEN INVENTORY.IS_CONSIGNED=2
          THEN
              CASE
                WHEN INVENTORY.SUBINVENTORY_CODE = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
                THEN INVENTORY.PRIMARY_TRANSACTION_QUANTITY
                ELSE 0
              END
          ELSE 0
      END)                                 as primaryQtyAvailableToTransact,
  MTL_SYSTEM_ITEMS.PRIMARY_UOM_CODE           primaryUOMCode,
  MTL_SYSTEM_ITEMS.SEGMENT1                   productCode,
  SUM(CASE  WHEN INVENTORY.IS_CONSIGNED=2
        THEN INVENTORY.SECONDARY_TRANSACTION_QUANTITY
        ELSE 0
  END)                                        secondaryQty,
  SUM(CASE  
          WHEN INVENTORY.IS_CONSIGNED=2
          THEN
              CASE
                WHEN INVENTORY.SUBINVENTORY_CODE = TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE
                THEN INVENTORY.SECONDARY_TRANSACTION_QUANTITY
                ELSE 0
              END
          ELSE 0
      END)                                  as secondaryQtyAvailableToTransact,
  MTL_SYSTEM_ITEMS.SECONDARY_UOM_CODE         secondaryUOMCode,
  COALESCE(TMP_SGA_COST.CMPNT_COST,0)         sgaCostPerUnitPrimary,
  MTL_SYSTEM_ITEMS.SHELF_LIFE_DAYS            shelfLifeDays,
  COALESCE(TMP_COST.STD_COST,0)+ COALESCE(TMP_COST.DUTY_COST,0) + COALESCE(TMP_COST.FREIGHT_COST,0)         stdCostPerUnitPrimary,
  INVENTORY.SUBINVENTORY_CODE                 subInventoryCode,
  COALESCE(GIC.ACCTG_COST ,0) totalCost
FROM
  onhand_quantities_detail_inc INVENTORY
  INNER JOIN EBS.MTL_SYSTEM_ITEMS_B MTL_SYSTEM_ITEMS
    ON  INVENTORY.INVENTORY_ITEM_ID = MTL_SYSTEM_ITEMS.INVENTORY_ITEM_ID
  LEFT OUTER JOIN TMP_MRPN
    ON INVENTORY.INVENTORY_ITEM_ID = TMP_MRPN.INVENTORY_ITEM_ID
  LEFT OUTER JOIN TMP_EXPIRATION_DATES
    ON INVENTORY.INVENTORY_ITEM_ID = TMP_EXPIRATION_DATES.INVENTORY_ITEM_ID
      AND INVENTORY.ORGANIZATION_ID = TMP_EXPIRATION_DATES.ORGANIZATION_ID
      AND INVENTORY.LOT_NUMBER = TMP_EXPIRATION_DATES.LOT_NUMBER
  INNER JOIN EBS.HR_ORGANIZATION_INFORMATION
    ON INVENTORY.ORGANIZATION_ID = HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID
  INNER JOIN EBS.HR_OPERATING_UNITS
    ON HR_ORGANIZATION_INFORMATION.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
  INNER JOIN EBS.GL_SETS_OF_BOOKS
    ON HR_OPERATING_UNITS.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT OUTER JOIN TMP_SEE_THRU_COST TMP_COST
    ON  INVENTORY.ORGANIZATION_ID = TMP_COST.ORGANIZATION_ID 
      AND INVENTORY.INVENTORY_ITEM_ID = TMP_COST.INVENTORY_ITEM_ID
      AND CURRENT_DATE -1 BETWEEN TMP_COST.START_DATE AND TMP_COST.END_DATE
  LEFT OUTER JOIN TMP_SGA_COST
    ON INVENTORY.ORGANIZATION_ID = TMP_SGA_COST.ORGANIZATION_ID 
      AND INVENTORY.INVENTORY_ITEM_ID = TMP_SGA_COST.INVENTORY_ITEM_ID
      AND CURRENT_DATE -1 BETWEEN TMP_SGA_COST.START_DATE AND TMP_SGA_COST.END_DATE
  LEFT OUTER JOIN EXCH_RATE EXCH_RATE
    ON GL_SETS_OF_BOOKS.CURRENCY_CODE = EXCH_RATE.TO_CURCY_CD
      AND CURRENT_DATE-1  >= EXCH_RATE.START_DT
      AND CURRENT_DATE-1  < EXCH_RATE.END_DT
   LEFT JOIN S_CORE.DATE DATE ON DATE.DAYID = DATE_FORMAT(LAST_DAY(CURRENT_DATE-1),'yyyyMMdd') 
   LEFT JOIN EBS.GL_ITEM_CST GIC   
   ON  INVENTORY.ORGANIZATION_ID= GIC.ORGANIZATION_ID 
   AND MTL_SYSTEM_ITEMS.INVENTORY_ITEM_ID= GIC.INVENTORY_ITEM_ID
   AND CURRENT_DATE -1 BETWEEN GIC.START_DATE AND GIC.END_DATE
   LEFT JOIN TEMBO_AVAIL_SUBINVENTORY_CODE_LKP
     ON TEMBO_AVAIL_SUBINVENTORY_CODE_LKP.KEY_VALUE = INVENTORY.SUBINVENTORY_CODE
WHERE
 HR_ORGANIZATION_INFORMATION.ORG_INFORMATION_CONTEXT = 'Accounting Information'
 and MTL_SYSTEM_ITEMS.ORGANIZATION_ID = 124
GROUP BY
  REPLACE(STRING(INT(INVENTORY.INVENTORY_ITEM_ID )), ",", "") ,
  REPLACE(STRING(INT(INVENTORY.ORGANIZATION_ID   )), ",", ""),
  HR_OPERATING_UNITS.ORGANIZATION_ID,
  MTL_SYSTEM_ITEMS.PRIMARY_UOM_CODE,
  MTL_SYSTEM_ITEMS.SECONDARY_UOM_CODE,
  MTL_SYSTEM_ITEMS.ATTRIBUTE15,
  INVENTORY.SUBINVENTORY_CODE,
  LAST_DAY(CURRENT_DATE-1),
  INVENTORY.LOT_NUMBER,
  COALESCE(TMP_COST.STD_COST,0)+ COALESCE(TMP_COST.DUTY_COST,0) + COALESCE(TMP_COST.FREIGHT_COST,0),
  COALESCE(TMP_SGA_COST.CMPNT_COST,0) ,
  TMP_EXPIRATION_DATES.EXPIRATION_DATE,
  CASE 
      WHEN MTL_SYSTEM_ITEMS.LOT_CONTROL_CODE = 1 
        THEN 'No Control' 
      WHEN MTL_SYSTEM_ITEMS.LOT_CONTROL_CODE = 2 
        THEN 'Full Control' 
      ELSE 'N/A'
     END,
  MTL_SYSTEM_ITEMS.ATTRIBUTE8,
  MTL_SYSTEM_ITEMS.SHELF_LIFE_DAYS,
  CASE WHEN TMP_EXPIRATION_DATES.EXPIRATION_DATE is not null 
     AND TMP_EXPIRATION_DATES.EXPIRATION_DATE <= add_months(current_date, 6)
         THEN 'Hold'
  END,
  DATE_ADD(TMP_EXPIRATION_DATES.EXPIRATION_DATE,  - int(shelf_life_days)),
  TMP_MRPN.MRPN,
  MTL_SYSTEM_ITEMS.DUAL_UOM_CONTROL,
  UPPER(DATE.periodName),
  GL_SETS_OF_BOOKS.CURRENCY_CODE,
  MTL_SYSTEM_ITEMS.SEGMENT1,
  CASE WHEN GL_SETS_OF_BOOKS.CURRENCY_CODE = 'USD' THEN 1 ELSE EXCH_RATE.EXCH_RATE END  ,
  COALESCE(GIC.ACCTG_COST ,0)
""")
main_inventory.createOrReplaceTempView("main_inventory")

# COMMAND ----------

main = main_intransit.unionAll(main_inventory)
main.cache()

# COMMAND ----------

main.display()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['itemID','inventoryWarehouseID','subInventoryCode','lotNumber','inventoryDate','owningBusinessUnitId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
    .transform(tg_default(source_name))
    .transform(tg_supplychain_inventory())
    .transform(apply_schema(schema))
    .transform(attach_unknown_record)
    .select(columns)
    .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE

keys_intransit = spark.sql("""
  SELECT
      REPLACE(STRING(INT(rsl.item_id)), ",", "") itemId,
      REPLACE(STRING(INT(rsh.ship_to_org_id)), ",", "")  inventoryWarehouseID,
      'IN TRANSIT' subInventoryCode,
      rlt.lot_num lotNumber,
      LAST_DAY(CURRENT_DATE-1) inventoryDate,
      REPLACE(STRING(INT(HR_OPERATING_UNITS.ORGANIZATION_ID)), ",", "") owningBusinessUnitId
    FROM ebs.rcv_shipment_headers rsh
    join ebs.rcv_shipment_lines rsl on rsh.shipment_header_id = rsl.shipment_header_id
    join ebs.rcv_lot_transactions rlt on Rsl.Shipment_Line_Id = Rlt.Shipment_Line_Id
    join ebs.HR_ORGANIZATION_INFORMATION HOI on HOI.ORGANIZATION_ID = RSH.ORGANIZATION_ID
    join EBS.HR_OPERATING_UNITS on HOI.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
""")

keys_inventory = spark.sql("""
 SELECT
      REPLACE(STRING(INT(INVENTORY_ITEM_ID)), ",", "") itemId,
      REPLACE(STRING(INT(mtl_onhand_quantities_detail.ORGANIZATION_ID)), ",", "") inventoryWarehouseID,
      SUBINVENTORY_CODE subInventoryCode,
      COALESCE(LOT_NUMBER, 'No Lot Number') lotNumber,
      LAST_DAY(CURRENT_DATE-1) inventoryDate,
      REPLACE(STRING(INT(HR_OPERATING_UNITS.ORGANIZATION_ID)), ",", "") owningBusinessUnitId
    FROM ebs.mtl_onhand_quantities_detail
      INNER JOIN EBS.HR_ORGANIZATION_INFORMATION
    ON mtl_onhand_quantities_detail.ORGANIZATION_ID = HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID
  INNER JOIN EBS.HR_OPERATING_UNITS
    ON HR_ORGANIZATION_INFORMATION.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
""")





full_keys_f = (
  keys_intransit
  .union(keys_inventory)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'itemID,inventoryWarehouseID,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

filter_date = str(spark.sql('SELECT LAST_DAY(CURRENT_DATE-1) AS inventoryDate').collect()[0]['inventoryDate'])
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID', date_field = 'inventoryDate', date_value = filter_date)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'ebs.mtl_onhand_quantities_detail')
  update_run_datetime(run_datetime, table_name, 'ebs.mtl_onhand_quantities_detail')
  
  cutoff_value = get_incr_col_max_value(main_inc_int)
  update_cutoff_value(cutoff_value, table_name, 'ebs.rcv_shipment_headers')
  update_run_datetime(run_datetime, table_name, 'ebs.rcv_shipment_headers')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain

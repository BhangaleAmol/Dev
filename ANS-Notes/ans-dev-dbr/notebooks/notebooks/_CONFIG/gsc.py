# Databricks notebook source
config = [
 {
   'PartitionKey': '1',
   'RowKey': '1',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.wc_qv_order_data_v',
   'TABLE_NAME': 'wc_qv_order_data_v',
   'COLUMNS': 'KEY,Company_Number_Detail,PROD_NUM,ORDER_NUM,ORDER_LINE_NUM,ORDER_LINE_DETAIL_NUM,ORDER_DT,REQUEST_DT,SCHEDULE_DT,SHIP_DT,ORDER_STATUS,ORDER_QTY_STD_UOM,ORDERED_AMOUNT_DOC_CURR,LE_CURRENCY,CUSTUMER_ID,SHIP_TO_DELIVERY_LOCATION_ID,ORDER_TYPE,CUST_PO_NUM,SOURCE_ORDER_STATUS,ORGANIZATION_CODE,W_UPDATE_DT,DOC_CURR_CODE,DISTRIBUTOR_ID,DISTRIBUTOR_NAME,X_DELIVERY_NOTE_ID,X_ORDER_HOLD_TYPE,X_RESERVATION_VALUE,X_INTRANSIT_TIME,X_SHIPDATE_945,X_FREIGHT_TERMS,X_CREATION_DATE,X_BOOKED_DATE,X_SHIPPED_QUANTITY,X_SHIPPING_QUANTITY_UOM,X_NEED_BY_DATE,X_RETD,X_PROMISED_DATE,X_CETD,ORDER_DATE_TIME,SO_PROMISED_DATE,CREATEDDATE,STATUS,paymentMethodName,paymentTermName'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '2',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.wc_qv_order_data_full',
   'TABLE_NAME': 'wc_qv_order_data_full',
   'COLUMNS': 'KEY,Company_Number_Detail,PROD_NUM,ORDER_NUM,ORDER_LINE_NUM,ORDER_LINE_DETAIL_NUM,ORDER_DT,REQUEST_DT,SCHEDULE_DT,SHIP_DT,ORDER_STATUS,ORDER_QTY_STD_UOM,ORDERED_AMOUNT_DOC_CURR,LE_CURRENCY,CUSTUMER_ID,SHIP_TO_DELIVERY_LOCATION_ID,ORDER_TYPE,CUST_PO_NUM,SOURCE_ORDER_STATUS,ORGANIZATION_CODE,W_UPDATE_DT,DOC_CURR_CODE,DISTRIBUTOR_ID,DISTRIBUTOR_NAME,X_DELIVERY_NOTE_ID,X_ORDER_HOLD_TYPE,X_RESERVATION_VALUE,X_INTRANSIT_TIME,X_SHIPDATE_945,X_FREIGHT_TERMS,X_CREATION_DATE,X_BOOKED_DATE,X_SHIPPED_QUANTITY,X_SHIPPING_QUANTITY_UOM,X_NEED_BY_DATE,X_RETD,X_PROMISED_DATE,X_CETD,ORDER_DATE_TIME,SO_PROMISED_DATE,CREATEDDATE,STATUS,paymentMethodName,paymentTermName'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '3',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.wc_qv_open_order_data_v',
   'TABLE_NAME': 'wc_qv_open_order_data_v',
   'COLUMNS': 'KEY,Company_Number_Detail,PROD_NUM,ORDER_NUM,ORDER_LINE_NUM,ORDER_LINE_DETAIL_NUM,ORDER_DT,REQUEST_DT,SCHEDULE_DT,SHIP_DT,ORDER_STATUS,ORDER_QTY_STD_UOM,ORDERED_AMOUNT_DOC_CURR,LE_CURRENCY,CUSTUMER_ID,SHIP_TO_DELIVERY_LOCATION_ID,ORDER_TYPE,CUST_PO_NUM,SOURCE_ORDER_STATUS,ORGANIZATION_CODE,W_UPDATE_DT,DOC_CURR_CODE,DISTRIBUTOR_ID,DISTRIBUTOR_NAME,X_DELIVERY_NOTE_ID,X_ORDER_HOLD_TYPE,X_RESERVATION_VALUE,X_INTRANSIT_TIME,X_SHIPDATE_945,X_FREIGHT_TERMS,X_CREATION_DATE,X_BOOKED_DATE,X_SHIPPED_QUANTITY,X_SHIPPING_QUANTITY_UOM,X_NEED_BY_DATE,X_RETD,X_PROMISED_DATE,X_CETD,ORDER_DATE_TIME,SO_PROMISED_DATE,CREATEDDATE,STATUS,paymentMethodName,paymentTermName'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '4',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 's_logility.dodemand',
   'TABLE_NAME': 'Raw_logility_DODemand',
   'COLUMNS': '_ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,generationDate,forecastMonth,actualdemand,futureDemand,ADSDShipments,adjustedDemand,createdOn,modifiedOn,createdBy,modifiedBy'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '5',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 's_logility.doforecast',
   'TABLE_NAME': 'Raw_logility_DOForecast',
   'COLUMNS': '_ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,forecastModelType,StatLVLComp,StatTrendComp,StatSeasonal,StatLVLValue,StatTrendValue,MovingAvg,StatMAD,StatError,StatResSdv,StatSysSdv,Product_Life_Cycle,unForceStatisticalForecast,resultantNetForecast,futureBudgetUnits,futureBudgetValue,ADS1Forecast,generationDate,forecastMonth,PrimeABC,PredecessorCode,SuccessorCode,SuccessorBeginDate,DemandVariability,_SOURCE,createdOn,modifiedOn,createdBy,modifiedBy'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '6',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_logility.forecastaccuracy',
   'TABLE_NAME': 'Raw_logility_ForecastAccuracy',
   'COLUMNS': '_ID,forecast1Id,forecast2Id,forecast3Id,generationDate,actualDemand,NetForecast_1,NetForecast_2,NetForecast_3,generationDate_1,generationDate_2,generationDate_3,AD3FForecast_1,AD3FForecast_2,AD3FForecast_3,UAF3Forecast_1,UAF3Forecast_2,UAF3Forecast_3,createdOn,modifiedOn,createdBy,modifiedBy'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '7',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_tembo.lom_aa_do_l1',
   'TABLE_NAME': 'Raw_logility_Master_Data',
   'COLUMNS': 'Pyramid_Level,Forecast_1_ID,Forecast_2_ID,Forecast_3_ID,Record_Type,Description,Forecast_calculate_indicator,Unit_price,Unit_cost,Unit_cube,Unit_Weight,Product_group_conversion_option,Product_group_conversion_factor,Forecast_Planner,User_Data_84,User_Data_01,User_Data_02,User_Data_03,User_Data_04,User_Data_05,User_Data_06,User_Data_07,User_Data_08,User_Data_09,User_Data_10,User_Data_11,User_Data_12,User_Data_13,User_Data_14,User_Data_15,User_Data_16,User_Data_17,User_Data_18,User_Data_19,User_Data_20,User_Data_21,User_Data_22,User_Data_23,User_Data_24,User_Data_25,User_Data_26,User_Data_27,User_Data_28,User_Data_29,User_Data_30,User_Data_31,User_Data_32,User_Data_33,Cube_unit_of_measure,Weight_unit_of_measure,Unit_of_measure,Case_quantity'
   
 }
  ,
 {
   'PartitionKey': '1',
   'RowKey': '8',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.wc_qv_inventory_valuation_a',
   'TABLE_NAME': 'Raw_Oracle_inventory',
   'COLUMNS': 'ITEM_NUMBER,INVENTORY_ITEM_ID,ORGANIZATION_CODE,PRIMARY_QUANTITY,PRIMARY_UOM_CODE,SECONDARY_QUANTITY,SECONDARY_UOM_CODE,ITEM_DESCRIPTION,TRANSDATE,ORGANIZATION_NAME,SUBINVENTORY_CODE,ORGANIZATION_ID,DUAL_UOM_CONTROL,DIVISION,ITEM_TYPE,PERIOD_CODE,LOT_NUMBER,STND_CST,EXTENDED_CST,SGA_CST,EX_SGA_CST,EXP_DATE,LOT_CONTROL_FLAG,ORIGIN_CODE,DATE_RECEIVED,ORIGIN_NAME,PO_NUMBER,SHELF_LIFE_DAYS,ANS_STD_UOM,ANS_STD_QTY,STATUS,MFG_DATE,MRPN,_Source'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '9',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.raw_oracle_inventory_all',
   'TABLE_NAME': 'Raw_Oracle_inventory_All',
   'COLUMNS': 'ITEM_NUMBER,INVENTORY_ITEM_ID,ORGANIZATION_CODE,PRIMARY_QUANTITY,PRIMARY_UOM_CODE,SECONDARY_QUANTITY,SECONDARY_UOM_CODE,ITEM_DESCRIPTION,TRANSDATE,ORGANIZATION_NAME,SUBINVENTORY_CODE,ORGANIZATION_ID,DUAL_UOM_CONTROL,DIVISION,ITEM_TYPE,PERIOD_CODE,LOT_NUMBER,STND_CST,EXTENDED_CST,SGA_CST,EX_SGA_CST,EXP_DATE,LOT_CONTROL_FLAG,ORIGIN_CODE,DATE_RECEIVED,ORIGIN_NAME,PO_NUMBER,SHELF_LIFE_DAYS,ANS_STD_UOM,ANS_STD_QTY,STATUS,MFG_DATE,MRPN,_Source,CreatedDate,Exchange_Rate,Currency,QTY_IN_CASES'
   
 },
 {
   'PartitionKey': '1',
   'RowKey': '10',
   'ACTIVE': True,
   'INCREMENTAL': False,
   'PRUNE_DAYS': 10,
   'PUBLISH': True,
   'SOURCE_TABLE': 'g_gsc_sup.raw_oracle_inventory_all_hist',
   'TABLE_NAME': 'Raw_Oracle_inventory_All_hist',
   'COLUMNS': 'ITEM_NUMBER,INVENTORY_ITEM_ID,ORGANIZATION_CODE,PRIMARY_QUANTITY,PRIMARY_UOM_CODE,SECONDARY_QUANTITY,SECONDARY_UOM_CODE,ITEM_DESCRIPTION,TRANSDATE,ORGANIZATION_NAME,SUBINVENTORY_CODE,ORGANIZATION_ID,DUAL_UOM_CONTROL,DIVISION,ITEM_TYPE,PERIOD_CODE,LOT_NUMBER,STND_CST,EXTENDED_CST,SGA_CST,EX_SGA_CST,EXP_DATE,LOT_CONTROL_FLAG,ORIGIN_CODE,DATE_RECEIVED,ORIGIN_NAME,PO_NUMBER,SHELF_LIFE_DAYS,ANS_STD_UOM,ANS_STD_QTY,STATUS,MFG_DATE,MRPN,_Source,CreatedDate,Exchange_Rate,Currency,QTY_IN_CASES'
   
 }
]

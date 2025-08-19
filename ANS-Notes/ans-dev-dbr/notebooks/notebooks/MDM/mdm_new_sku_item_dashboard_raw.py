# Databricks notebook source
dbutils.widgets.text("source_folder", "","") 
sourceFolder = getArgument("source_folder")
sourceFolder = '/l1_raw_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","") 
targetFolder = getArgument("target_folder")
targetFolder = '/l2_clean_data' if targetFolder == '' else targetFolder

# COMMAND ----------

# MAGIC %run ./SHARED/functions

# COMMAND ----------

fileSystemUrl = get_filesystem_url()
sourceFolderUrl = fileSystemUrl + sourceFolder + '/'
targetFolderUrl = fileSystemUrl + targetFolder + '/'

# COMMAND ----------

# sourceFileUrl = sourceFolderUrl + 'PDHSTG.OUTB_STG_PDH_DASHBOARD.par'
# dfraw = spark.read.format('parquet').load(sourceFileUrl)

dfraw = spark.table("mdm.pdhstg_OUTB_STG_PDH_DASHBOARD_19nov20")
#dfraw = spark.table("mdm.OUTB_STG_PDH_DASHBOARD")
dfraw.createOrReplaceTempView("dfraw")

# COMMAND ----------

# MAGIC %sql SELECT * FROM dfraw where GPID IN ('841770', '840214','841554') and Region='EMEA'

# COMMAND ----------

def add_days_erp_status_active(df):
  return df.withColumn("Days ERP Status Active", 
    f.when(f.col("ERP Status Active Completion").isNull(),  
      f.when(f.col("ERP Status New Completion").isNotNull(), 
        f.greatest(f.lit(0), days_till_today_udf(f.col("ERP Status New Completion")))))
    .when(f.col("ERP Status Active Completion").isNotNull(),
      f.when(f.col("ERP Status New Completion").isNotNull(),                  
        f.greatest(f.lit(0), days_between_udf(f.col("ERP Status New Completion"), f.col("ERP Status Active Completion")))))
      .otherwise(None))

def add_days_erp_status_new(df):
  return df.withColumn("Days ERP Status New", 
    f.when(f.col("GTC_ITEM") == "Y",
       f.when(f.col("ERP Status New Completion").isNull() & f.col("SKU_GTC_CMPL_DT").isNotNull(), 
        f.greatest(f.lit(0), days_till_today_udf(f.col("SKU_GTC_CMPL_DT"))))
      .otherwise(
        f.greatest(f.lit(0), days_between_udf(f.col("SKU_GTC_CMPL_DT"), f.col("ERP Status New Completion"))))
    )
     .otherwise(
       f.when(f.col("ERP Status New Completion").isNull() & f.col("RMDM Completion").isNotNull(),
        f.greatest(f.lit(0), days_till_today_udf(f.col("RMDM Completion"))))
      .otherwise(
        f.greatest(f.lit(0), days_between_udf(f.col("RMDM Completion"), f.col("ERP Status New Completion"))))))
  
def add_days_gppm(df):
  return df.withColumn("Days GPPM", 
    f.when(f.col("GPPM_SKU_CMPL_DT").isNull(), 
      f.greatest(f.lit(0),days_till_today_udf(df.SKU_ITEM_CRT_DT)))
    .otherwise(
      f.greatest(f.lit(0),days_between_udf(df.SKU_ITEM_CRT_DT, f.col("GPPM_SKU_CMPL_DT")))))
     
def add_days_gtc(df):
  return df.withColumn("Days GTC", 
    f.when(f.col("GTC_ITEM") == "N", 0)
     .when((f.col("SKU_GTC_CMPL_DT").isNull()) & (f.col("RMDM Completion").isNotNull()), 
       f.greatest(f.lit(0), days_till_today_udf(f.col("RMDM Completion"))))
    .otherwise(
       f.greatest(f.lit(0), days_between_udf(f.col("RMDM Completion"), f.col("SKU_GTC_CMPL_DT")))))

def add_days_rmdm(df):
  return df.withColumn("Days RMDM", 
    f.when((f.col("RMDM Completion").isNull()) & (f.col("RPPM Completion").isNotNull()), 
      f.greatest(f.lit(0), days_till_today_udf(f.col("RPPM Completion"))))
    .otherwise(
      f.greatest(f.lit(0), days_between_udf(f.col("RPPM Completion"),f.col("RMDM Completion")))))

def add_days_rppm(df):
  return df.withColumn("Days RPPM",
    f.when((f.col("RPPM Completion").isNull()) & (f.col("GPPM_SKU_CMPL_DT").isNotNull()), 
      f.greatest(f.lit(0), days_till_today_udf(f.col("GPPM_SKU_CMPL_DT"))))
    .otherwise(
      f.greatest(f.lit(0), days_between_udf(f.col("GPPM_SKU_CMPL_DT"), f.col("RPPM Completion") )))) 

def add_days_total(df):
  return df.withColumn("Days Total", 
    f.col("Days GPPM") + f.col("Days RPPM") + f.col("Days RMDM") + f.col("Days GTC") + f.col("Days ERP Status New") + f.col("Days ERP Status Active"))

def add_days_total_till_today(df):
  return df.withColumn("Days Total Till Today", 
    days_till_today_udf(df.SKU_ITEM_CRT_DT)) 

def add_erp_completed(df):
  return df.withColumn("ERP Completed", f.col("ERP Status Active Completion")) 

def add_erp_status_active_completion(df):
  return df.withColumn("ERP Status Active Completion", 
    f.when((f.col("REGION") == "EMEA") & (df.SKU_ITM_SAP_ACTIVE_DT.isNotNull()), df.SKU_ITM_SAP_ACTIVE_DT)
    .when((f.col("REGION") == "NALAC") & (df.SKU_ITM_ORCL_ACTIVE_DT.isNotNull()), df.SKU_ITM_ORCL_ACTIVE_DT)
    .when((f.col("REGION") == "APAC")  & (df.SKU_ITM_ORCL_ACTIVE_DT.isNotNull()), df.SKU_ITM_ORCL_ACTIVE_DT)               
    .when((f.col("REGION") == "EMEA")  & (df.SKU_ITM_SAP_ACTIVE_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"), f.col("RMDM Completion"))      
    .when((f.col("REGION") == "NALAC") & (df.SKU_ITM_ORCL_ACTIVE_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"), f.col("RMDM Completion"))
    .when((f.col("REGION") == "APAC") & (df.SKU_ITM_ORCL_ACTIVE_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"), f.col("RMDM Completion"))
    .when((f.col("REGION") == "EMEA") & (f.col("REGION_STATUS") != "New")  & (f.col("SKU_ITM_SAP_ACTIVE_DT").isNull()), f.col("RMDM Completion"))
    .when((f.col("REGION") == "NALAC") & (f.col("REGION_STATUS") != "New") & (f.col("SKU_ITM_ORCL_ACTIVE_DT").isNull()), f.col("RMDM Completion"))
    .when((f.col("REGION") == "APAC") & (f.col("REGION_STATUS") != "New")  & (f.col("SKU_ITM_ORCL_ACTIVE_DT").isNull()), f.col("RMDM Completion"))     
    .otherwise(None))
                                     
def add_erp_status_new_completion(df):
  return df.withColumn("ERP Status New Completion", 
    f.when((f.col("REGION") == "EMEA") & (df.SKU_ITM_SAP_CMPL_DT.isNotNull()), df.SKU_ITM_SAP_CMPL_DT)                   
    .when((f.col("REGION") == "NALAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNotNull()), df.SKU_ITM_ORCL_CMPL_DT)
    .when((f.col("REGION") == "APAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNotNull()), df.SKU_ITM_ORCL_CMPL_DT)
    .when((f.col("REGION") == "EMEA") & (df.SKU_ITM_SAP_CMPL_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"), f.col("RMDM Completion"))
    .when((f.col("REGION") == "NALAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"), f.col("RMDM Completion"))
    .when((f.col("REGION") == "APAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNull()) & (f.col("PPM") == "BULK LOAD") & (f.col("REGION_STATUS") == "Active"),f.col("RMDM Completion"))
    .when((f.col("REGION") == "EMEA") & (df.SKU_ITM_SAP_CMPL_DT.isNull()) & (f.col("REGION_STATUS") != "New") & (f.col("SKU_ITM_ORCL_CMPL_DT").isNull()),f.col("RMDM Completion"))                 
    .when((f.col("REGION") == "NALAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNull()) & (f.col("REGION_STATUS") != "New"), f.col("RMDM Completion"))
    .when((f.col("REGION") == "APAC") & (df.SKU_ITM_ORCL_CMPL_DT.isNull()) & (f.col("REGION_STATUS") != "New"), f.col("RMDM Completion"))                 
    .otherwise(None))

def add_month_completed(df):
  return df.withColumn("Month Completed", 
    f.when(f.col("ERP Status Active Completion").isNull(), None)
    .otherwise(f.date_format(f.col("ERP Status Active Completion"), 'yyyy-MM')))

def add_month_started(df):
  return df.withColumn("Month Started", f.date_format(f.col("SKU_ITEM_CRT_DT"), 'yyyy-MM'))

def add_ppm_column(df):
  return df.withColumn("PPM", 
    f.when(f.col("SKU_ITEM_CRT_USER") == "shobhit.sahu@ansell.com", "BULK LOAD")
    .when(f.col("SKU_ITEM_CRT_USER") == "venu.narapureddy@ansell.com", "BULK LOAD")
    .when(f.col("SKU_ITEM_CRT_USER") == "bipin.rajalingam@ansell.com", "BULK LOAD")
    .otherwise(f.col("SKU_ITEM_CRT_USER")))

def add_rmdm_completion(df):
  return df.withColumn("RMDM Completion", 
    f.when(f.col("PPM") == "BULK LOAD", df.SKU_ITEM_CRT_DT) 
    .when((f.col("REGION_STATUS") != "New") & (df.RMDM_EMEA_SKU_REVIEW_DT.isNull()), f.col("GPPM_SKU_CMPL_DT")) 
    .when((f.col("REGION_STATUS") != "New") & (df.RMDM_NALAC_SKU_REVIEW_DT.isNull()), f.col("GPPM_SKU_CMPL_DT"))   
    .when((f.col("REGION_STATUS") != "New") & (df.RMDM_APAC_SKU_REVIEW_DT.isNull()), f.col("GPPM_SKU_CMPL_DT"))                 
    .when(f.col("REGION") == "EMEA", df.RMDM_EMEA_SKU_REVIEW_DT)
    .when(f.col("REGION") == "NALAC", df.RMDM_NALAC_SKU_REVIEW_DT)
    .when(f.col("REGION") == "APAC", df.RMDM_APAC_SKU_REVIEW_DT)                   
    .otherwise(None))

def add_rppm_completion(df):
  return df.withColumn("RPPM Completion", 
    f.when(f.col("PPM") == "BULK LOAD", df.SKU_ITEM_CRT_DT)
    .when((f.col("REGION_STATUS") != "New") & (f.col("RPPM_EMEA_SKU_CMPL_DT").isNull()), df.GPPM_SKU_CMPL_DT)
    .when((f.col("REGION_STATUS") != "New") & (f.col("RPPM_NALAC_SKU_CMPL_DT").isNull()), df.GPPM_SKU_CMPL_DT)
    .when((f.col("REGION_STATUS") != "New") & (f.col("RPPM_APAC_SKU_CMPL_DT").isNull()), df.GPPM_SKU_CMPL_DT)
    .when(f.col("REGION") == "EMEA", df.RPPM_EMEA_SKU_CMPL_DT)
    .when(f.col("REGION") == "NALAC", df.RPPM_NALAC_SKU_CMPL_DT)
    .when(f.col("REGION") == "APAC", df.RPPM_APAC_SKU_CMPL_DT)                
    .otherwise(None))

def fix_gppm_sku_cmpl_dt_value(df):
  return df.withColumn("GPPM_SKU_CMPL_DT", 
    f.when((f.col("REGION_STATUS") != "New") & (f.col("GPPM_SKU_CMPL_DT").isNull()),df.SKU_ITEM_CRT_DT)                  
    .when(f.col("GPPM_SKU_CMPL_DT") < f.col("SKU_ITEM_CRT_DT"), df.SKU_ITEM_CRT_DT)   
     .otherwise(df.GPPM_SKU_CMPL_DT))

def fix_sku_gtc_cmpl_dt(df):
  return df.withColumn("SKU_GTC_CMPL_DT", 
    f.when((f.col("GTC_ITEM") == "N"), f.col("RMDM Completion"))
    .when((f.col("REGION_STATUS") == "Active") & (f.col("PPM") == "BULK LOAD"), f.col("RMDM Completion"))                  
    .when((f.col("SKU_GTC_CMPL_DT").isNull()) & (f.col("REGION_STATUS") != "New"), f.col("RMDM Completion"))
    .otherwise(df.SKU_GTC_CMPL_DT))


# COMMAND ----------

dfcl = (
  dfraw
  .transform(
    parse_date_column(
      ['SKU_ITEM_CRT_DT', 'GPPM_SKU_CMPL_DT', 'RPPM_NALAC_SKU_CMPL_DT', 'RMDM_NALAC_SKU_REVIEW_DT', 
       'SKU_GTC_CMPL_DT', 'SKU_ITM_SAP_CMPL_DT', 'SKU_ITM_SAP_ACTIVE_DT', 'SKU_ITM_ORCL_CMPL_DT',   
       'SKU_ITM_ORCL_ACTIVE_DT', 'RPPM_EMEA_SKU_CMPL_DT', 'RPPM_APAC_SKU_CMPL_DT', 'RMDM_EMEA_SKU_REVIEW_DT', 
       'RMDM_APAC_SKU_REVIEW_DT']))
   .transform(add_ppm_column)
   .transform(fix_gppm_sku_cmpl_dt_value)
   .transform(add_rmdm_completion)
   .transform(fix_sku_gtc_cmpl_dt)
   .transform(add_days_gppm)
   .transform(add_rppm_completion)
   .transform(add_days_rppm)
   .transform(add_days_rmdm)
   .transform(add_days_gtc)
   .transform(add_erp_status_new_completion)
   .transform(add_days_erp_status_new)
   .transform(add_erp_status_active_completion)
   .transform(add_erp_completed)
   .transform(add_days_erp_status_active)
   .transform(add_days_total)
   .transform(add_days_total_till_today)
   .transform(add_month_started)
   .transform(add_month_completed)   
)

dfcl.createOrReplaceTempView("dfcl")
display(dfcl)

# COMMAND ----------

destFolderUrl = targetFolderUrl + 'new_sku_item_dashboard'
dfcl.coalesce(1).write.save(destFolderUrl, format="csv", sep="|", header="true", mode="overwrite", quote="\u0000")
fileName = list(filter(lambda r : r.name.endswith(".csv"), dbutils.fs.ls(destFolderUrl)))[0].name
destFileUrl = targetFolderUrl + 'new_sku_item_dashboard/' + fileName
destSingleFileUrl = targetFolderUrl + 'new_sku_item_dashboard.csv'
dbutils.fs.cp(destFileUrl, destSingleFileUrl)

# COMMAND ----------

# read saved file
# destSingleFileUrl = targetFolderUrl + 'new_sku_item_dashboard.csv'
# df2 = spark.read.load(destSingleFileUrl, format="csv", inferSchema="true", header="true", delimiter="|")
# df2.createOrReplaceTempView("df2")
# display(df2)

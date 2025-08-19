# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.timedimension

# COMMAND ----------

# LOAD DATASETS
time_dimension_ind = spark.table('amazusftp1.time_dimension')
if sampling: time_dimension_ind = time_dimension_ind.limit(10)
time_dimension_ind.createOrReplaceTempView('time_dimension')
display(time_dimension_ind)

# COMMAND ----------

main = spark.sql("""
SELECT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    dayID,
    to_date(dateid,'dd/MM/yyyy') dateId,
    fiscalMonthId,
    fiscalQuarterId,
    fiscalYearId,
    periodName,
    CASE 
WHEN DATE_FORMAT(to_date(dateid,'dd/MM/yyyy'),"yyyyMM") = DATE_FORMAT(ADD_MONTHS(current_timestamp(),0),"yyyyMM")  THEN 'Current'
WHEN DATE_FORMAT(to_date(dateid,'dd/MM/yyyy'),"yyyyMM") = DATE_FORMAT(ADD_MONTHS(current_timestamp(),-1),"yyyyMM") THEN 'Previous'
WHEN DATE_FORMAT(to_date(dateid,'dd/MM/yyyy'),"yyyyMM") = DATE_FORMAT(ADD_MONTHS(current_timestamp(),1),"yyyyMM")  THEN 'Next'
ELSE NULL
END currentPeriodCode,
    weekDayFull,
    weekDayShort,
    dayNumOfWeek,
    dayNumOfMonth,
    dayNumOfYear,
    monthId,
    monthTimeSpan,
    to_date(monthEndDate,'dd/MM/yyyy') monthEndDate,
    monthShortDesc,
    monthLongDesc,
    monthShort,
    monthLong,
    monthNumofYear,
    quarterId,
    quarterTimeSpan,
    to_date(quarterEndDate,'dd/MM/yyyy') quarterEndDate,
    quarterNumOfYear,
    halfNumOfYear,
    halfOfYearId,
    halfYearTimeSpan,
    to_date(halfYearEndDate,'dd/MM/yyyy') halfYearEndDate,
    yearId,
    yearTimeSpan,
    to_date(yearEndDate,'dd/MM/yyyy') yearEndDate,
    workDayUs,
    workDayCa,
    workDayLa,
    workDayAu,
    workDayRoa,
    workDayEmea,
    calWeek,
    fiscalWeek,
    to_date(weekStarting,'dd/MM/yyyy') weekStarting,
    to_date(weekEnding,'dd/MM/yyyy') weekEnding,
    CASE 
		WHEN fiscalYearId = DATE_FORMAT(ADD_MONTHS(current_timestamp(),-6),"yyyy") then 'Previous' 
		WHEN fiscalYearId = DATE_FORMAT(ADD_MONTHS(current_timestamp(),6),"yyyy") then 'Current' 
		WHEN fiscalYearId = DATE_FORMAT(ADD_MONTHS(current_timestamp(),18),"yyyy") then 'Next' 
END  currentFiscalYearCode,
    CASE 
		WHEN yearId = DATE_FORMAT(current_timestamp(),"yyyy")-1 then 'Previous' 
		WHEN yearId = DATE_FORMAT(current_timestamp(),"yyyy") then 'Current' 
		WHEN yearId = DATE_FORMAT(current_timestamp(),"yyyy")+1 then 'Next' 
END currentCalYearCode,
    weekInMonth,
    to_date(fiscalYearStarting,'dd/MM/yyyy') fiscalYearStarting,
    to_date(fiscalYearEnding,'dd/MM/yyyy') fiscalYearEnding,
    to_date(calYearStarting,'dd/MM/yyyy') calYearStarting,
    to_date(calYearEnding,'dd/MM/yyyy') calYearEnding
  FROM amazusftp1.time_dimension
  where dayid is not null
 """)

main.createOrReplaceTempView('main')
main.cache()
display(main)

# COMMAND ----------

# VALIDATE NK - DISTINCT COUNT
valid_count_rows(main, ['dayID'])

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_time())
  .transform(apply_schema(schema))
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE SK
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)
add_unknown_record(main_f, table_name)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core

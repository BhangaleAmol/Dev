# Databricks notebook source
# MAGIC %run ../../_SHARED/FUNCTIONS/1.3/func_transformation

# COMMAND ----------

# UDF
def regexp_search(pattern, text):
  if(text is None):
    return False
  return True if re.search(pattern, text) else False
regexp_search_udf = udf(regexp_search, BooleanType())  

# COMMAND ----------

def add_sf_mobile_number(df):
  return df.withColumn("MOBILE_NUMBER", df["MOBILE_PHONE"])

def add_sf_phone_number(df):
  return df.withColumn("PHONE_NUMBER", df["PHONE"])

def clean_sf_account_name(df):
  df = df.withColumn('ACCOUNT_NAME', f.upper(df.ACCOUNT_NAME))
  df = df.withColumn('ACCOUNT_NAME', f.regexp_replace(df.ACCOUNT_NAME, r"^\?+", ""))
  df = df.withColumn('ACCOUNT_NAME', f.when(df.ACCOUNT_NAME == "", None).otherwise(df.ACCOUNT_NAME))
  return df

def clean_sf_all_numbers(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.regexp_replace(c_name, r'[^a-zA-Z0-9]', "")) #skip special characters 
      df = df.withColumn(c_name, f.regexp_extract(c_name, '(\d+)([a-zA-Z]*)', 1)) #skip extensions
      df = df.withColumn(c_name, f.regexp_replace(c_name, r'^0+', ""))
      df = df.withColumn(c_name, f.when(regexp_search_udf(f.lit("E\+[0-9]+"), f.col(c_name)), None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(regexp_search_udf(f.lit("^(0+)$"), f.col(c_name)), None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.col(c_name).isin({"0", "Unknown", ""}), None).otherwise(f.col(c_name)))     
      df = df.withColumn(c_name, f.when(f.length(f.col(c_name)) < 5, None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when((df.BILLING_COUNTRY == "United States") & (f.length(f.col(c_name)) < 10), None).otherwise(f.col(c_name)))
    return df
  return inner

def clean_sf_billing_country(df):
  return df.withColumn("BILLING_COUNTRY", 
    f.when(df.BILLING_COUNTRY == 'Iran, Islamic Republic of', 'Iran') 
    .when(df.BILLING_COUNTRY == 'Congo, the Democratic Republic of the', 'Congo')    
    .when(df.BILLING_COUNTRY == 'Korea, Republic of', 'Korea')                   
    .when(df.BILLING_COUNTRY == "Lao People's Democratic Republic", 'Laos')                     
    .when(df.BILLING_COUNTRY == "Macedonia, the former Yugoslav Republic of", 'Macedonia')
    .when(df.BILLING_COUNTRY == "Moldova, Republic of", 'Moldova')        
    .when(df.BILLING_COUNTRY == "Syrian Arab Republic", 'Syria')                     
    .when(df.BILLING_COUNTRY == "Tanzania, United Republic of", 'Tanzania')            
    .when(df.BILLING_COUNTRY == "Venezuela, Bolivarian Republic of", 'Venezuela')
    .when(df.BILLING_COUNTRY == "Viet Nam", 'Vietnam')     
    .otherwise(df.BILLING_COUNTRY)
  )

def clean_sf_mobile_number(df):
  df = df.withColumn(
    "MOBILE_NUMBER", 
    f.when((df.BILLING_COUNTRY == "Brazil") & (f.substring(df.MOBILE_NUMBER, 1, 2) == "55"), df.MOBILE_NUMBER.substr(f.lit(1), f.lit(13)))
    .otherwise(df.MOBILE_NUMBER))
  return df

def clean_sf_phone_number(df):
  return df.withColumn(
    "PHONE_NUMBER", 
    f.when((df.PHONE_NUMBER == "7323455400") & ~(f.col("COMPANY_NAME").isin({"ANSELL", "ANSELL HEATLHCARE"})), None)
    .when((df.BILLING_COUNTRY == "Brazil") & (f.substring(df.PHONE_NUMBER, 1, 2) != "55") & (f.length(df.PHONE_NUMBER) == 10), f.concat(f.lit("55"), df.PHONE_NUMBER))
    .when((df.BILLING_COUNTRY == "Brazil") & (f.substring(df.PHONE_NUMBER, 1, 2) == "55"), df.PHONE_NUMBER.substr(f.lit(1), f.lit(12)))
    .otherwise(df.PHONE_NUMBER)
  )

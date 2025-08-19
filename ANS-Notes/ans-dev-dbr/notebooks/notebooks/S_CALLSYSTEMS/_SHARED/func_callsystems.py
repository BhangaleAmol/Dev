# Databricks notebook source
# MAGIC %run ../../_SHARED/FUNCTIONS/1.3/func_transformation

# COMMAND ----------

def regexp_search(pattern, text):
  if(text is None):
    return False
  return True if re.search(pattern, text) else False
regexp_search_udf = udf(regexp_search, BooleanType()) 

def extract_city(df):
   return df.withColumn(
     "CITY", 
     f.when(regexp_search_udf(f.lit("COWANSVILLE"), df.QUEUE_NAME), "Cowansville")
     .when(regexp_search_udf(f.lit("ISELIN"), df.QUEUE_NAME), "Iselin")
     .when(regexp_search_udf(f.lit("RENO"), df.QUEUE_NAME), "Reno")
     .when(regexp_search_udf(f.lit("MUMBAI"), df.QUEUE_NAME), "Mumbai")
     .otherwise("N/A"))
  
def extract_location_static(df):
  
  queue_region_dict =	{
    "Q_NA_ONE_SOLUTION": {"CITY": "Iselin", "COUNTRY": "US", "CONTINENT": "North America", "REGION": "AMER"},
    "Q_USA_CUSTOMER_SERVICE": {"CITY": "N/A", "COUNTRY": "US", "CONTINENT": "North America", "REGION": "AMER"}  
  }
  
  def extract_city(text):
    return queue_region_dict[text]['CITY'] if text in queue_region_dict else None
  extract_city_udf = udf(extract_city, StringType())
  
  def extract_country(text):
    return queue_region_dict[text]['COUNTRY'] if text in queue_region_dict else None
  extract_country_udf = udf(extract_country, StringType())
  
  def extract_continent(text):
    return queue_region_dict[text]['CONTINENT'] if text in queue_region_dict else None
  extract_continent_udf = udf(extract_continent, StringType())
  
  def extract_region(text):
    return queue_region_dict[text]['REGION'] if text in queue_region_dict else None
  extract_region_udf = udf(extract_region, StringType())
  
  return (
    df
    .withColumn("CITY", f.when(extract_city_udf(df.QUEUE_NAME).isNotNull(), extract_city_udf(df.QUEUE_NAME)).otherwise(df.CITY))
    .withColumn("COUNTRY", f.when(extract_country_udf(df.QUEUE_NAME).isNotNull(), extract_country_udf(df.QUEUE_NAME)).otherwise(df.COUNTRY))
    .withColumn("CONTINENT", f.when(extract_continent_udf(df.QUEUE_NAME).isNotNull(), extract_continent_udf(df.QUEUE_NAME)).otherwise(df.CONTINENT))
    .withColumn("REGION", f.when(extract_region_udf(df.QUEUE_NAME).isNotNull(), extract_region_udf(df.QUEUE_NAME)).otherwise(df.REGION))
  )

def extract_continent(df):
   return df.withColumn(
     "CONTINENT", 
     f.when(regexp_search_udf(f.lit("_NA_"), df.QUEUE_NAME), "North America")
     .when(regexp_search_udf(f.lit("BRAZIL"), df.QUEUE_NAME), "South America")
     .when(regexp_search_udf(f.lit("CANADA"), df.QUEUE_NAME), "North America")  
     .when(regexp_search_udf(f.lit("MEXICO"), df.QUEUE_NAME), "North America")
     .when(regexp_search_udf(f.lit("COWANSVILLE"), df.QUEUE_NAME), "North America")
     .when(regexp_search_udf(f.lit("ISELIN"), df.QUEUE_NAME), "North America")
     .when(regexp_search_udf(f.lit("RENO"), df.QUEUE_NAME), "North America")
     .when(regexp_search_udf(f.lit("CHINA"), df.QUEUE_NAME), "Asia")       
     .when(regexp_search_udf(f.lit("KOREA"), df.QUEUE_NAME), "Asia")
     .when(regexp_search_udf(f.lit("MUMBAI"), df.QUEUE_NAME), "Asia")    
     .when(regexp_search_udf(f.lit("JAPAN"), df.QUEUE_NAME), "Asia")
     .when(regexp_search_udf(f.lit("_APAC_"), df.QUEUE_NAME), "Asia")     
     .when(regexp_search_udf(f.lit("GERMANY"), df.QUEUE_NAME), "Europe")
     .when(regexp_search_udf(f.lit("France"), df.QUEUE_NAME), "Europe")
     .when(regexp_search_udf(f.lit("UK"), df.QUEUE_NAME), "Europe")
     .when(regexp_search_udf(f.lit("BELGUIM"), df.QUEUE_NAME), "Europe")
     .when(regexp_search_udf(f.lit("EUROPE"), df.QUEUE_NAME), "Europe")
     .when(regexp_search_udf(f.lit("_EMEA_"), df.QUEUE_NAME), "Europe") 
     .when(regexp_search_udf(f.lit("AUSTRALIA"), df.QUEUE_NAME), "Australia")  
     .otherwise("N/A")) 
  
def extract_country(df):
   return df.withColumn(
     "COUNTRY", 
     f.when(regexp_search_udf(f.lit("BRAZIL"), df.QUEUE_NAME), "Brazil")
     .when(regexp_search_udf(f.lit("CANADA"), df.QUEUE_NAME), "Canada")  
     .when(regexp_search_udf(f.lit("CHINA"), df.QUEUE_NAME), "China")  
     .when(regexp_search_udf(f.lit("MEXICO"), df.QUEUE_NAME), "Mexico")
     .when(regexp_search_udf(f.lit("FRANCE"), df.QUEUE_NAME), "France")    
     .when(regexp_search_udf(f.lit("COWANSVILLE"), df.QUEUE_NAME), "Canada")
     .when(regexp_search_udf(f.lit("ISELIN"), df.QUEUE_NAME), "US")
     .when(regexp_search_udf(f.lit("RENO"), df.QUEUE_NAME), "US")
     .when(regexp_search_udf(f.lit("GERMANY"), df.QUEUE_NAME), "Germany")
     .when(regexp_search_udf(f.lit("France"), df.QUEUE_NAME), "France")
     .when(regexp_search_udf(f.lit("UK"), df.QUEUE_NAME), "UK")
     .when(regexp_search_udf(f.lit("BELGUIM"), df.QUEUE_NAME), "Belgium")
     .when(regexp_search_udf(f.lit("MUMBAI"), df.QUEUE_NAME), "Indie")
     .when(regexp_search_udf(f.lit("KOREA"), df.QUEUE_NAME), "Korea")
     .when(regexp_search_udf(f.lit("JAPAN"), df.QUEUE_NAME), "Japan") 
     .otherwise("N/A"))

def extract_region(df):
   return df.withColumn(
     "REGION", 
     f.when(regexp_search_udf(f.lit("_NA_"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("_EMEA_"), df.QUEUE_NAME), "EMEA")
     .when(regexp_search_udf(f.lit("_APAC_"), df.QUEUE_NAME), "APAC")    
     .when(regexp_search_udf(f.lit("BRAZIL"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("CANADA"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("CHINA"), df.QUEUE_NAME), "APAC")
     .when(regexp_search_udf(f.lit("MUMBAI"), df.QUEUE_NAME), "APAC")
     .when(regexp_search_udf(f.lit("KOREA"), df.QUEUE_NAME), "APAC")
     .when(regexp_search_udf(f.lit("JAPAN"), df.QUEUE_NAME), "APAC") 
     .when(regexp_search_udf(f.lit("MEXICO"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("COWANSVILLE"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("ISELIN"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("RENO"), df.QUEUE_NAME), "AMER")
     .when(regexp_search_udf(f.lit("GERMANY"), df.QUEUE_NAME), "EMEA")
     .when(regexp_search_udf(f.lit("France"), df.QUEUE_NAME), "EMEA")
     .when(regexp_search_udf(f.lit("UK"), df.QUEUE_NAME), "EMEA")
     .when(regexp_search_udf(f.lit("BELGUIM"), df.QUEUE_NAME), "EMEA")  
     .otherwise("N/A"))
  
def upper_queue_name(df):
   return df.withColumn("QUEUE_NAME", f.upper(df.QUEUE_NAME))

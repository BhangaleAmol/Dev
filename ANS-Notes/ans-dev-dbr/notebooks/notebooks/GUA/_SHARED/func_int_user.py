# Databricks notebook source
def remove_int_user_duplicates(df):
  df.createOrReplaceTempView('df')
  
  return spark.sql("""
    WITH df_num AS (
      SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY is_deleted) AS num
      FROM df
    )
    SELECT * FROM df_num WHERE num = 1
  """).drop('num')

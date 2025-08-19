# Databricks notebook source
def apply_mappings(mapping):
  def inner(df): 
    
    mapping_df = get_mapping_dataframe(mapping).withColumn("columnName", f.lower(f.col("columnName")))
    mapping_columns = mapping_df.select("columnName").distinct().rdd.flatMap(lambda x: x).collect()     
    columns = [c for c in df.columns if c.lower() in mapping_columns]
    
    for column in columns:   
      df = (
        df.alias('a')
        .join(mapping_df.alias('m'),
          [
            f.col('m.columnName') == column.lower(),
            f.trim(f.col(column)) == f.col('m.sourceValue')
          ], 
          how='left'
        )
        .select([f.when(f.col('m.match') == True, f.col('m.targetValue')).otherwise(f.col('a.' + column)).alias(column)
          if c == column 
          else f.col('a.' + c) 
          for c in df.columns
        ])
      )
    
    return df
  return inner

def get_mapping_dataframe(input_dict, schema = "columnName: string, sourceValue: string, targetValue: string, match: boolean"):
  result = []
  for k, v in input_dict.items():  
    for e, f in v.items():
      result.append([k, e, f, True])
      
  return spark.createDataFrame(result, schema)

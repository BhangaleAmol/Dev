# Databricks notebook source
def display_sorted(df):
  df.select(sorted(df.columns)).display()

def print_dataframe_as_dict(df):
  print('schema = {')
  for data_type in df.dtypes:    
    print("\t'{0}':'{1}',".format(data_type[0], data_type[1]))
  print('}')

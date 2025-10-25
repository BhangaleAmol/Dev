from pyspark.sql import SparkSession
from databricks.functions.shared.dataframe import DataFrame
import pandas as pd
from databricks.functions.shared.paths import get_test_data_folder_path
from databricks.functions.shared.transformations import cast_data_type, sort_columns
import pyspark.sql.functions as f


# def print_dataframe_details(df: DataFrame):
#     print(f'\ncolumn number: {len(df.columns)}')
#     i = 0
#     for t in df.dtypes:
#         print(f'{i} {t[0]} ({t[1]})')
#         i += 1


def check_equal_columns(df1: DataFrame, df2: DataFrame, details: bool = False) -> bool:
    equal_columns = set(df1.columns) == set(df2.columns)
    if not equal_columns and details:
        print(f'equal_columns: {str(equal_columns)}')
        print(set(df1.columns).difference(df2.columns))
        print(set(df2.columns).difference(df1.columns))
        print(f'actual')
        df1.sort(df1.columns).display()
        print(f'expected')
        df2.sort(df2.columns).display()
    return equal_columns


def check_equal_rows(df1: DataFrame, df2: DataFrame, details: bool = False) -> bool:
    equal_rows = df1.count() == df2.count()
    if not equal_rows and details:
        print(f'equal_rows: {str(equal_rows)}')
        print(f'actual ({df1.count()}) vs expected ({df2.count()})')
        print(f'actual')
        df1.sort(df1.columns).display()
        print(f'expected')
        df2.sort(df2.columns).display()
    return equal_rows


def check_equal_types(df1: DataFrame, df2: DataFrame, details: bool = False) -> bool:
    equal_types = set(df1.dtypes) == set(df2.dtypes)
    if not equal_types and details:
        print(f'equal_types: {str(equal_types)}')
        print(set(df1.dtypes).difference(df2.dtypes))
        print(set(df2.dtypes).difference(df1.dtypes))
    return equal_types


def check_equal_values(df1: DataFrame, df2: DataFrame, details: bool = False) -> bool:
    df1_except_df2 = df1.exceptAll(df2)
    df2_except_df1 = df2.exceptAll(df1)
    equal_values = df1_except_df2.count() == df2_except_df1.count() == 0

    if not equal_values and details:

        print(f'equal_values: {str(equal_values)}')
        print(f'actual')
        df1_except_df2.sort(df1_except_df2.columns).display()
        print(f'expected')
        df2_except_df1.sort(df2_except_df1.columns).display()

        # compare column values to find differences
        column_datasets = []
        for column in df1.columns:
            column_datasets.append(
                df1
                .select(column)
                .exceptAll(df2.select(column))
                .withColumn('length', f.length(f.col(column)))
            )
            column_datasets.append(
                df2
                .select(column)
                .exceptAll(df1.select(column))
                .withColumn('length', f.length(f.col(column)))
            )

        for column_df in column_datasets:
            if column_df.count() > 0:
                column_df.show()

    return equal_values


def dataframes_equal(df1: DataFrame, df2: DataFrame, options={}) -> bool:

    details = options.get('details', False)
    ignore_schema = options.get('ignore_schema', False)

    df1 = df1.transform(sort_columns).cache()
    df2 = df2.transform(sort_columns).cache()

    # equal_columns
    equal_columns = check_equal_columns(df1, df2, details)
    if not equal_columns:
        return False

    # equal_types
    if ignore_schema:
        equal_types = True
        df1 = df1.transform(cast_data_type, df1.columns)
        df2 = df2.transform(cast_data_type, df2.columns)
    else:
        equal_types = check_equal_types(df1, df2, details)

    if not equal_types:
        return False

    # equal_rows
    equal_rows = check_equal_rows(df1, df2, details)
    if not equal_rows:
        return False

    # equal_values
    equal_values = check_equal_values(df1, df2, details)
    if not equal_values:
        return False

    return True


def read_test_data(file_path, folder_path=get_test_data_folder_path()) -> DataFrame:
    pandas_df = pd.read_csv(
        f"{folder_path}{file_path}", keep_default_na=False, dtype=str)
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(pandas_df)

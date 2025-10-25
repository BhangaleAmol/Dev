from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from databricks.sdk.runtime import *
import pyspark.sql.functions as f
import logging
import re
from datetime import datetime


def add_column(table_name: str, column_name: str, column_type: str, after_column: str = None):

    table_name = get_table_name(table_name)
    logger = logging.getLogger('edm')

    if not table_exists(table_name):
        raise Exception(f'Table {table_name} does not exist.')

    if column_exists(table_name, column_name):
        logger.debug(
            f'Column {column_name} already exists in {table_name} table.')
    else:
        logger.debug(f'Adding column {column_name} to {table_name} table.')

        after_column_str = ''
        if after_column is not None:
            after_column_str = f"AFTER {after_column}"

        query = f"ALTER TABLE {table_name} ADD COLUMNS ({column_name} {column_type} {after_column_str})"
        run_query(query)


def alter_column_name(table_name: str, column_name: str, new_column_name: str):
    table_name = get_table_name(table_name)
    table_path = get_table_location(table_name)
    table_partitions = get_table_partitions(table_name)
    spark.read.table(table_name) \
        .withColumnRenamed(column_name, new_column_name) \
        .write.format('delta') \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", table_path) \
        .partitionBy(*table_partitions) \
        .saveAsTable(table_name)


def alter_column_type(table_name: str, column_name: str, data_type: str):

    df = spark.read.table(table_name)

    if dict(df.dtypes)[column_name] == data_type:
        logger = logging.getLogger('edm')
        logger.debug(
            f'Column {column_name} already has {data_type} data type.')
        return

    table_name = get_table_name(table_name)
    table_path = get_table_location(table_name)
    table_partitions = get_table_partitions(table_name)
    spark.read.table(table_name) \
        .withColumn(column_name, f.col(column_name).cast(data_type)) \
        .write.format('delta') \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", table_path) \
        .partitionBy(*table_partitions) \
        .saveAsTable(table_name)


def alter_table_partition(table_name: str, table_partitions: list[str]):
    table_name = get_table_name(table_name)
    table_path = get_table_location(table_name)

    current_table_partitions = get_table_partitions(table_name)
    if set(current_table_partitions) == set(table_partitions):
        logger = logging.getLogger('edm')
        logger.debug(
            f'Table {table_name} already has {str(table_partitions)} partitions.')
        return

    spark.read.table(table_name) \
        .write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", table_path) \
        .partitionBy(*table_partitions) \
        .saveAsTable(table_name)


def apply_soft_delete(full_keys_df: DataFrame, table_name: str, options={}):

    source_name = options.get('source_name')
    dataset_name = options.get('dataset_name')

    table_name = get_table_name(table_name)

    key_filter = ' AND '.join(
        [f"t.{c} = full_keys.{c}" for c in full_keys_df.columns])

    source_filter = ''
    if source_name is not None:
        source_filter = f" AND _SOURCE = '{source_name}'"

    dataset_filter = ''
    if dataset_name is not None:
        dataset_filter = f" AND _DATASET = '{dataset_name}'"

    full_keys_df.createOrReplaceTempView('full_keys')
    query = f"""
    UPDATE {table_name} t 
    SET 
        t._DELETED = 'True', 
        t._MODIFIED = CURRENT_TIMESTAMP() 
    WHERE 
        t._DELETED IS FALSE
        AND NOT EXISTS (SELECT 1 FROM full_keys WHERE {key_filter})
        {source_filter}
        {dataset_filter} 
    """
    run_query(query)


def column_exists(table_name: str, column_name: str) -> bool:
    table_name = get_table_name(table_name)
    columns = spark.sql(f"SHOW COLUMNS IN {table_name}").select(
        'col_name').collect()
    column_list = [row[0] for row in columns]
    return (column_name in column_list)


def configure_table(table_name):
    try:
        query = f'''
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            delta.autoOptimize.optimizeWrite = true, 
            delta.autoOptimize.autoCompact = true)
        '''
        run_query(query)
    except Exception as e:
        print(e)


def create_database(database_name):
    database_name = get_database_name(database_name)
    run_query(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    return


def create_table(table_name, table_schema, table_location, options={}):

    run_configure_table = options.get('configure_table', True)
    partition_columns = options.get('partition_columns')
    table_format = options.get('table_format', 'delta')

    table_name = get_table_name(table_name)
    logger = logging.getLogger('edm')

    table_exist = table_exists(table_name)
    if table_exist:
        logger.debug(f'TABLE {table_name} ALREADY EXISTS')
        return False

    logger.debug(f"CREATE TABLE {table_name}")
    logger.debug(f"{table_location}")

    spark = SparkSession.builder.getOrCreate()
    empty_df = spark.createDataFrame([], table_schema)
    command = (
        empty_df.write
        .format(table_format)
        .option("path", table_location)
        .option("overwriteSchema", "true")
        .mode('overwrite')
    )

    # partitions
    if partition_columns is not None:
        logger.debug(f'partition by: {partition_columns}')
        command.partitionBy(partition_columns)

    command.saveAsTable(table_name)

    if run_configure_table:
        configure_table(table_name)
    return True


def database_exists(database_name) -> bool:
    try:
        database_name = get_database_name(database_name)
        catalog_name, _ = split_database_name(database_name)
        database_names = get_databases(catalog_name)
        database_exists = database_name in database_names
    except Exception as e:
        database_exists = False

    return database_exists


def drop_column(table_name: str, column_name: list):
    table_name = get_table_name(table_name)
    location = get_table_location(table_name)
    partitions = get_table_partitions(table_name)

    spark.table(table_name) \
        .drop(*column_name) \
        .write \
        .format('delta') \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", location) \
        .partitionBy(*partitions) \
        .saveAsTable(table_name)


def drop_database(database_name):
    database_name = get_database_name(database_name)
    table_names = get_tables(database_name)
    for table_name in table_names:
        drop_table(table_name)
    run_query(f'DROP DATABASE {database_name}')


def drop_table(table_name, remove_data=True):

    table_name = get_table_name(table_name)

    table_exist = table_exists(table_name)
    if (not table_exist):
        return

    if remove_data:
        location = get_table_location(table_name)

    query = f'DROP TABLE IF EXISTS {table_name}'
    run_query(query)

    if remove_data:
        logger = logging.getLogger('edm')
        logger.debug(f'DROP FOLDER ({location})')
        dbutils.fs.rm(location, True)


def get_database_name(database_name: str):
    catalog_name, database_name = split_database_name(database_name)
    return f'{catalog_name}.{database_name}'


def get_databases(catalog_name):
    return [
        get_database_name(row.databaseName)
        for row in run_query(f'SHOW DATABASES FROM {catalog_name}').collect()]


def get_table_location(table_name: str) -> str:

    table_name = get_table_name(table_name)
    desc = run_query(f'DESC FORMATTED {table_name}')

    location = (
        desc
        .filter("col_name = 'Location'")
        .select('data_type').collect()[0][0]
    )
    if location == 'string':
        location = (
            desc
            .filter("col_name = 'Location'")
            .select('data_type').collect()[1][0]
        )
    return location


def get_table_name(table_name: str):
    catalog_name, database_name, table_name = split_table_name(table_name)
    return f'{catalog_name}.{database_name}.{table_name}'


def get_table_partitions(table_name: str) -> list:
    table_name = get_table_name(table_name)
    try:
        partitions = run_query(f'SHOW PARTITIONS {table_name}').columns
    except Exception as e:
        partitions = []
    return partitions


def get_table_schema(table_name: str) -> dict:
    table_name = get_table_name(table_name)
    schema_list = run_query(f'DESCRIBE TABLE {table_name}').collect()
    col_names = [r.col_name for r in schema_list]
    if '# Partition Information' in col_names:
        split_index = col_names.index('# Partition Information')
        return {r.col_name: r.data_type for idx, r in enumerate(schema_list) if idx < split_index}
    else:
        return {r.col_name: r.data_type for _, r in enumerate(schema_list)}


def get_tables(database_name):
    database_name = get_database_name(database_name)
    data = run_query(f'SHOW TABLES FROM {database_name}').select(
        'tableName').collect()
    return [
        get_table_name(database_name + '.' + row.tableName)
        for row in data
        if row.tableName[-5:] != 'delta']


def run_query(query):
    logger = logging.getLogger('edm')
    query_str = query.replace('\n', '')
    query_str = re.sub(' +', ' ', query_str)
    logger.debug(query_str)

    spark = SparkSession.builder.getOrCreate()
    return spark.sql(query)


def merge_into_table(df, table_name, key_columns, options={}):

    merge_schema = options.get('merge_schema', False)
    source_name = options.get('source_name')

    table_name = get_table_name(table_name)

    if isinstance(key_columns, str):
        key_columns = [column.strip() for column in key_columns.split(',')]

    if merge_schema is True:
        run_query('SET spark.databricks.delta.schema.autoMerge.enabled = true')

    # join
    join_str = ' AND '.join([f't.{c} = delta.{c}' for c in key_columns])

    # source name
    if source_name:
        join_str = f"{join_str} AND t._SOURCE = '{source_name}'"

    df.createOrReplaceTempView('delta')
    query = f'''
    MERGE INTO {table_name} t USING delta ON {join_str} 
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    '''
    run_query(query)
    return True


def insert_into_table(df, table_name, options={}):
    merge_schema = options.get('merge_schema', False)
    overwrite = options.get('overwrite', False)
    overwrite_schema = options.get('overwrite_schema', False)
    partition_columns = options.get('partition_columns')
    mode = 'overwrite' if overwrite else 'append'

    table_name = get_table_name(table_name)

    logger = logging.getLogger('edm')
    logger.debug(f'insert_into_table {table_name}')
    logger.debug(f'merge_schema: {str(merge_schema)}')
    logger.debug(f'overwrite: {str(overwrite)}')
    logger.debug(f'overwrite_schema: {str(overwrite_schema)}')
    logger.debug(f'partition_columns: {str(partition_columns)}')
    command = df.write.mode(mode)

    if merge_schema:
        command.option("mergeSchema", "true")

    if overwrite_schema:
        command.option("overwriteSchema", "true")

    # partitions
    if partition_columns is not None:
        command.partitionBy(partition_columns)

    command.saveAsTable(table_name)
    return True


def read_table(
        table_name: str,
        incremental_column: str = None,
        cutoff_value: str = None,
        filter_deleted: bool = False) -> DataFrame:
    """
    This function returns full or incremental dataset from the table.
    If incremental_column and cutoff_value are provided, it will run incremental.
    If _DATE field is in the table, it will use it for partition pruning during incremental.
    If _DELETED filed is in the table and filter_deleted = True, it will filter out soft deleted records.
    """

    table_name = get_table_name(table_name)

    if (incremental_column is None) is not (cutoff_value is None):
        raise Exception('incremental_column or cutoff_value not provided.')

    incremental = incremental_column is not None

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(table_name)

    # incremental
    if incremental:
        df = (
            df
            .filter(f"{incremental_column} >= '{cutoff_value}'")
        )

        if incremental_column == '_MODIFIED' and '_DATE' in df.columns:
            format_date = datetime.strptime(
                cutoff_value, '%Y-%m-%d %H:%M:%S').date()
            df = df.filter(f"_DATE >= '{str(format_date)}'")

    # soft delete
    if filter_deleted and '_DELETED' in df.columns:
        df = df.filter('_DELETED IS FALSE')

    logger = logging.getLogger('edm')
    message = f'table_name: {table_name}, incremental: {str(incremental)}, filter_deleted: {str(filter_deleted)}'
    message += f', rows: {df.count()}' if incremental else ''
    logger.debug(message)

    return df


def save_to_table(df, table_name, options={}):
    append_only = options.get('append_only', False)
    incremental = options.get('incremental', False)
    key_columns = options.get('key_columns')
    merge_schema = options.get('merge_schema', False)
    overwrite = options.get('overwrite', False)
    overwrite_schema = options.get('overwrite_schema', False)
    partition_columns = options.get('partition_columns')

    incremental = False if incremental is None else incremental

    table_name = get_table_name(table_name)

    # overwrite table
    if overwrite:
        options = {
            'overwrite': True,
            'overwrite_schema': True,
            'partition_columns': partition_columns}
        insert_into_table(df, table_name, options=options)

    # append only
    elif append_only:
        options = {
            'merge_schema': merge_schema,
            'overwrite_schema': overwrite_schema,
            'partition_columns': partition_columns}
        insert_into_table(df, table_name, options=options)

    # full load with overwrite
    elif incremental == False and key_columns is None:
        options = {
            'overwrite': True,
            'overwrite_schema': True,
            'partition_columns': partition_columns}
        insert_into_table(df, table_name, options=options)

    # merge to table
    elif key_columns is not None:
        options = {'merge_schema': merge_schema}
        merge_into_table(df, table_name, key_columns, options=options)

    else:
        raise Exception('no method to load data')


def split_database_name(database_name: str):

    database_name = database_name.lower()
    if database_name.count('.') == 2:
        catalog_name, database_name, _ = database_name.split('.')
    elif database_name.count('.') == 1:
        catalog_name, database_name = database_name.split('.')
    else:
        catalog_name = spark.sql(
            'SELECT current_catalog() AS c').collect()[0]['c']
    return (catalog_name, database_name)


def split_table_name(table_name: str):

    current_catalog = spark.sql(
        'SELECT current_catalog() AS c').collect()[0]['c']
    current_database = spark.sql(
        'SELECT current_database() AS db').collect()[0]['db']

    if table_name.count('.') == 2:
        catalog_name, database_name, table_name = table_name.lower().split('.')
    elif table_name.count('.') == 1:
        catalog_name = current_catalog
        database_name, table_name = table_name.lower().split('.')
    else:
        catalog_name = current_catalog
        database_name = current_database
        table_name = table_name.lower()

    return (catalog_name, database_name, table_name)


def table_exists(table_name: str) -> bool:
    try:
        table_name = get_table_name(table_name)
        database_name = get_database_name(table_name)
        table_names = get_tables(database_name)
        table_exists = table_name in table_names
    except Exception as e:
        table_exists = False

    return table_exists

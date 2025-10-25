from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f


def read_data(params: dict) -> dict[DataFrame]:

    source_tables = [
        's_core.account_col', 's_core.account_ebs', 's_core.account_kgd', 's_core.account_prms',
        's_core.account_sap', 's_core.account_sf', 's_core.account_tot']

    datasets = {}
    for source_table in source_tables:

        if params.get('incremental'):
            cutoff_value = get_cutoff_value(
                params.get('table_name'),
                source_table,
                params.get('prune_days'))

            datasets[source_table] = read_table(
                source_table, '_MODIFIED', cutoff_value)
        else:
            datasets[source_table] = read_table(source_table)

    return datasets


def save_data(df: DataFrame, params: dict):
    create_database(params.get('database_name'))

    options = {'partition_columns': params.get('partition_columns')}
    create_table(
        params.get('table_name'),
        df.schema,
        params.get('table_path'), options)

    options = {
        'append_only': params.get('append_only'),
        'incremental': params.get('incremental'),
        'key_columns': '_ID',
        'overwrite': params.get('overwrite'),
        'partition_columns': params.get('partition_columns')
    }
    save_to_table(df, params.get('table_name'), options)


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_agg')
    table_columns = list(table_schema.keys())

    # empty dataset creation
    schema = get_schema_from_config('s_core.account_agg')
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([], schema)

    for source_table_df in list(datasets.values()):
        source_table_df = (
            source_table_df
            .where(f.col('_ID') != '0')
            .transform(t.cast_data_types_from_schema, table_schema)
            .select(table_columns)
        )

        df = df.union(source_table_df)

    return (
        df
        .transform(t.fix_data_values)
        .transform(t.add_missing_columns, table_columns)
        .transform(t.cast_data_types_from_schema, table_schema)
        .select(table_columns)
        .transform(t.attach_unknown_record)
        .transform(t.sort_internal_columns_first)
    )


def update_keys(params: dict):
    update_key(
        params.get('table_name'), ['createdBy', '_SOURCE'], 'createdBy_ID', 'edm.user')
    update_key(
        params.get('table_name'), ['modifiedBy', '_SOURCE'], 'modifiedBy_ID', 'edm.user')


def update_timestamps(datasets: dict[DataFrame], params: dict):

    for source_table, source_table_df in list(datasets.items()):

        cutoff_value = get_max_value(source_table_df, '_MODIFIED')
        update_cutoff_value(
            cutoff_value, params.get('table_name'), source_table)

        update_run_datetime(
            params.get('run_datetime'), params.get('table_name'), source_table)

from databricks.functions.shared.runtime import is_adf_run
from databricks.functions.shared.schema import get_schema_as_json
from databricks.functions.shared.catalog import *


def footer(params: dict):

    if is_adf_run():

        # register table in legacy catalog
        _, database_name, table_name = split_table_name(
            params.get('table_name'))

        legacy_database_name = f'hive_metastore.{database_name}'
        create_database(legacy_database_name)

        legacy_table_name = f'hive_metastore.{database_name}.{table_name}'
        df = spark.table(params.get('table_name'))
        create_table(legacy_table_name, df.schema, params.get('table_path'))

    # export table schema
    table_schema = get_schema_as_json(params.get('table_name'))
    dbutils.fs.put(params.get('schema_path'), table_schema, True)

    logger = logging.getLogger('edm')
    logger.debug(f"metadata saved to: {params.get('schema_path')}")

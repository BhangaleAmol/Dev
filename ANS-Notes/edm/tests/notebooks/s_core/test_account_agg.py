import pytest
from databricks.functions.shared.tests import *
from databricks.functions.shared.transformations import (
    cast_data_types_from_schema,
    drop_key_columns,
    replace_null_string_with_null
)
from databricks.functions.shared.schema import get_dict_schema_from_config


def get_datasets():
    return {
        's_core.account_col': read_test_data('/s_core/account_agg/s_core.account_col.csv'),
        's_core.account_ebs': read_test_data('/s_core/account_agg/s_core.account_ebs.csv'),
        's_core.account_kgd': read_test_data('/s_core/account_agg/s_core.account_kgd.csv'),
        's_core.account_prms': read_test_data('/s_core/account_agg/s_core.account_prms.csv'),
        's_core.account_sap': read_test_data('/s_core/account_agg/s_core.account_sap.csv'),
        's_core.account_sf': read_test_data('/s_core/account_agg/s_core.account_sf.csv'),
        's_core.account_tot': read_test_data('/s_core/account_agg/s_core.account_tot.csv')
    }


@pytest.fixture(scope='module')
def details(request):
    return request.config.getoption('--details') == 'true'


@pytest.fixture(scope='module')
def datasets():
    return get_datasets()


@pytest.fixture(scope='module')
def expected_df():
    return read_test_data('/s_core/account_agg/_expected.csv')


def test_account_agg(datasets, expected_df, details):

    from databricks.functions.s_core.account_agg import transform_data

    drop_columns = [
        '_DATE', '_MODIFIED', '_PART', 'insertedOn', 'updatedOn']

    actual_df = (
        transform_data(datasets)
        .transform(drop_key_columns)
        .drop(*drop_columns)
    )

    table_schema = get_dict_schema_from_config('s_core.account_agg')
    excepted_df = (
        expected_df
        .transform(drop_key_columns)
        .drop(*drop_columns)
        .transform(replace_null_string_with_null)
        .transform(cast_data_types_from_schema, table_schema)
    )

    assert dataframes_equal(
        actual_df, excepted_df, options={'details': details})

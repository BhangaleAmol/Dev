import pytest
from databricks.functions.shared.tests import *
from databricks.functions.b_col.merge_to_delta import transform_data


def get_datasets():
    return {
        'vw_company': read_test_data('/b_col/vw_company.csv'),
        'vw_company_out': read_test_data('/b_col/vw_company_out.csv'),
        'vw_customers': read_test_data('/b_col/vw_customers.csv'),
        'vw_customers_out': read_test_data('/b_col/vw_customers_out.csv')
    }


@pytest.fixture(scope='module')
def details(request):
    return request.config.getoption('--details') == 'true'


@pytest.fixture(scope='module')
def datasets():
    return get_datasets()


@pytest.mark.parametrize("table_name", [('vw_company'), ('vw_customers')])
def test_merge_to_delta(datasets, table_name, details):
    drop_columns = ['_DATE', '_MODIFIED']
    actual_df = (
        transform_data({'df': datasets[table_name]})
        .drop(*drop_columns)
    )

    excepted_df = (
        datasets[f'{table_name}_out']
        .drop(*drop_columns)
    )

    assert dataframes_equal(
        actual_df,
        excepted_df,
        {'details': details, 'ignore_schema': True}
    )

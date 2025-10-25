import sys
sys.dont_write_bytecode = True


def pytest_addoption(parser):
    parser.addoption('--database_name', action='store', default='test')
    parser.addoption('--details', action='store', default='false')
    parser.addoption('--environment', action='store', default='default')


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "test: marks tests executed in test environment"
    )
    config.addinivalue_line(
        "markers", "prod: marks tests executed in prod environment"
    )

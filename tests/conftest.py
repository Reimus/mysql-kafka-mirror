def pytest_configure(config):
    config.addinivalue_line('markers', 'integration: docker-based integration tests')

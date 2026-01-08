from mysql_interceptor.config.settings import Settings

def test_capture_ddl_default_is_true():
    settings = Settings()
    assert settings.capture_ddl is True

def test_capture_ddl_from_env():
    import os
    from unittest import mock
    with mock.patch.dict(os.environ, {"INTERCEPTOR_CAPTURE_DDL": "false"}):
        settings = Settings.from_env()
        assert settings.capture_ddl is False
    
    with mock.patch.dict(os.environ, {"INTERCEPTOR_CAPTURE_DDL": "true"}):
        settings = Settings.from_env()
        assert settings.capture_ddl is True

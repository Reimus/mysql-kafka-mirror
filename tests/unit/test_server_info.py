from mysql_interceptor.utils import extract_server_info_best_effort

class _Boom:
    def __getattr__(self, name: str):
        raise RuntimeError("boom")

def test_extract_server_info_never_raises_and_sets_error():
    info, had_err = extract_server_info_best_effort(_Boom(), _Boom())
    assert info is None
    assert had_err is True

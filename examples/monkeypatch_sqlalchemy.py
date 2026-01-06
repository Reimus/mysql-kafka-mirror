from __future__ import annotations

from mysql_interceptor.monkeypatch import patch_sqlalchemy


def main() -> None:
    unpatch = patch_sqlalchemy()
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine("mysql+pymysql://root:secret@127.0.0.1:3306/test")
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO employees (first_name, hire_date) VALUES (:n, :d)"),
                [{"n": "Jane", "d": "2005-02-12"}, {"n": "Joe", "d": "2006-05-23"}],
            )
    finally:
        unpatch()


if __name__ == "__main__":
    main()

from __future__ import annotations

from mysql_interceptor.monkeypatch import patch_sqlalchemy


def main() -> None:
    unpatch = patch_sqlalchemy()
    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("mysql+pymysql://root:secret@127.0.0.1:3306/test")
        Session = sessionmaker(bind=engine)

        with Session.begin() as s:
            s.execute(text("SELECT 1"))
            s.execute(text("SELECT :x"), {"x": 2})
    finally:
        unpatch()


if __name__ == "__main__":
    main()

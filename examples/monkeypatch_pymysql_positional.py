from __future__ import annotations

from mysql_interceptor.monkeypatch import patch_pymysql


def main() -> None:
    unpatch = patch_pymysql()
    try:
        import pymysql

        # Positional connect (legacy)
        conn = pymysql.connect("127.0.0.1", "root", "secret", "test", 3306)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.commit()
        cur.close()
        conn.close()
    finally:
        unpatch()


if __name__ == "__main__":
    main()

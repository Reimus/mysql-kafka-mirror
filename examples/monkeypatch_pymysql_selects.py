from __future__ import annotations

from mysql_interceptor.monkeypatch import patch_pymysql


def main() -> None:
    unpatch = patch_pymysql()
    try:
        import pymysql

        conn = pymysql.connect(host="127.0.0.1", user="root", password="secret", database="test", port=3306)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.execute("SELECT 2")
        conn.commit()
        cur.close()
        conn.close()
    finally:
        unpatch()


if __name__ == "__main__":
    main()

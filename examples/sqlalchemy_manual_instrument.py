from __future__ import annotations

from mysql_interceptor.config.settings import Settings
from mysql_interceptor.connect import _build_default_kafka_publisher
from mysql_interceptor.kafka.publisher import StdoutPublisher
from mysql_interceptor.sqlalchemy_interceptor import instrument_engine


def main() -> None:
    from sqlalchemy import create_engine, text

    settings = Settings.from_env()
    publisher = StdoutPublisher()
    if settings.kafka_bootstrap_servers:
        publisher = _build_default_kafka_publisher(settings)

    engine = create_engine("mysql+pymysql://root:secret@127.0.0.1:3306/test")
    instrument_engine(engine=engine, publisher=publisher, settings=settings)

    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))


if __name__ == "__main__":
    main()

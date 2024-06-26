from contextlib import contextmanager
import psycopg2

from etl.settings import logger
from etl.shared.backoff import backoff


class PostgresService:
    """Установка и закрытие соединения с Postgres"""

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    @contextmanager
    def conn_context(self, dsn: str):
        try:
            conn = psycopg2.connect(dsn)
            logger.info("Подключились к постгрес")
            yield conn
        except Exception as e:
            logger.error(f"Не удалось подключиться к базe: {e}")
            raise
        finally:
            conn.close()

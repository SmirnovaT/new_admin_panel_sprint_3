import time

from etl import settings
from etl.services.base_storage_service import State, JsonFileStorage
from etl.services.elastic_load import ElasticService
from etl.services.film_work_service import FilmWorkService
from etl.services.postgres_service import PostgresService
from etl.settings import logger
from etl.shared.get_date import date_now
from etl.shared.get_file_path import get_path_file


class MainService:
    """Сервис получения обновленной информации по фильмам из постгрес,
    преобразоание данных и запись в эластик"""

    service = FilmWorkService()
    elastic_service = ElasticService()

    def main(self, pg_conn):
        date_state = self.get_state()
        date_end = date_now()

        film_dicts = self.service.film_work_service(
            pg_conn, date_start=date_state, date_end=date_end
        )
        success = True

        try:
            first_item = next(film_dicts)
        except StopIteration:
            logger.error(f"Нет данных для обновления")
            success = False
        else:
            self.elastic_service.elastic_load(first_item)
        for i in film_dicts:
            result = self.elastic_service.elastic_load(i)
            if result is None:
                success = False
                logger.error(f"Данные в эластик не будут загружены")
        if success:
            self.update_state(date_end)

    def update_state(self, date):
        file = get_path_file("", "state.json")
        storage = JsonFileStorage(file)
        state_manager = State(storage)
        state_manager.set_state(key="current_date", value=date)
        saved_date = state_manager.get_state("current_date")
        logger.info(f"Данные из постгрес загружены в эластик до даты: {saved_date}")
        return saved_date

    def get_state(self) -> str:
        file = get_path_file("", "state.json")
        storage = JsonFileStorage(file)
        state_manager = State(storage)
        state_date = state_manager.get_state("current_date")
        return state_date


if __name__ == "__main__":
    postgres_service = PostgresService()
    main_service = MainService()
    while True:
        with postgres_service.conn_context(settings.POSTGRES_DSN) as pg_conn:
            main_service.main(pg_conn)
        time.sleep(3 * 60)

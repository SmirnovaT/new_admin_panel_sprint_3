import logging

from etl import settings
from etl.services.base_storage_service import State, BaseStorage, JsonFileStorage
from etl.services.elastic_load import ElasticService
from etl.services.film_work_service import FilmWorkService
from etl.services.postgres_service import PostgresService
from etl.shared.get_date import get_date
from etl.shared.get_file_path import get_path_file


class MainService:
    service = FilmWorkService()
    elastic_service = ElasticService()

    def main(self, pg_conn):
        state = self.get_state()
        date_end = get_date()
        film_dicts = self.service.film_work_service(pg_conn, state, date_end)
        if film_dicts:
            save_elastic = self.elastic_service.elastic_load(film_dicts)
            if save_elastic:
                pass
                # Обновить стейт

    def update_state(self):
        one_second_ago = get_date()
        file = get_path_file("", "state.json")
        storage = JsonFileStorage(file)
        state_manager = State(storage)
        state_manager.set_state(key="current_date", value=one_second_ago)
        saved_date = state_manager.get_state("current_date")
        logging.info(f"Началась загрузка данных в эластик до даты: {saved_date}")
        return saved_date

    def get_state(self):
        file = get_path_file("", "state.json")
        storage = JsonFileStorage(file)
        state_manager = State(storage)
        state_date = state_manager.get_state("current_date")
        return state_date


if __name__ == "__main__":
    postgres_service = PostgresService()
    main_service = MainService()
    with postgres_service.conn_context(settings.POSTGRES_DSN) as pg_conn:
        main_service.main(pg_conn)

from etl.dto.film_work_dto import FilmWorkDto
from etl.repositories.film_work_repository import FilmWorkRepository
from etl.settings import logger
from itertools import zip_longest, chain
from typing import Generator, List, Tuple
from uuid import UUID


class FilmWorkService:
    """Сервис получения и обработки обновленных записей по фильмам."""

    repository = FilmWorkRepository()

    def film_work_service(
        self, pg_conn, date_start: str, date_end: str
    ) -> Generator[List[dict], None, None]:
        try:
            film_work_ids = self.film_work(pg_conn, date_start, date_end, "film_work")
            film_ids_by_persons = self.get_film_work_by_table(
                pg_conn, date_start, date_end, "person"
            )
            film_ids_by_genres = self.get_film_work_by_table(
                pg_conn, date_start, date_end, "genre"
            )
            unique_set = self.common_generator(
                film_work_ids, film_ids_by_persons, film_ids_by_genres
            )

            for i in unique_set:
                db_result = self.repository.full_info_film_work(pg_conn, i)
                film_dicts = self.mapping_data(db_result)
                yield film_dicts
        except Exception as e:
            logger.error(f"Ошибка при получении обновленных записей: {e}")

    def film_work(
        self, pg_conn, state: str, date_end: str, table: str
    ) -> Generator[List[Tuple[UUID]], None, None]:
        """Функция получения id записей из таблицы film_work, которые обновились"""
        film_work_ids = self.repository.get_update_ids(
            pg_conn, table, date_start=state, date_end=date_end
        )
        if len(list(film_work_ids)) == 0:
            logger.info(f"Нет обновленных записей в таблице {table}")
        yield from film_work_ids

    def get_film_work_by_table(
        self, pg_conn, state: str, date_end: str, table: str
    ) -> Generator[List[Tuple[UUID]], None, None]:
        """Функция получения film_work_id по обновленным записям из переданной таблицы (genre, person)"""
        table_ids = self.repository.get_update_ids(
            pg_conn, table, date_start=state, date_end=date_end
        )
        list_ids = []
        for row_list in table_ids:
            for row in row_list:
                ids = str(row[0]).replace("-", "")
                list_ids.append(ids)
        if len(list_ids) == 0:
            logger.info(f"Нет обновленных записей в таблице {table}")
            return None
        film_work_ids = self.repository.execute_query(pg_conn, tuple(list_ids), table)
        yield from film_work_ids

    def common_generator(
        self,
        film_work_ids: Generator[List[Tuple[UUID]], None, None],
        film_ids_by_persons: Generator[List[Tuple[UUID]], None, None],
        film_ids_by_genres: Generator[List[Tuple[UUID]], None, None],
    ) -> Generator[Tuple[str], None, None]:
        """Функция, объединяющая возвращаемые зачения генераторов в один кортеж и удалющая дубли"""
        for row_1, row_2, row_3 in zip_longest(
            film_work_ids, film_ids_by_persons, film_ids_by_genres, fillvalue=[]
        ):
            unique_set = set()
            for item in chain(row_1, row_2, row_3):
                unique_set.add(str(item[0]).replace("-", ""))
            yield tuple(unique_set)

    def mapping_data(self, rows: list) -> list:
        """Преобразование данный для записи в индекс эластика"""
        films_dict = {}
        for row in rows:
            fw_id = str(row["fw_id"])
            if fw_id not in films_dict:
                films_dict[fw_id] = {
                    "id": fw_id,
                    "rating": row["rating"],
                    "title": row["title"],
                    "description": row["description"],
                    "directors_names": [],
                    "writers_names": [],
                    "actors_names": [],
                    "genres": row["genres"].strip().split(),
                    "directors": [],
                    "actors": [],
                    "writers": [],
                }
            if row["full_name"]:
                person_info = {"id": str(row["p_id"]), "full_name": row["full_name"]}
                if row["role"] == "director":
                    films_dict[fw_id]["directors"].append(person_info)
                    films_dict[fw_id]["directors_names"].append(row["full_name"])
                elif row["role"] == "actor":
                    films_dict[fw_id]["actors"].append(person_info)
                    films_dict[fw_id]["actors_names"].append(row["full_name"])
                elif row["role"] == "writer":
                    films_dict[fw_id]["writers"].append(person_info)
                    films_dict[fw_id]["writers_names"].append(row["full_name"])
        film_work_dto = [(FilmWorkDto(**row)) for row in films_dict.values()]
        film_dicts = [i.dict() for i in film_work_dto]
        return film_dicts

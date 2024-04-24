from typing import Any

from etl import settings
from etl.dto.film_work_dto import FilmWorkDto
from etl.repositories.film_work_repository import FilmWorkRepository
from etl.settings import logger


class FilmWorkService:
    repository = FilmWorkRepository()

    def film_work_service(self, pg_conn, state, date_end):
        unique_film_ids = self.unique_film_id(pg_conn, state, date_end)
        if len(unique_film_ids) > 0:
            full_info = self.repository.full_info_film_work(pg_conn, unique_film_ids)
            film_dicts = self.mapping_data(full_info)
            return film_dicts
        else:
            logger.info("Нет данных для обновления")
            return None

    def mapping_data(self, rows: Any) -> list:
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

    def unique_film_id(self, pg_conn, state, date_end):
        unique_set = set()
        for table in settings.CHECK_TABLE:
            rows = self.get_from_postgres(
                pg_conn, table, date_start=state, date_end=date_end
            )
            if rows and len(rows) > 0:
                for t in rows:
                    unique_set.add(t)
            else:
                logger.info(f"В таблице {table} нет данных для обновления")
        return tuple(unique_set)

    def get_from_postgres(self, pg_conn, table, date_start, date_end):
        update_ids = self.repository.get_update_ids(
            pg_conn, table, date_start, date_end
        )
        if table == "film_work" and update_ids:
            return update_ids
        if table == "person" and update_ids:
            film_work_ids = self.repository.person_film_work(pg_conn, update_ids)
            return film_work_ids
        elif table == "genre" and update_ids:
            film_work_ids = self.repository.genre_film_work(pg_conn, update_ids)
            return film_work_ids

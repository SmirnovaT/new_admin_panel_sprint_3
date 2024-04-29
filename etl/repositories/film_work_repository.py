from uuid import UUID
from typing import Generator, List, Tuple
import psycopg2
import psycopg2.extras

from etl import settings


class FilmWorkRepository:
    def get_update_ids(
        self, pg_conn, table: str, date_start: str, date_end: str
    ) -> Generator[List[Tuple[UUID]], None, None]:
        """Получение обновленных id за переданный период времени из таблиц (film_work, genre, person)"""
        cur = pg_conn.cursor()
        cur.execute(
            f"""SELECT id FROM content.{table}
        WHERE updated_at >= '{date_start}' AND updated_at <= '{date_end}'
        """
        )
        while rows := cur.fetchmany(settings.BUTCH_SIZE):
            yield rows

    def execute_query(
        self, pg_conn, update_ids: tuple, table: str
    ) -> Generator[List[Tuple[UUID]], None, None]:
        """Общая функция для получения film_work_ids по genre_ids или по person_ids
        в зависимости от переданной таблицы - person или genre"""

        query = f"""SELECT DISTINCT fw.id
        FROM content.film_work fw 
        LEFT JOIN content.{table}_film_work tfw ON fw.id = tfw.film_work_id
        WHERE tfw.{table}_id """
        full_query = self.count_tuple(query, update_ids)
        cur = pg_conn.cursor()
        cur.execute(full_query)
        while rows := cur.fetchmany(settings.BUTCH_SIZE):
            yield rows

    def full_info_film_work(self, pg_conn, film_ids: tuple) -> List[list]:
        """Получение полной информации по фильмам по id film_work"""
        cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = f"""SELECT
        fw.id as fw_id,
        fw.title,
        fw.description,
        fw.rating,
        pfw.role,
        p.id as p_id, p.full_name,
        string_agg(DISTINCT CASE WHEN pfw.role = 'director' THEN p.full_name END, ',') 
        FILTER (WHERE pfw.role = 'director') AS directors_names,
        string_agg(DISTINCT CASE WHEN pfw.role = 'writer' THEN p.full_name END, ',') 
        FILTER (WHERE pfw.role ='writer') AS writers_names,
        string_agg(DISTINCT CASE WHEN pfw.role = 'actor' THEN p.full_name END,',') 
        FILTER (WHERE pfw.role = 'actor') AS actors_names,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT g.name) 
        FILTER (WHERE g.name IS NOT NULL), ', ') AS genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id """

        group_by = " GROUP BY fw.id, p.id, pfw.role"
        if len(film_ids) == 1:
            query += f"= '{film_ids[0]}'{group_by}"
        else:
            query += f"IN {str(film_ids)}{group_by}"

        cur.execute(query)
        rows = cur.fetchall()
        return rows

    def count_tuple(self, query: str, update_ids: tuple) -> str:
        """Формирование запроса в зависимости от количества данных в кортеже"""
        if len(update_ids) == 1:
            query += f"= '{update_ids[0]}'"
        else:
            query += f"IN {str(update_ids)}"
        return query

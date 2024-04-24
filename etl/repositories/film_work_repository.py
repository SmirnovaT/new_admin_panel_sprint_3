import psycopg2.extras

import psycopg2

import psycopg2.extensions
import psycopg2.extras

psycopg2.extras.register_uuid()


class FilmWorkRepository:
    def get_update_ids(self, pg_conn, table, date_start, date_end):
        cur = pg_conn.cursor()
        cur.execute(
            f"""SELECT id FROM content.{table}
        WHERE updated_at >= '{date_start}' AND updated_at <= '{date_end}'
        """
        )
        rows = cur.fetchall()
        result = tuple([str(row[0]) for row in rows])
        return result

    def person_film_work(self, pg_conn, update_ids):
        cur = pg_conn.cursor()
        cur.execute(
            f"""SELECT fw.id
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
        WHERE pfw.person_id IN {update_ids}"""
        )
        rows = cur.fetchall()
        result = tuple([str(row[0]) for row in rows])
        return result

    def genre_film_work(self, pg_conn, update_ids):
        cur = pg_conn.cursor()
        cur.execute(
            f"""SELECT fw.id
        FROM content.film_work fw 
        LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
        WHERE gfw.genre_id IN {update_ids}"""
        )
        rows = cur.fetchall()
        result = tuple([str(row[0]) for row in rows])
        return result

    def full_info_film_work(self, pg_conn, film_ids):
        cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            f"""SELECT
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
        WHERE fw.id IN {film_ids}
        GROUP BY
        fw.id, p.id, pfw.role"""
        )
        rows = cur.fetchall()
        return rows

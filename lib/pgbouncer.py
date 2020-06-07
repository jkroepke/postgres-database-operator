import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def enabled() -> bool:
    return os.getenv('PGBOUNCER_AUTH_QUERY_SUPPORT') == 'true'


def connect_to_postgres() -> psycopg2:
    con = psycopg2.connect(
        host=os.getenv('PGBOUNCER_AUTH_QUERY_HOST'),
        port=os.getenv('PGBOUNCER_AUTH_QUERY_POST', '5432'),
        user=os.getenv('PGBOUNCER_AUTH_QUERY_USER'),
        password=os.getenv('PGBOUNCER_AUTH_QUERY_PASSWORD'),
        database=os.getenv('PGBOUNCER_AUTH_QUERY_DATABASE')
    )

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return con


def create_database(con: psycopg2):
    with con.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS pg_shadow (usename text, passwd text);")


def insert_db_username(con: psycopg2, db_username: str, db_password: str):
    with con.cursor() as cur:
        cur.execute("INSERT INTO pg_shadow VALUES (%s, %s);", (db_username, db_password))


def remove_db_username(con: psycopg2, db_username: str):
    with con.cursor() as cur:
        cur.execute("DELETE FROM pg_shadow WHERE usename=%s;", (db_username,))

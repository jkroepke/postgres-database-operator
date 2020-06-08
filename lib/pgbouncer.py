import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, encrypt_password


def enabled() -> bool:
    return os.getenv('PGBOUNCER_AUTH_QUERY_SUPPORT') == 'true'


def connect_to_postgres() -> psycopg2:
    con = psycopg2.connect(
        host=os.getenv('PGBOUNCER_AUTH_QUERY_DB_HOST'),
        port=os.getenv('PGBOUNCER_AUTH_QUERY_DB_POST', '5432'),
        user=os.getenv('PGBOUNCER_AUTH_QUERY_DB_USER'),
        password=os.getenv('PGBOUNCER_AUTH_QUERY_DB_PASSWORD'),
        database=os.getenv('PGBOUNCER_AUTH_QUERY_DB_NAME')
    )

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return con


def create_database(con: psycopg2):
    with con.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS pgbouncer_shadow (usename text, passwd text);")


def insert_db_username(con: psycopg2, name: str, password: str):
    password_encrypted = encrypt_password(password=password, user=name, scope=con, algorithm='md5')

    with con.cursor() as cur:
        cur.execute("INSERT INTO pgbouncer_shadow VALUES (%s, %s);", (name, password_encrypted))


def remove_db_username(con: psycopg2, db_username: str):
    with con.cursor() as cur:
        cur.execute("DELETE FROM pgbouncer_shadow WHERE usename=%s;", (db_username,))

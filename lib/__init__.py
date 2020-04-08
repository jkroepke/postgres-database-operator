import os
import random
import string
from typing import Optional

import pykube
import psycopg2
from hashlib import sha1

from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, encrypt_password


def generate_password(length: int) -> str:
    # https://gist.github.com/23maverick23/4131896
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def generate_db_name(namespace: str, name: str) -> str:
    value = "_".join([namespace, name]).encode('utf-8')
    return sha1(value).hexdigest()


def generate_db_username(namespace: str, name: str) -> str:
    return generate_db_name(namespace, name)


def connect_to_postgres() -> psycopg2:
    con = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_POST', '5432'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DATABASE')
    )

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return con


def create_db_username(con: psycopg2, name: str, password: str) -> None:
    encrypted_password = encrypt_password(password=password, user=name, scope=con, algorithm='md5')

    with con.cursor() as cur:
        cur.execute(sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD {}").format(
            sql.Identifier(name),
            sql.Literal(encrypted_password)
        ))


def grant_role_to_current_user(con: psycopg2, new_user: str, operator_user: str) -> None:
    # https://stackoverflow.com/a/34898033/8087167
    with con.cursor() as cur:
        cur.execute(sql.SQL("GRANT {} TO {};").format(
            sql.Identifier(new_user),
            sql.Identifier(operator_user)
        ))


def create_db(con: psycopg2, name: str, owner: str, encoding: Optional[str], lc_collate: Optional[str],
              lc_ctype: Optional[str], comment: Optional[str]) -> None:
    query = "CREATE DATABASE {} OWNER {}"
    query += " ENCODING {}".format(encoding) if encoding else ''
    query += " LC_COLLATE {}".format(lc_collate) if lc_collate else ''
    query += " LC_CTYPE {}".format(lc_ctype) if lc_ctype else ''

    with con.cursor() as cur:
        cur.execute(sql.SQL(query).format(
            sql.Identifier(name),
            sql.Identifier(owner)
        ))

        # no connection privileged by default to preserve multi tenancy
        cur.execute(sql.SQL("REVOKE connect ON DATABASE {} FROM PUBLIC;").format(
            sql.Identifier(name)
        ))

        if comment:
            cur.execute(sql.SQL("COMMENT ON DATABASE {} IS {};").format(
                sql.Identifier(name),
                sql.Literal(comment),
            ))


def grant_connect_on_db(con: psycopg2, database: str, username: str) -> None:
    with con.cursor() as cur:
        cur.execute(sql.SQL("GRANT connect ON DATABASE {} TO {};").format(
            sql.Identifier(database),
            sql.Identifier(username)
        ))


def db_exists(con: psycopg2, name: str) -> bool:
    with con.cursor() as cur:
        cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(%s);", (name,))

        return cur.fetchone() is not None


def delete_db(con: psycopg2, name: str, owner: str) -> None:
    if not db_exists(con, name):
        return

    # Stop accepting new connections
    with con.cursor() as cur:
        cur.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC, {};").format(
            sql.Identifier(name),
            sql.Identifier(owner)
        ))

        cur.execute(sql.SQL("DROP DATABASE {};").format(
            sql.Identifier(name)
        ))


def delete_db_username(con: psycopg2, username: str) -> None:
    with con.cursor() as cur:
        cur.execute(sql.SQL("DROP USER IF EXISTS {};").format(
            sql.Identifier(username)
        ))


def generate_kubernetes_secret(name: str, db_host: str, db_port: str, db_name: str, db_username: str,
                               db_password: str) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
        },
        "stringData": {
            'DB_HOSTNAME': db_host,
            'DB_PORT': db_port,
            'DB_NAME': db_name,
            'DB_USER': db_username,
            'DB_PASSWORD': db_password,
        }
    }


def create_kubernetes_secret(doc: dict) -> pykube.Secret:
    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    secret = pykube.Secret(api, doc)
    secret.create()
    api.session.close()

    return secret

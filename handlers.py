#!/usr/bin/env python3

import os
import random
import string
import kopf
import kubernetes.client
import psycopg2

from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, encrypt_password


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


@kopf.on.startup()
async def startup_fn_simple(**_):
    conn = connect_to_postgres()
    conn.close()


@kopf.on.create('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def create_fn(spec: dict, meta: dict, **_):
    name = meta.get('name')
    try:
        conn = connect_to_postgres()
    except Exception as e:
        message = "Can't connect to database: " + str(e)
        raise kopf.TemporaryError(message)

    # https://gist.github.com/23maverick23/4131896
    password = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(32))

    try:
        encrypted_password = encrypt_password(password=password, user=name, scope=conn, algorithm='md5')

        query = sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD {}").format(
            sql.Identifier(name),
            sql.Literal(encrypted_password)
        )

        with conn.cursor() as cur:
            cur.execute(query)

        if os.getenv('RDS_WORKAROUND', 'false') == 'true':
            grant_query = sql.SQL("GRANT {} TO {};").format(
                sql.Identifier(name),
                sql.Identifier(os.getenv('POSTGRES_USER'))
            )
            with conn.cursor() as cur:
                cur.execute(grant_query)

        message = "Created user {0}.".format(name)
        kopf.info(spec, reason="Create user", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't create user: " + str(e)
        raise kopf.PermanentError(message)

    try:
        raw_query = "CREATE DATABASE {} OWNER {}"

        encoding = spec.get('encoding')
        if encoding:
            raw_query += " ENCODING {}".format(encoding)

        lc_collate = spec.get('lcCollate')
        if lc_collate:
            raw_query += " LC_COLLATE {}".format(lc_collate)

        lc_ctype = spec.get('lcCollate')
        if lc_ctype:
            raw_query += " LC_CTYPE {}".format(lc_ctype)

        query = sql.SQL(raw_query).format(
            sql.Identifier(name),
            sql.Identifier(name)
        )

        comment_query = sql.SQL("COMMENT ON DATABASE {} IS {};").format(
            sql.Identifier(name),
            sql.Literal(
                "@".join([
                    meta.get('namespace'),
                    meta.get('uid')
                ])
            ),
        )

        revoke_query = sql.SQL("REVOKE connect ON DATABASE {} FROM PUBLIC;").format(
            sql.Identifier(name)
        )

        grant_operator_query = sql.SQL("GRANT connect ON DATABASE {} TO {};").format(
            sql.Identifier(name),
            sql.Identifier(os.getenv('POSTGRES_USER'))
        )

        grant_user_query = sql.SQL("GRANT connect ON DATABASE {} TO {};").format(
            sql.Identifier(name),
            sql.Identifier(name)
        )

        with conn.cursor() as cur:
            cur.execute(query)
            cur.execute(comment_query)
            cur.execute(revoke_query)
            cur.execute(grant_operator_query)
            cur.execute(grant_user_query)

        message = "Created database " + name + "."
        kopf.info(spec, reason="Create database", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't create database: " + str(e)
        raise kopf.PermanentError(message)

    secret_name = spec.get('secretName')

    secret_data = {
        'DB_HOSTNAME': os.getenv('POSTGRES_HOST'),
        'DB_PORT': os.getenv('POSTGRES_POST', '5432'),
        'DB_DATABASE': name,
        'DB_USER': name,
        'DB_PASSWORD': password,
    }

    secret = kubernetes.client.V1Secret(
        metadata=kubernetes.client.V1ObjectMeta(name=secret_name),
        string_data=secret_data,
    )
    api = kubernetes.client.ApiClient()
    doc = api.sanitize_for_serialization(secret)

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(doc)

    # Actually create an object by requesting the Kubernetes API.
    api = kubernetes.client.CoreV1Api()
    try:
        response = api.create_namespaced_secret(namespace=meta.get('namespace'), body=doc)
        kopf.info(response.to_dict(), reason='Secret create', message='Secret {} created'.format(secret_name))
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't create secret: " + str(e)
        raise kopf.PermanentError(message)

    conn.close()

    return {'children': response.metadata.name}


@kopf.on.update('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def update_fn(**_):
    pass


@kopf.on.delete('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def delete_fn(spec: dict, meta: dict, **_):
    name = meta.get('name')
    try:
        conn = connect_to_postgres()
    except Exception as e:
        message = "Can't connect to database: " + str(e)
        raise kopf.TemporaryError(message)

    try:
        # Check if DB exists
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(%s);", (name, ))

            db_exists = cur.fetchone()

        if db_exists is not None:
            # Stop accepting new connections
            revoke_query = sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC, {};").format(
                sql.Identifier(name),
                sql.Identifier(name)
            )

            with conn.cursor() as cur:
                cur.execute(revoke_query)

            query = sql.SQL("DROP DATABASE {};").format(
                sql.Identifier(name)
            )

            with conn.cursor() as cur:
                cur.execute(query)

            message = "Delete database " + name + "."
            kopf.info(spec, reason="Delete database", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't delete postgresql database: " + str(e)
        raise kopf.TemporaryError(message, delay=10.0)

    try:
        query = sql.SQL("DROP USER {};").format(
            sql.Identifier(name)
        )

        with conn.cursor() as cur:
            cur.execute(query)

        message = "Delete user " + name + "."
        kopf.info(spec, reason="Delete user", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't delete postgresql user: " + str(e)
        raise kopf.TemporaryError(message, delay=10.0)

    conn.close()
    return {'message': message}

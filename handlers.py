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
async def startup(**_):
    conn = connect_to_postgres()
    conn.close()


@kopf.on.create('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def create(spec: dict, meta: dict, **_):
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

        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD {}").format(
                sql.Identifier(name),
                sql.Literal(encrypted_password)
            ))

        if os.getenv('RDS_WORKAROUND', 'false') == 'true':
            with conn.cursor() as cur:
                cur.execute(sql.SQL("GRANT {} TO {};").format(
                    sql.Identifier(name),
                    sql.Identifier(os.getenv('POSTGRES_USER'))
                ))

        message = "Created user {0}.".format(name)
        kopf.info(spec, reason="Create user", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't create user: " + str(e)
        raise kopf.PermanentError(message)

    try:
        encoding = spec.get('encoding')
        lc_collate = spec.get('lcCollate')
        lc_ctype = spec.get('lcCollate')

        query = "CREATE DATABASE {} OWNER {}"
        query += " ENCODING {}".format(encoding) if encoding else ''
        query += " LC_COLLATE {}".format(lc_collate) if lc_collate else ''
        query += " LC_CTYPE {}".format(lc_ctype) if lc_ctype else ''

        with conn.cursor() as cur:
            cur.execute(sql.SQL(query).format(
                sql.Identifier(name),
                sql.Identifier(name)
            ))

            cur.execute(sql.SQL("COMMENT ON DATABASE {} IS {};").format(
                sql.Identifier(name),
                sql.Literal(
                    "@".join([
                        meta.get('namespace'),
                        meta.get('uid')
                    ])
                ),
            ))

            cur.execute(sql.SQL("REVOKE connect ON DATABASE {} FROM PUBLIC;").format(
                sql.Identifier(name)
            ))

            cur.execute(sql.SQL("GRANT connect ON DATABASE {} TO {};").format(
                sql.Identifier(name),
                sql.Identifier(os.getenv('POSTGRES_USER'))
            ))

            cur.execute(sql.SQL("GRANT connect ON DATABASE {} TO {};").format(
                sql.Identifier(name),
                sql.Identifier(name)
            ))

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
def update(**_):
    pass


@kopf.on.delete('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def delete(spec: dict, meta: dict, **_):
    name = meta.get('name')
    try:
        conn = connect_to_postgres()
    except Exception as e:
        message = "Can't connect to database: " + str(e)
        raise kopf.TemporaryError(message)

    try:
        # Check if DB exists
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(%s);", (name,))

            db_exists = cur.fetchone()

        if db_exists is not None:
            # Stop accepting new connections
            with conn.cursor() as cur:
                cur.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC, {};").format(
                    sql.Identifier(name),
                    sql.Identifier(name)
                ))

                cur.execute(sql.SQL("DROP DATABASE {};").format(
                    sql.Identifier(name)
                ))

            message = "Delete database {}.".format(name)
            kopf.info(spec, reason="Database deleted", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't delete postgresql database: " + str(e)
        raise kopf.TemporaryError(message, delay=10.0)

    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP USER {};").format(
                sql.Identifier(name)
            ))

        message = "Delete user {}.".format(name)
        kopf.info(spec, reason="Delete user", message=message)
    except Exception as e:
        if conn:
            conn.close()

        message = "Can't delete postgresql user: " + str(e)
        raise kopf.TemporaryError(message, delay=10.0)

    conn.close()
    return {'message': message}

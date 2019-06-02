#!/usr/bin/env python3

import os
import argparse
import kopf
import psycopg2
import string
import random
import kubernetes.client
import yaml

class PostgresqlDatabaseService:
    conn = None

    def __init__(self, postgresql_host: str, postgresql_port: int, postgresql_user: str, postgresql_password: str):
        self.conn = psycopg2.connect(
            host=postgresql_host,
            port=postgresql_port,
            user=postgresql_user,
            password=postgresql_password
        )

    def close(self):
        self.conn.close()

    def create_user(self, name: str, password: str):
        query = f"CREATE USER {0} WITH UNENCRYPTED PASSWORD '{1}'".format(name, password)

        with self.conn.cursor() as cursor:
            cursor.execute(query)

    def create_database(self, name: str, owner: str,
                        encoding: str, lc_collate: str, lc_ctype: str):

        query = f"CREATE DATABASE {0}".format(name)

        if not owner:
            query+= f" OWNER {0}".format(owner)

        if not encoding:
            query+= f" ENCODING {0}".format(encoding)

        if not lc_collate:
            query+= f" LC_COLLATE {0}".format(lc_collate)

        if not lc_ctype:
            query+= f" LC_CTYPE {0}".format(lc_ctype)

        with self.conn.cursor() as cursor:
            cursor.execute(query)

def password_generator(size=8, chars=string.ascii_letters + string.digits):
    """
    Returns a string of random characters, useful in generating temporary
    passwords for automated password resets.

    size: default=8; override to provide smaller/larger passwords
    chars: default=A-Za-z0-9; override to provide more/less diversity

    Credit: Ignacio Vasquez-Abrams
    Source: http://stackoverflow.com/a/2257449
    Source: https://gist.github.com/23maverick23/4131896
    """
    return ''.join(random.choice(chars) for i in range(size))

def get_service() -> PostgresqlDatabaseService:
    return PostgresqlDatabaseService(
        postgresql_host=args.host,
        postgresql_port=args.port,
        postgresql_user=args.user,
        postgresql_password=args.password
    )

@kopf.on.create('k8s.jkroepke.de', 'v1', 'postgresqldatabase')
def create_fn(body: dict, meta: dict, status: dict, logger, **kwargs):
    name = meta.get('name')
    create_user = body.get('createUser')

    service = get_service()

    secret_data = {'database-name': name }

    if create_user:
        password = password_generator(32)

        try:
            service.create_user(name, password)
        except Exception as e:
            message = 'Failed to create postgresql user.'
            logger.error('Failed to create postgresql user.')
            logger.error(e)
            return {'message': message}

        secret_data['database-user'] = name
        secret_data['database-password'] = password

    try:
        service.create_database(name)
        message = f"Created database {0}.".format(name)
        logger.info(message)
    except Exception as e:
        message = 'Failed to create postgresql database.'
        logger.error('Failed to create postgresql database.')
        logger.error(e)
        return {'message': message}

    secret = kubernetes.client.V1Secret(
        api_version="v1",
        kind="Secret",
        metadata={'name': f"postgresql-database-{0}".format(name) },
        string_data=secret_data,
    )

    doc = yaml.dump(secret.to_dict())

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(doc, owner=body)

    # Actually create an object by requesting the Kubernetes API.
    api = kubernetes.client.CoreV1Api()
    response = api.create_namespaced_secret(namespace=doc['metadata']['namespace'], body=doc)

    return {'children': [response.metadata.uid]}


@kopf.on.update('k8s.jkroepke.de', 'v1', 'postgresqldatabase')
def update_fn(body: dict, meta: dict, status: dict, logger, **kwargs):
    return {'message': 'Updating the crd is not implemented yet!'}


@kopf.on.delete('k8s.jkroepke.de', 'v1', 'postgresqldatabase')
def delete_fn(body: dict, meta: dict, status: dict, logger, **kwargs):
    name = meta.get('name')
    create_user = body.get('createUser')

    service = get_service()

    try:
        service.delete_database(name)
        message = f"Delete database {0}.".format(name)
        logger.info(message)
    except Exception as e:
        message = 'Failed to delete postgresql database.'
        logger.error('Failed to delete postgresql database.')
        logger.error(e)

    if create_user:
        try:
            service.delete_user(name)
            message = f"Delete user {0}.".format(name)
            logger.info(message)
        except Exception as e:
            message = 'Failed to delete postgresql user.'
            logger.error('Failed to delete postgresql user.')
            logger.error(e)

    return {'message': message}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Kubernetes operator to create databases inside a running postgresql instance.')
    parser.add_argument('--host', help='PostgresSQL host (environment variable: POSTGRESQL_HOST)', default=os.getenv('POSTGRESQL_HOST'))
    parser.add_argument('--port', help='PostgresSQL port (environment variable: POSTGRESQL_POST)', type=int, default=int(os.getenv('POSTGRESQL_POST', 5432)))
    parser.add_argument('--user', help='PostgresSQL user (environment variable: POSTGRESQL_USER)', default=os.getenv('POSTGRESQL_USER'))
    parser.add_argument('--password', help='PostgresSQL password (environment variable: POSTGRESQL_PASSWORD)', default=os.getenv('POSTGRESQL_PASSWORD'))

    args = parser.parse_args()

    if not args.host or not args.port or not args.user or not args.password:
        exit(parser.print_usage())

    # Just test the credentials given by environment. Fail fast here.
    connTest = get_service()
    connTest.close()
    del connTest

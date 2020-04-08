#!/usr/bin/env python3

import os
import kopf
import lib


@kopf.on.startup()
async def startup(**_):
    con = lib.connect_to_postgres()
    con.close()


@kopf.on.create('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def create(spec: dict, meta: dict, **_):
    db_name = lib.generate_db_name(meta.get('namespace'), meta.get('name'))
    db_username = lib.generate_db_username(meta.get('namespace'), meta.get('name'))

    operator_db_username = os.getenv('POSTGRES_USER')

    try:
        con = lib.connect_to_postgres()
    except Exception as e:
        message = "Can't connect to database: " + str(e)
        raise kopf.TemporaryError(message)

    db_password = lib.generate_password(32)

    try:
        lib.create_db_username(con, db_username, db_password)
        lib.grant_role_to_current_user(con, db_username, operator_db_username)

        message = "Created user {0}.".format(db_username)
        kopf.info(spec, reason="Create user", message=message)
    except Exception as e:
        con.close()

        message = "Can't create user: {}".format(str(e))
        raise kopf.PermanentError(message)

    try:
        db_comment = "@".join([
            meta.get('namespace'),
            meta.get('name')
        ])

        lib.create_db(con, db_name, db_username, spec.get('encoding'), spec.get('lcCollate'), spec.get('lcCollate'),
                      db_comment)

        lib.grant_connect_on_db(con, db_name, db_username)
        lib.grant_connect_on_db(con, db_name, operator_db_username)

        message = "Created database {}.".format(db_name)
        kopf.info(spec, reason="Create database", message=message)
    except Exception as e:
        lib.delete_db(con, db_name, db_username)
        lib.delete_db_username(con, db_username)
        con.close()

        message = "Can't create database: {}".format(str(e))
        raise kopf.PermanentError(message)

    secret_name = spec.get('secretName')

    secret = lib.generate_kubernetes_secret(
        secret_name,
        os.getenv('POSTGRES_HOST'), os.getenv('POSTGRES_POST', '5432'),
        db_name, db_username, db_password
    )

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(secret)

    try:
        response = lib.create_kubernetes_secret(meta.get('namespace'), secret)
        kopf.info(response.to_dict(), reason='Secret created', message='Secret {} created'.format(secret_name))
    except Exception as e:
        lib.delete_db(con, db_name, db_username)
        lib.delete_db_username(con, db_username)
        con.close()

        message = "Can't create secret '{}': {}".format(secret_name, str(e))
        raise kopf.PermanentError(message)

    con.close()

    return {'children': response.metadata.name}


@kopf.on.update('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def update(**_):
    pass


@kopf.on.delete('postgres.database.k8s.jkroepke.de', 'v1alpha1', 'postgresdatabases')
def delete(spec: dict, meta: dict, **_):
    db_name = lib.generate_db_name(meta.get('namespace'), meta.get('name'))
    db_username = lib.generate_db_username(meta.get('namespace'), meta.get('name'))

    # connect to DB
    try:
        con = lib.connect_to_postgres()
    except Exception as e:
        message = "Can't connect to database: " + str(e)
        raise kopf.TemporaryError(message)

    # reject new connections and delete database
    try:
        lib.delete_db(con, db_name, db_username)

        message = "Delete database {}.".format(db_name)
        kopf.info(spec, reason="Database deleted", message=message)
    except Exception as e:
        con.close()

        message = "Can't delete postgresql database: {}".format(str(e))
        raise kopf.TemporaryError(message, delay=10.0)

    # delete database owner
    try:
        lib.delete_db_username(con, db_username)

        message = "Delete user {}.".format(db_username)
        kopf.info(spec, reason="Delete user", message=message)
    except Exception as e:
        con.close()

        message = "Can't delete postgresql user: {}".format(str(e))
        raise kopf.TemporaryError(message, delay=10.0)

    con.close()
    return {'message': message}

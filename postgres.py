import psycopg2
import json

with open('assets/creds.json') as f:
    CREDS = json.load(f)

CREDS = CREDS['postgres']


def py_connection(creds=CREDS):
    global _connection

    try:
        _connection
    except NameError:
        _connection = psycopg2.connect(
            host=creds['host'],
            database=creds['database'],
            user=creds['user'],
            password=creds['password']
        )

    return _connection


def py_cursor(func):
    def wrapped(*args, **kwargs):
        with py_connection() as connection:
            with connection.cursor() as cursor:
                return func(cursor=cursor, *args, **kwargs)

    return wrapped

#!/usr/bin/env python

from flask import Flask
from flask import make_response
from flask import request
from flask.json import jsonify

import psycopg2
import psycopg2.extras
import psycopg2.pool

import sys

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

class ConnectionPool:
    def __init__(self, *args, **kwargs):
        self.pool = psycopg2.pool.ThreadedConnectionPool(*args, **kwargs)

    def getconn(self):
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def putconn(self, conn):
        self.pool.putconn(conn)

    def closeall(self):
        self.pool.closeall()

pool = ConnectionPool(
    minconn=app.config['MIN_CONNECTIONS'],
    maxconn=app.config['MAX_CONNECTIONS'],
    dsn=app.config['POSTGRES_DATABASE_URI'],
    cursor_factory=psycopg2.extras.RealDictCursor
)

@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    if not req:
        return make_response('', 400)

    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute(req['query'])
            results = cur.fetchall()

        return jsonify({
            'status': 200,
            'results': results
        })

    except (psycopg2.DataError, psycopg2.ProgrammingError, psycopg2.NotSupportedError) as e:
        # User error
        return jsonify({
            'status': 400,
            'error': e.pgerror
        }), 400

    except:
        return jsonify({
            'status': 500,
            'error': str(sys.exc_info()[1])
        }), 500

    finally:
        pool.putconn(conn)
    
if __name__ == '__main__':
    app.run()

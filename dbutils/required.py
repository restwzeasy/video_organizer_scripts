#!/usr/bin/python

#
# Basic connection and CRUD utilities for Postgresql db.
#

import psycopg2
import sys

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# PostgreSQL default system database that we connect to and then create a custom db
SYSDBNAME = 'postgres'

# custom database to be created
DBNAME = 'filehashes'

# database user with permissions to create new databases
DBUSER = ''

# database user's password
#//FIXME switch password to ~/.pgpass ---> https://www.postgresql.org/docs/9.2/static/libpq-pgpass.html
#DBUSERPWD = ''

# database host/IP
DBHOST = '127.0.0.1'
conn=''

def validateDbCon():
    try:
        cur.execute("""SELECT datname from pg_database""")
    except Exception as e:
        print("Unable to read from pg_database! Cause:" + e)

def getConn():
    return conn

try:
    con = psycopg2.connect(host=DBHOST, dbname=SYSDBNAME, user=DBUSER)
    conn = con
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
except Exception as e:
    print("Unable to connect to postgreqld db!  Cause: " + e)

cur = conn.cursor()
validateDbCon()




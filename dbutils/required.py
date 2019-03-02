#!/usr/bin/python

#
# Basic connection and CRUD utilities for Postgresql db.
#

import psycopg2
from psycopg2 import pool
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
# conn=''
# threaded_postgresql_pool=None
threaded_postgresql_pool = psycopg2.pool.ThreadedConnectionPool(5, 10, user = DBUSER, host=DBHOST, database=str(SYSDBNAME))

# def validateDbCon():
#     try:
#         cur.execute("""SELECT datname from pg_database""")
#     except Exception as e:
#         print("Unable to read from pg_database! Cause:" + e)

###############
# Method for obtaining a system connection to a database.  These types of connections are powerful and should NOT be
# used for general business operations.  This connection is meant to create a new application database and hand over
# all business processing for a general application connection pool.
###############
def getDefaultConnection():
    return __getConnection__(SYSDBNAME)

###############
# Method for initializing a connection pool.
###############
def initPool(database):
    try:
        global threaded_postgresql_pool
        threaded_postgresql_pool = psycopg2.pool.ThreadedConnectionPool(20, 50, user=DBUSER, host=DBHOST, database=str(database))
        if (threaded_postgresql_pool):
            print("Connection pool created successfully using ThreadeConnectionPool")
            ps_connection = threaded_postgresql_pool.getconn()
            if (ps_connection):
                print("successfully received connection from connection pool")
                threaded_postgresql_pool.putconn(ps_connection)
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while creating PostgreSQL connection pool!", error)

###############
# Method for closing a connection pool.
###############
def closePool():
    global threaded_postgresql_pool
    if (threaded_postgresql_pool):
        threaded_postgresql_pool.closeall
        print("Threaded PostgreSQL connection pool is closed")

###############
# Method for leasing a connection from a connection pool.
###############
def getConnection():
    global threaded_postgresql_pool
    con = threaded_postgresql_pool.getconn()
    con.autocommit = True
    return con

###############
# Method for returning a connection to a connection pool.
###############
def returnConnection(con):
    global threaded_postgresql_pool
    threaded_postgresql_pool.putconn(con)


###############
# Method for actually creating a connection to the specified database.
###############
def __getConnection__(database):
    try:
        con = psycopg2.connect(host=DBHOST, dbname=str(database), user=DBUSER)
        # conn = con
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        print("Unable to connect to postgreqld db!  Cause: " + e)

    return con




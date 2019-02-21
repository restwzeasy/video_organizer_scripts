#!/usr/bin/python

#
# Logic for computing missing and duplicate files using Postgresql DB.
#

from dbutils import required as db

def handleDuplicates(conn):
    # conn = db.getConnection(dbName)
    cur = conn.cursor
    try:
        # duplicates on the source location
        cur.execute("SELECT * from filehashes WHERE ")
        # duplicates on the comparison location
        cur.execute("SELECT * from filehashes ")
    finally:
        cur.close
        # conn.close
        db.returnConnection(conn)

def handleMissing(sourceSchema, compSchema):
    # conn = db.getConnection(dbName)
    conn = db.getConnection()
    cur = conn.cursor()
    try:
        command = "SELECT * from %s.filehashes except select * from %s.filehashes;" %(sourceSchema, compSchema)
        cur.execute( command )
        missingInSource = cur.fetchall()
        print("Missing files in source location: %s" %missingInSource)
    finally:
        cur.close()
        # conn.close
        db.returnConnection(conn)
    return;
#!/usr/bin/python

#
# Logic for computing missing and duplicate files using Postgresql DB.
#

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

def handleMissing(srcCompDbPair):
    # conn = db.getConnection(dbName)
    conn = db.getConnection()
    cur = conn.cursor
    try:
        cur.execute("SELECT * from filehashes ")
    finally:
        cur.close
        # conn.close
        db.returnConnection(conn)
    return;
#!/usr/bin/python

#
# Logic for computing missing and duplicate files using Postgresql DB.
#

import os

from shutil import copy
from dbutils import required as db

MISSING_FROM_COMP_FOLDER = "missing_from_comp"
MISSING_FROM_SRC_FOLDER = "missing_from_source"

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

#######################
# Logic for identifying missing files in the source and destination/comparison locations.
#######################
def handleMissing(sourceSchema, compSchema):
    # conn = db.getConnection(dbName)
    conn = db.getConnection()
    cur = conn.cursor()
    try:
        # identify missing files from the comparison location
        command = "SELECT * from %s.filehashes except select * from %s.filehashes;" %(sourceSchema, compSchema)
        cur.execute( command )
        missingInComp = cur.fetchall()
        # print("Missing files in comparison location: %s" % missingInComp)
        __collectMissingFiles__(missingInComp, MISSING_FROM_COMP_FOLDER)

        # identify missing files from the source location
        command = "SELECT * from %s.filehashes except select * from %s.filehashes;" % (compSchema, sourceSchema)
        cur.execute(command)
        missingInSource = cur.fetchall()
        # print("Missing files in source location: %s" % missingInSource)
        __collectMissingFiles__(missingInSource, MISSING_FROM_SRC_FOLDER)

    finally:
        cur.close()
        # conn.close
        db.returnConnection(conn)
    print("Done processing missing files.  Please look at %s and %s folders for missing files." % (MISSING_FROM_SRC_FOLDER, MISSING_FROM_COMP_FOLDER))
    return;

#######################
# Utility for copying all of the missing files from the specified result-set into the specified folder.
#######################
def __collectMissingFiles__( missingFiles, folderName ):
    if not os.path.exists(folderName):
        os.makedirs(folderName)

    for missingFile in missingFiles:
        copy( missingFile[0],  folderName)
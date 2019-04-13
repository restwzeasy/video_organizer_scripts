#!/usr/bin/python

#
# Logic for computing missing and duplicate files using Postgresql DB.
#

import os
import fnmatch

# from shutil import copy2
# from shutil import copyfile
from dbutils import required as db

MISSING_FROM_COMP_FOLDER = "missing_from_comp"
MISSING_FROM_SRC_FOLDER = "missing_from_source"
FILE_NAME_COLUMN = "file"
FILE_EXTENSIONS_TO_SKIP = ['.ini', '.db', '.info', '.pdfcp ']

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
def handleMissing(dbName, sourceSchema, compSchema):
    conn = db.getConnection(dbName)
    cur = conn.cursor()
    try:
        # identify missing files from the comparison location
        command = "SELECT %s from %s.filehashes where hash in (select hash from %s.filehashes except select hash from %s.filehashes);" %(FILE_NAME_COLUMN, sourceSchema, sourceSchema, compSchema)
        print("Identifying all missing files in comparison location.  Executing: " + command)
        cur.execute( command )
        missingInComp = cur.fetchall()
        # print("Missing files in comparison location: %s" % missingInComp)
        # __collectMissingFiles__(list(missingInComp), MISSING_FROM_COMP_FOLDER)
        try:
            for missingFile in missingInComp:
                __collectMissingFiles__(missingFile[0], MISSING_FROM_COMP_FOLDER)
        except Exception as ce:
            print("There was a problem locating files on comparison location.  The comparison location files may have changed/moved since the scan. ")
            print(ce)
            return

        # identify missing files from the source location
        command = "SELECT %s from %s.filehashes where hash in (select hash from %s.filehashes except select hash from %s.filehashes);" % (FILE_NAME_COLUMN, compSchema, compSchema, sourceSchema)
        print("Identifying all missing files in source location.  Executing: " + command)
        cur.execute(command)
        missingInSource = cur.fetchall()
        # print("Missing files in source location: %s" % missingInSource)
        # __collectMissingFiles__(list(missingInSource), MISSING_FROM_SRC_FOLDER)
        try:
            for missingFile in missingInSource:
                __collectMissingFiles__(missingFile[0], MISSING_FROM_SRC_FOLDER)
        except Exception as se:
            print("There was a problem locating files on source location.  The source location files may have changed/moved since the scan. ")
            print(se)
    except Exception as e:
        print("Unable to identify missing files!  Cause: " + e)
    finally:
        cur.close()
        # conn.close
        db.returnConnection(conn)
    print("Done processing missing files.  Please look at %s and %s folders for missing files." % (MISSING_FROM_SRC_FOLDER, MISSING_FROM_COMP_FOLDER))


#######################
# Utility for copying all of the missing files from the specified result-set into the specified folder.
#######################
def __collectMissingFiles__( missingFile, folderName ):
    # for missingFile in missingFiles:
    # missingFile.endswith(tuple(set(['.ini', '.db'])))
    if not missingFile.endswith(tuple(FILE_EXTENSIONS_TO_SKIP)):
        dst = "./" + folderName + missingFile
        print("Attempting to copy missing file: " + missingFile + " to destination: " + dst)
        if not os.path.exists(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))
        # TODO implement file type filtering to only get files we want and skip ones we don't care about like *.txt, *.ini, etc.
        # if not fnmatch.fnmatch(missingFile, '.*.ini'):
        # copyfile( missingFile,  dst)
        # copy2(missingFile, dst)
        os.system('cp -v -p "' + missingFile + '" "' + dst + '"')
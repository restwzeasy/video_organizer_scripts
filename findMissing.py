#!/usr/bin/python

import os, sys, getopt, errno
import platform, sys, hashlib
import datetime
import asyncio

from dbutils import required as db

BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


class FileHashEntry:
    def __init__(self, fullPath, timestamp, hash):
        self.fullPath = fullPath
        self.timestamp = timestamp
        self.hash = hash

    def __str__(self):
        return self.fullPath + " | " + self.timestamp + " | " + self.hash

class SrcCompDatabasePair:
    def __init__(self, sourceDbPath, compDbPath):
        self.sourceDbPath = sourceDbPath
        self.compDbPath = compDbPath


def main(argv):
    sourcepath = ''
    comppath = ''
    skipScan = False

    if platform.system() == 'Windows':
        print("Currently don't support Windows based systems due to file datestamp processing complexities.  Please use a Mac.")
        sys.exit(2)

    try:
        opts, args = getopt.getopt(argv, "hxs:c:", ["org=","comp="])
    except getopt.GetoptError:
        print('findMissing.py -s <original_directory_with_files> -c <comparison_directory_with_files> -x')
        print(' Use the -x option to skip scanning the folders and creating hashes.  Useful if the hashes were created already and files didn\'t change - including no file additions/deletions.')
        print("Example:")
        print("        ./findMissing.py -s /Volumes/Photos/ -c /Volumes/Multimedia/photos2/")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('./findMissing.py -s <original_directory_with_files> -c <comparison_directory_with_files> -x')
            print(' Use the -x option to skip scanning the folders and creating hashes.  Useful if the hashes were created already and files didn\'t change - including no file additions/deletions.')
            print("Example:")
            print("       ./findMissing.py -s /Volumes/Photos/ -c /Volumes/Multimedia/photos2/")
            sys.exit()
        elif opt == '-x':
            skipScan = True
        elif opt in ("-s", "-org"):
            sourcepath = arg
        elif opt in ("-c", "-comp"):
            comppath = arg

    if sourcepath == "":
        print("Missing required parameter -s. Exiting.")
        sys.exit(2)
    elif comppath == "":
        print("Missing required parameter -c. Exiting.")
        sys.exit(2)



    if skipScan == True:
        print("Skipping directory scan of folders: %s and %s" % (sourcepath, comppath))
    else:
        print("Scanning directory", sourcepath, " and comparing vs files in", comppath)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(scandir(sourcepath))
        loop.run_until_complete(scandir(comppath))
        loop.close()

    extractDuplicatesAndMissing( sourcepath, comppath )

#######################
# Compute the unique db name to use based on the specified path and datetime stamp.
#######################
def getDbName(mypath):
    return mypath + datetime.datetime.utcnow().isoformat()


#######################
# Create and initialize the database to use by the hash calculation and analysis functionality
#######################
async def initdb(dbname):
    conn = db.getDefaultConnection()
    cur = conn.cursor()
    try:
        cur.execute( "CREATE DATABASE \"%s\";" % dbname )
    finally:
        cur.close
        conn.close

    # This won't work because the directory hasn't been selected yet - source vs destination.  Plus, the timestamp on the db name won't allow for skip in the future because we won't know the name of the db.  Best we can do is only use date and that limits the -x to be executed on the same date.
    conn = db.getConnection(dbname)
    cur = conn.cursor()
    try:
        cur.execute( "CREATE TABLE filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" )
    finally:
        cur.close
        conn.close


#######################
# Scan the specified folder, create hashes for all found files and insert the hash records into the working db.
#######################
async def scandir(mypath):
    dbname = getDbName(mypath)
    await initdb(dbname)

    conn = db.getConnection( dbname )
    cur = conn.cursor()
    try:
        for root, dirs, files in os.walk(mypath):
            for file in files:
                fullSourcePath = os.path.join(root, file)
                hash = await createHash( fullSourcePath )
                fileHashEntry = FileHashEntry(fullSourcePath, datetime.datetime.utcnow().isoformat(), hash)
                print("file hash record: " + str(fileHashEntry))
                cur.execute("INSERT INTO filehashes (file, hash) values (%s, %s);", (fileHashEntry.fullPath, fileHashEntry.hash))
    finally:
        cur.close()
        conn.close


#######################
# Helper utility to create hashes of files.
#######################
async def createHash( bfile ):
    sha1 = hashlib.sha1()

    with open(bfile, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    print("file: " + bfile, "sha1: ", sha1.hexdigest())
    return sha1.hexdigest()


def extractDuplicatesAndMissing( sourcepath, comppath ):
    srcCompDbPair = identifySrcAndDestDbNames()
    handleDuplicates( srcCompDbPair.sourceDbPath )

def identifySrcAndDestDbNames ( sourcepath, comppath ):

    conn = db.getDefaultConnection()
    cur = conn.cursor
    sourceDatabases=''
    compDatabases=''
    try:
        cur.execute("SELECT datname FROM pg_database WHERE datisTemplate = false and datname LIKE '%sourcepath'")
        sourceDatabases=cur.fetchall()

        cur.execute("SELECT datname FROM pg_database WHERE datisTemplate = false and datname LIKE '%comppath'")
        compDatabases=cur.fetchall()
    finally:
        cur.close
        conn.close

    sourceDatabases = sorted(sourceDatabases, reverse=True)
    compDatabases = sorted(compDatabases, reverse=True)
    srcDbPath = sourceDatabases[0]
    compDbPath = compDatabases[0]

    return SrcCompDatabasePair( srcDbPath, compDbPath )


def handleDuplicates(dbName):
    conn = db.getConnection(dbName)
    cur = conn.cursor
    try:
        cur.execute("SELECT * from filehashes ")
    finally:
        cur.close
        conn.close

def handleMissing():


if __name__ == "__main__":
    main(sys.argv[1:])


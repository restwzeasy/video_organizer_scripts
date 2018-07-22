#!/usr/bin/python

import os, sys, getopt, errno
import platform, sys, hashlib
import datetime
import asyncio
import psycopg2 as ps

from dbutils import required as db

BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


class FileHashEntry:
    def __init__(self, fullPath, timestamp, hash):
        # super(FileHashEntry, self).__init__()
        self.fullPath = fullPath
        self.timestamp = timestamp
        self.hash = hash

    def __str__(self):
        return self.fullPath + " | " + self.timestamp + " | " + self.hash


def main(argv):
    sourcepath = ''
    comppath = ''

    if platform.system() == 'Windows':
        print("Currently don't support Windows based systems due to file datestamp processing complexities.  Please use a Mac.")
        sys.exit(2)

    try:
        opts, args = getopt.getopt(argv, "hs:c:", ["org=","comp="])
    except getopt.GetoptError:
        print('findMissing.py -s <original_directory_with_files> -c <comparison_directory_with_files>')
        print("Example:")
        print("        ./findMissing.py -s /Volumes/Photos/ -c /Volumes/Multimedia/photos2/")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('./findMissing.py -s <original_directory_with_files> -c <comparison_directory_with_files>')
            print("Example:")
            print("       ./findMissing.py -s /Volumes/Photos/ -c /Volumes/Multimedia/photos2/")
            sys.exit()
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

    print("Scanning directory", sourcepath, " and comparing vs files in", comppath)
    scandir(sourcepath)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(scandir(sourcepath))
    loop.run_until_complete(scandir(comppath))
    loop.close()

def getDbName(mypath):
    path = str(mypath)
    date = str(datetime.datetime.utcnow().isoformat())
    # return path + date
    return mypath + datetime.datetime.utcnow().isoformat()


async def initdb(dbname):
    conn = db.getDefaultConnection()
    cur = conn.cursor()
    try:
        cur.execute( "CREATE DATABASE \"%s\";" % dbname )
    finally:
        cur.close
        conn.close

    conn = db.getConnection(dbname)
    cur = conn.cursor()
    try:
        cur.execute( "CREATE TABLE filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" )
    finally:
        cur.close
        conn.close


async def scandir(mypath):
    # conn = db.getConn()
    # cur = conn.cursor()
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
                # cur.execute('INSERT INTO filehashes (file, hashtimestamp, hash) values (%s, %s, %s);', fileHashEntry.fullPath, fileHashEntry.timestamp, fileHashEntry.hash)
    finally:
        cur.close()
        conn.close


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


if __name__ == "__main__":
    main(sys.argv[1:])


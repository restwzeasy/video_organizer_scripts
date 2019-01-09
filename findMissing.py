#!/usr/bin/python

import os, sys, getopt, errno
import platform, sys, hashlib
import datetime
import asyncio
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from multiprocessing import Pool, TimeoutError
import time
import concurrent

from dbutils import required as db

# import psycopg2
# from psycopg2 import pool

# try:
#     threaded_postgresql_pool = psycopg2.pool.ThreadedConnectionPool(5, 20, )


# db.
# persist = Persi
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
POOL_SIZE = 50 # concurrent number of processes to use for parallel operations
loop = asyncio.get_event_loop()
executor = ProcessPoolExecutor(POOL_SIZE);

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
        # in case of skipping hash creation, we need to get the dbname as input or assume the most recent db as the one to use
    else:
        print("Scanning directory", sourcepath, " and comparing vs files in", comppath)
        dbname = getDbName()
        print("Working with db: ", dbname)
        scan(dbname, sourcepath, comppath)

    extractDuplicatesAndMissing( sourcepath, comppath )

#######################
# Scan the source and comparison paths and create hashes for all contained files.  This is step 1 of the diff comparison
# logic.  This step can be skipped if the paths have already been scanned and there haven't been any changes to the files
# and folders.  If this step is skipped, only the hashes that were previously created will be considered for diff
# analysis.
#######################
def scan(dbname, sourcepath, comppath):
    sourceschema = getSchemaName(sourcepath)
    compschema = getSchemaName(comppath)

    try:
        initdb(dbname, sourceschema, compschema)
    # loop = asyncio.get_event_loop()
    # executor = ProcessPoolExecutor(40);
        sourceFuture = asyncio.ensure_future( loop.run_in_executor(executor, scandir(dbname, sourcepath, sourceschema)) )
        compFuture = asyncio.ensure_future( loop.run_in_executor(executor, scandir(dbname, comppath, comppath)) )
        loop.run_until_complete(sourceFuture)
        loop.run_until_complete(compFuture)
    #     scandir(dbname, sourcepath, sourceschema)
    #     scandir(dbname, comppath, comppath)
    finally:
        db.closePool()
        loop.close()

#######################
# Compute a unique db name.
#######################
def getDbName():
    return "filecompare_" + datetime.datetime.utcnow().isoformat()

#######################
# Compute the unique schema name to use based on the specified path and datetime stamp.
#######################
def getSchemaName(mypath):
    tPath = "path_" + mypath + datetime.datetime.utcnow().isoformat()
    tPath = tPath.replace('.', '_')
    tPath = tPath.replace(':', '')
    tPath = tPath.replace('/','$')
    tPath = tPath.replace('-', '_')
    return tPath


#######################
# Create and initialize the database to be used by the hash calculation and analysis functionality.
#######################
def initdb(dbname, sourceschema, compschema):
    # create the unique database that we are going to use
    conn = db.getDefaultConnection()
    print("Connected to default DB and creating database for our work.")
    cur = conn.cursor()
    try:
        cur.execute( "CREATE DATABASE \"%s\";" % dbname )
    finally:
        cur.close
        conn.close

    db.initPool(dbname)

    # create the source and comparison schemas and filehashes table in each schema.
    print("Trying to connect to db: ", dbname)
    # conn = db.__getConnection__( dbname )
    conn = db.getConnection()
    cur = conn.cursor()
    try:
        cur.execute( "CREATE SCHEMA %s;" % sourceschema )
        cur.execute( "CREATE SCHEMA %s;" % compschema )

        cur.execute( "CREATE TABLE %s.filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" % sourceschema )
        cur.execute( "CREATE TABLE %s.filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" % compschema )
    finally:
        cur.close
        # conn.close
        db.returnConnection(conn)


#######################
# Scan the specified folder, create hashes for all found files and insert the hash records into the working db.
#######################
def scandir(dbname, mypath, schema):
    print("Connecting to db: ", dbname)
    # conn = db.getConnection( dbname )
    # cur = conn.cursor()
    print("Traversing %s to insert data into schema %s" % (mypath, schema))

    # pool = Pool(processes=POOL_SIZE)
    # executor = concurrent.futures.ThreadPoolExecutor(max_workers=POOL_SIZE,)
    # executor = ProcessPoolExecutor(POOL_SIZE)
    # event_loop = asyncio.get_event_loop()
    # new_loop = asyncio.new_event_loop()

    tasks = []

    # try:
    for root, dirs, files in os.walk(mypath):
        for file in files:
            tasks.append(asyncio.ensure_future( loop.run_in_executor(executor, __computeHashAndInsert__(dbname, schema, root, file))) )
            # if(len(tasks) > POOL_SIZE):
            #     time.sleep(1)
            # new_loop.call_soon(__computeHashAndInsert__, cur, schema, root, file)
            # __computeHashAndInsert__(cur, schema, root, file)
    loop.run_until_complete(asyncio.wait(tasks))

            # hash_results = [
            #     pool.apply_async(__computeHashAndInsert__, (cur, schema, root, file))
            #     for file in files
            # ]



        # event_loop.run_until_complete(
        #     for root, dirs, files in os.walk(mypath):
        #         __scanFiles_(cur, schema, root, files)

            # for root, dirs, files in os.walk(mypath):
            # # event_loop.call_soon(__computeHashAndInsert__, (cur, schema, root, file))
            #
            #
            #     hash_tasks = [
            #         asyncio.ensure_future( event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file) )
            #         for file in files
            #     ]
            #     completed, pending = await asyncio.wait(hash_tasks)

            # event_loop.run_until_complete(asyncio.gather(*hash_tasks))
            # completed, pending = await asyncio.wait(hash_tasks)
            # await __computeHashAndInsert__()
            # event_loop.run_forever()
        # )

        # await __computeHashAndInsert__()

            # for file in files:



                # pool = multiprocessing.Pool(POOL_SIZE)
                # results = pool.map_async(__computeHashAndInsert__, (cur, schema, root, file))

                #     hash_tasks = [
                #         event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file)
                #         for file in files
                #     ]
                #     completed, pending = await asyncio.wait(hash_tasks)

            # event_loop.run_until_complete(
            #     for file in files:
                    # await event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file)
                    # event_loop.
                # await __computeHashAndInsert__()

            # )
            # for file in files:
            #     hash_tasks = [
            #         event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file)
            #         for file in files
            #     ]
            #     completed, pending = await asyncio.wait(hash_tasks)

            # results = [t.result() for t in completed]

            # for file in files:
                # asyncio.run(__computeHashAndInsert__(cur, schema, root, file))
                # hash_tasks = [
                #     event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file)
                # ]
                # completed, pending = await asyncio.wait(hash_tasks)
                # fullSourcePath = os.path.join(root, file)
                # hash = createHash( fullSourcePath )
                # fileHashEntry = FileHashEntry(fullSourcePath, datetime.datetime.utcnow().isoformat(), hash)
                # print("file hash record: " + str(fileHashEntry))
                # # print("about to execute: ", "INSERT INTO \"%s\".filehashes (file, hash) values (%s, %s);", (schema.strip('\''), fileHashEntry.fullPath, fileHashEntry.hash))
                # command = "INSERT INTO %s.filehashes(file, hash) values (\'%s\', \'%s\');" % (schema, fileHashEntry.fullPath, fileHashEntry.hash)
                # # cur.execute("INSERT INTO %s.filehashes (file, hash) values (%s, %s);", (schema, fileHashEntry.fullPath, fileHashEntry.hash))
                # print("about to execute: ", command)
                # cur.execute(command)
    # finally:
    #     cur.close()
    #     conn.close
        # event_loop.close()
        # pool.close()
        # pool.join()


# async def __scanFiles_(cur, schema, root, files):
#     executor = concurrent.futures.ThreadPoolExecutor(max_workers=POOL_SIZE,)
#     event_loop = asyncio.new_event_loop()
#     hash_tasks = [
#         asyncio.ensure_future( event_loop.run_in_executor(executor, __computeHashAndInsert__, cur, schema, root, file) )
#         for file in files
#     ]
#     completed, pending = await asyncio.wait(hash_tasks)


def __computeHashAndInsert__(dbname, schema, root, file):
    # conn = db.getConnection( dbname )
    conn = db.getConnection()
    cur = conn.cursor()
    try :
        fullSourcePath = os.path.join(root, file)
        hash = createHash( fullSourcePath )
        fileHashEntry = FileHashEntry(fullSourcePath, datetime.datetime.utcnow().isoformat(), hash)
        print("file hash record: " + str(fileHashEntry))
        # print("about to execute: ", "INSERT INTO \"%s\".filehashes (file, hash) values (%s, %s);", (schema.strip('\''), fileHashEntry.fullPath, fileHashEntry.hash))
        command = "INSERT INTO %s.filehashes(file, hash) values (\'%s\', \'%s\');" % (schema, __sanitizeFilename__(fileHashEntry.fullPath), fileHashEntry.hash)
        # cur.execute("INSERT INTO %s.filehashes (file, hash) values (%s, %s);", (schema, fileHashEntry.fullPath, fileHashEntry.hash))
        print("about to execute: ", command)
        cur.execute(command)
    finally:
        cur.close()
        # conn.close
        db.returnConnection(conn)


#######################
# Helper utility to create hashes of files.
#######################
def __sanitizeFilename__( filename ):
    tFilename = filename
    tFilename = tFilename.replace('\'', '')
    return tFilename

def createHash( bfile ):
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
    srcCompDbPair = identifySrcAndDestDbNames(sourcepath, comppath)
    # handleDuplicates( srcCompDbPair.sourceDbPath )
    handleMissing(srcCompDbPair)

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
        # conn.close
        db.returnConnection(conn)

    sourceDatabases = sorted(sourceDatabases, reverse=True)
    compDatabases = sorted(compDatabases, reverse=True)
    srcDbPath = sourceDatabases[0]
    compDbPath = compDatabases[0]
    print("Identified ", srcDbPath, " as the source DB path and ", compDbPath, " as the comparison DB path.")

    return SrcCompDatabasePair( srcDbPath, compDbPath )


def handleDuplicates(dbName):
    conn = db.getConnection(dbName)
    cur = conn.cursor
    try:
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

if __name__ == "__main__":
    main(sys.argv[1:])


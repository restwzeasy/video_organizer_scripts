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
from diffutils import diff as fdiff

# import psycopg2
# from psycopg2 import pool

# try:
#     threaded_postgresql_pool = psycopg2.pool.ThreadedConnectionPool(5, 20, )


BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
POOL_SIZE = 50 # concurrent number of processes to use for parallel operations
DB_NAME_PREFIX = "filecompare_" # prefix for the database containing the comparison hash data.  The suffix is the datetimestamp.
SCHEMA_NAME_PREFIX = "path_" # prefix for schema names/paths.  THe suffix is the datetimestamp.
PATH_SEGMENT_DELIMITER = "$" # delimiter between schema parts

loop = asyncio.get_event_loop()
# executor = ProcessPoolExecutor((multiprocessing.cpu_count() - 1) * 2)

sourceSchema=''
compSchema=''
dbName=''

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
    global dbName

    sourcepath = ''
    comppath = ''
    skipScan = False

    if platform.system() == 'Windows':
        print("Currently don't support Windows based systems due to file datestamp processing complexities.  Please use a Mac/Linux or a Linux based container.")
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


    # Don't need source and comp location parameters for skip-scan workflow, but need to validate it for normal workflow.
    if skipScan == True:
        print("Skipping directory scan.  Scanning DB for existing DB and schema.")
        # In case of skipping hash creation, we need to get the dbname as input or assume the most recent db as the one to use.
        # If we have an existing DB with schemas that were previously populated, evaluate the timestamps to determine
        # source (earlier created schema) and comparison (later created schema) schemas.
        __identifySrcAndDestSchemaNames__()
    else:
        if sourcepath == "":
            print("Missing required parameter -s. Exiting.")
            sys.exit(2)
        elif comppath == "":
            print("Missing required parameter -c. Exiting.")
            sys.exit(2)

        print("Scanning directory", sourcepath, " and comparing vs files in", comppath)
        dbname = getDbName()
        print("Working with db: ", dbname)
        scan(dbname, sourcepath, comppath)

    extractDuplicatesAndMissing(dbName, sourceSchema, compSchema )

#######################
# Scan the source and comparison paths and create hashes for all contained files.  This is step 1 of the diff comparison
# logic.  This step can be skipped if the paths have already been scanned and there haven't been any changes to the files
# and folders.  If this step is skipped, only the hashes that were previously created will be considered for diff
# analysis.
#######################
def scan(dbname, sourcepath, comppath):
    global sourceSchema
    global compSchema

    sourceSchema = getSchemaName(sourcepath)
    time.sleep(2) # sleep for 2 seconds to allow source-path vs comp-path timestamp difference to aid identification in skip-scan workflows.  Source path must be created first.
    compSchema = getSchemaName(comppath)
    # pool = multiprocessing.Pool(multiprocessing.cpu_count())

    try:
        initdb(dbname, sourceSchema, compSchema)
    # loop = asyncio.get_event_loop()
    # executor = ProcessPoolExecutor(40);
        with concurrent.futures.ProcessPoolExecutor(2) as executor:
            scanResults = []
            scanResults.append(executor.map(scandir(dbname, sourcepath, sourceSchema)))
            scanResults.append(executor.map(scandir(dbname, comppath, compSchema)))
        # scanResults.append(pool.apply_async(scandir(dbname, sourcepath, sourceSchema)))
        # scanResults.append(pool.apply_async(scandir(dbname, comppath, compSchema)))
            print("end of scan - processing asynchronously")
        # loop.run_until_complete(scanResults)
        # sourceFuture = asyncio.ensure_future( loop.run_in_executor(executor, scandir(dbname, sourcepath, sourceschema)) )
        # compFuture = asyncio.ensure_future( loop.run_in_executor(executor, scandir(dbname, comppath, compschema)) )
        # loop.run_until_complete(sourceFuture)
        # loop.run_until_complete(compFuture)
    #     scandir(dbname, sourcepath, sourceschema)
    #     scandir(dbname, comppath, comppath)
    finally:
        db.closePool()
        loop.close()
        print("Done scan method")

#######################
# Compute a unique db name.
#######################
def getDbName():
    return DB_NAME_PREFIX + datetime.datetime.utcnow().isoformat()

#######################
# Compute the unique schema name to use based on the specified path and datetime stamp.
#######################
def getSchemaName(mypath):
    tPath = SCHEMA_NAME_PREFIX + mypath + datetime.datetime.utcnow().isoformat()
    return __sterilizePath__(tPath)

#######################
# Clean-up path to conform to Postgres schema naming requirements - remove invalid characters.
#######################
def __sterilizePath__(path):
    tPath = path
    tPath = tPath.replace('.', '_')
    tPath = tPath.replace(':', '')
    tPath = tPath.replace('/', PATH_SEGMENT_DELIMITER)
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
    conn = db.__getConnection__( dbname )
    cur = conn.cursor()
    try:
        cur.execute( "CREATE SCHEMA %s;" % sourceschema )
        cur.execute( "CREATE SCHEMA %s;" % compschema )

        cur.execute( "CREATE TABLE %s.filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" % sourceschema )
        cur.execute( "CREATE TABLE %s.filehashes ( file varchar(256), hashtimestamp timestamp DEFAULT current_timestamp, hash varchar(128));" % compschema )
    finally:
        cur.close
        conn.close


#######################
# Scan the specified folder, create hashes for all found files and insert the hash records into the working db.
#######################
def scandir(dbname, mypath, schema):
    print("Connecting to db: ", dbname)
    # conn = db.getConnection( dbname )
    # cur = conn.cursor()
    print("Traversing %s to insert data into schema %s" % (mypath, schema))

    # pool = Pool(processes=POOL_SIZE)
    # tExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=100)
    # executor = ProcessPoolExecutor(POOL_SIZE)
    # event_loop = asyncio.get_event_loop()
    # new_loop = asyncio.new_event_loop()
    # pool = multiprocessing.Pool(multiprocessing.cpu_count())

    # tasks = []
    results = []

    # try:
    # for root, dirs, files in os.walk(mypath):
    #     with concurrent.futures.ProcessPoolExecutor(multiprocessing.cpu_count()) as executor:
    #         for file in files:
    #             executor.map(__computeHashAndInsert__(dbname, schema, root, file))

    # with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(mypath):
            for file in files:
                executor.submit(__computeHashAndInsert__(dbname, schema, root, file))

            #     TODO Add error handling if the process is terminated early



def __computeHashAndInsert__(dbname, schema, root, file):
    conn = db.getConnection( dbname )
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


def extractDuplicatesAndMissing(dbName, sourceSchema, compSchema ):
    print("dbname: %s and sourceSchema: %s and compSchema: %s" %(dbName, sourceSchema, compSchema))
    fdiff.handleMissing(dbName, sourceSchema, compSchema)

def __identifySrcAndDestSchemaNames__():
    global sourceSchema
    global compSchema
    global dbName

    latestDbName = ''.join(__findMostRecentDB__())
    dbName = latestDbName

    conn = db.getConnection(latestDbName)
    cur = conn.cursor()
    try:
        command = "select schema_name from \"" + latestDbName + "\".information_schema.schemata where schema_name LIKE '" + SCHEMA_NAME_PREFIX + "%'"
        print("Identifying existing schemas.  Executing: " + command)
        cur.execute(command)
        paths = cur.fetchall()
        #TODO - add error checking for cases where DB existed, but no existing schema found
    finally:
        cur.close
        # conn.close
        db.returnConnection(conn)

    # compare the converted path tuple into string
    if (__getTimestamp__(''.join(paths[0])) < __getTimestamp__(''.join(paths[1]))):
        sourceSchema = ''.join(paths[0])
        compSchema = ''.join(paths[-1])
    else:
        sourceSchema = ''.join(paths[-1])
        compSchema = ''.join(paths[0])


def __getTimestamp__(path):
    pathSegments = path.split(PATH_SEGMENT_DELIMITER)
    dateTimeStr = pathSegments[-1]
    timestamp = datetime.datetime.strptime(dateTimeStr, '%Y_%m_%dt%H%M%S_%f')
    return timestamp


def __findMostRecentDB__():
    conn = db.getDefaultConnection()
    cur = conn.cursor()
    try:
        command = "SELECT datname FROM pg_database WHERE datisTemplate = false and datname LIKE '" + DB_NAME_PREFIX + "%' order by datname desc"
        print("Identifying most recent DB.  Executing: " + command)
        cur.execute(command)
        latestDbName = cur.fetchone()
        print(latestDbName)
        # print("found latest DB " + latestDbName[0])
    finally:
        cur.close
        conn.close

    return latestDbName




if __name__ == "__main__":
    main(sys.argv[1:])


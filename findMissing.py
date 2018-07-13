#!/usr/bin/python

import psycopg2
import os, sys, getopt, errno
import platform, sys, hashlib
import datetime

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
    scandir(comppath)


def scandir(mypath):
    for root, dirs, files in os.walk(mypath):
        for file in files:
            fullSourcePath = os.path.join(root, file)
            hash = createHash( fullSourcePath )
            # global FileHashEntry
            fileHashEntry = FileHashEntry(fullSourcePath, datetime.datetime.utcnow().isoformat(), hash)
            print("file hash record: " + str(fileHashEntry))

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


if __name__ == "__main__":
    main(sys.argv[1:])


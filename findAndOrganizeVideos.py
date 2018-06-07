#!/usr/bin/python

import os, sys, getopt, errno, shutil
import re # regular expressions
import platform
import datetime # computing datestamp from file birthtime

def main(argv):
    sourcepath = ''
    dstpath = ''

    if platform.system() == 'Windows':
        print "Currently don't support Windows based systems due to file datestamp processing complexities.  Please use a Mac."
        sys.exit(2)

    try:
        opts, args = getopt.getopt(argv, "hs:t:", ["src=","dst="])
    except getopt.GetoptError:
        print('findAndOrganizeVideos.py -s <directory_with_movies> -t <destination_directory>')
        print "Example:"
        print "        ./findAndOrganizeVideos.py -s /Volumes/Photos/photos_to_sort/2011/2011-08-30_Europe-2011_b -t /Volumes/Multimedia/photos2/sorted_videos/"
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'findAndOrganizeVideos.py -s <directory_with_movies> -t <destination_directory>'
            print "Example:"
            print "       ./findAndOrganizeVideos.py -s /Volumes/Photos/photos_to_sort/2011/2011-08-30_Europe-2011_b -t /Volumes/Multimedia/photos2/sorted_videos/"
            sys.exit()
        elif opt in ("-s", "-src"):
            sourcepath = arg
        elif opt in ("-t", "-dst"):
            dstpath = arg

    if sourcepath == "":
        print "Missing required parameter -s. Exiting."
        sys.exit(2)
    elif dstpath == "":
        print "Missing required parameter -t. Exiting."
        sys.exit(2)

    print "Scanning directory", sourcepath, "for video files to be moved to", dstpath
    scandir(sourcepath, dstpath)


def scandir( mypath, dstpath ):
    for root, dirs, files in os.walk(mypath):
        for file in files:
            if (file.endswith(".m2ts") or file.endswith(".modd") or file.endswith(".moff") or file.endswith(".MTS")):
                print "processing filename:", file
                fullSourcePath = os.path.join(root, file)
                targetDir = computeDatastamp(file, fullSourcePath)

                fullTargetDir = dstpath + targetDir
                # print "fullTargetDir ===", fullTargetDir

                fullTargetPath = os.path.join(fullTargetDir, file)

                createdir( fullTargetDir )
                movefile( fullSourcePath, fullTargetPath )
            else:
                print "Skipping unrecognized filename", file
    return;

# compute datestamp
def computeDatastamp( file, fullSourcePath ):
    datestampPat = re.compile('[0-9]{8}')

    if datestampPat.search(file) is not None:
        datestamp=file[:8]
        year=datestamp[:4]
        month=datestamp[4:6]
        day=datestamp[6:8]
        targetdir=year + "-" + month + "-" + day
    else:
        # need to get datestamp from created date
        stat = os.stat(fullSourcePath)
        try:
            datestamp = datetime.date.fromtimestamp( stat.st_birthtime )
            print "Will use file birthtime as datestamp."
        except AttributeError:
            datestamp = datetime.date.fromtimestamp( stat.st_mtime )
            print "Will make datestamp equivalent to last modification date."
        print "using datestamp", datestamp
        targetdir = datestamp.isoformat()

    return targetdir;


def createdir( fulltargetpath ):
    if not os.path.exists(fulltargetpath):
        print "Creating directory ", fulltargetpath
        try:
            os.makedirs(fulltargetpath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    return;


def movefile( source, destination ):
    try:
        print "moving", source, "to", destination
        shutil.move(source, destination)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return;

if __name__ == "__main__":
    main(sys.argv[1:])

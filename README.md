# Sony Video File Organizer Utility

If you don't do regular/daily imports of your files and happen to find yourself
in a situation where you have multiple days/weeks/months of Sony video (*.MTS, *.M2TS, *.MODD, *.MOFF)
files within the same folder; use this utility to organize the files according to the date of the video.

The script loops through the specified source folder and examines all supported Sony video files.  For each file,
the script attempts to extract the file creation date by examining (preferably) the file name and in case of failure,
the file creation date.  The script then checks the target location to see if a directory corresponding to the file
creation date exists.  _(Note: Destination folders use YYYY-MM-DD format.)_  If a destination subfolder corresponding
to the creation date doesn't exist, the script will attempt to create one and then move the file to that location.

Files such as JPGs that are not supported for the organization task are skipped. 

### Limitations:
- Currently, only Mac based file processing is supported.  The datestamp extraction logic would have to be expanded in
order to support Windows and Linux systems and at this time I simply didn't have a need to do that.  A great example on
the web (https://stackoverflow.com/questions/237079/how-to-get-file-creation-modification-date-times-in-python) should
be handy for expanding the functionality.
- I haven't done heavy testing of file conflicts as I'm usually specifying a new destination folder
and am dealing with merging to existing folder manually.  More complex case testing is a TODO for future.

###Requirements:
- The file has to have one of 2 ways to identify the corresponding timestamp.
    - either the video has to use a datestamp filename like 20180902142357.m2ts
    - or, the file has to have valid/original timestamp such as file 00785.MTS corresponding to datestamp of 2018-03-12
     and some time.
- To deal with directory creation permissions, the script has to be executed via _sudo_.
  
## Usage:

```pauls-imac:video_organizer_scripts pauld$ sudo ./findAndOrganizeVideos.py -s /Volumes/Photos/photos_to_sort/2011/2011-12 -t /Volumes/Multimedia/photos2/sorted_videos/```

where
- _-s_ corresponds to the source folder that contains the videos to organise
- _-t_ corresponds to the target folder where sorted video folders with videos should be placed
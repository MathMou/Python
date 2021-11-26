# This script moves data from Impala/HIVE to document through mail_merge and finally writes it to HDFS.
The reason for using for loops is to combine a document with a fixed length and form with multiple other documents which match the same keys in a different database. 
Hereby moving past the limitations of the normal mail_merge usage, where it is not possible to combine one document with several rows from another source. 

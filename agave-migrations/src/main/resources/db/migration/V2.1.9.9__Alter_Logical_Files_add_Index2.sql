###############################################################
# Migration: V2.1.9.9__Alter_Logical_Files_add_index.sql
#
# These changes are intended to update the path_hash field with correct value. 
# The previous version of this script was updating the column with 8 characters from the md5 digest
# Please note that md5 returns characters in hex and there are 2 characters per byte.
# Hence we need to take first 16 characters instead of 8 as was the case in the previous script.
#
# Index changes:
# - logical_files_system_id_path_hash
# + logical_files_system_id_path_hash
#
# Column changes:
# 	
# Data changes:
# Updates the column path_hash with first 8 bytes(16 characters) of MD5 hash of the field path converted 
# to a decimal. Note that -16 base implies signed value.
# The update clause below does the following:
# 1. obtains the md5 hash from the path column value.
# 2. obtains first 16 characters from the digest. These 16 characters are basically equivalent to 8 bytes.
# 3. converts the string to the base 10 from base 16.
# 4. negative 10 implies that the input string (which is in hex i.e base 16) is ok to be interpreted as negative number in base 10
# 5. MySQL docs refer to negative numbers in "from" base, but it does not work and creates output with same long values for two dissimilar strings.
#.   e.g select conv('bcd56daa0ca62978', -16, 10), conv('d6585fc7719e8941', -16, 10); results in
#     '9223372036854775807', '9223372036854775807' 
#################################################################


DROP INDEX logical_files_system_id_path_hash on logical_files;

update logical_files set path_hash = conv(substring(md5(path), 1, 16), 16, -10);

CREATE INDEX `logical_files_system_id_path_hash` ON `logical_files` 
(`system_id`, `path_hash`);


###############################################################
# Migration: V2.1.9.7__Alter_Logical_Files_add_index.sql
#
# LogicalFileDAO.findBySystemAndPath() causes a 
# sequential scan of the logical_files table because there is no
# index to search based on path. There are about 2 million paths
# mapped to a single system in logical_files table. 
#                                      
# Database changes:
#
# Table changes:
# The script below alters the logical_files table adds a new "not null" column
# path_hash.
# The new column will be henceforth used for searches.
#
# Index changes:
# + logical_files_system_id_path_hash
# - FKBB45CEC1BBBF083F
#
# Column changes:
# 	
# Data changes:
# Updates the column path_hash with first 8 bytes of MD5 hash of the field path converted 
# to a decimal.
#################################################################


alter table logical_files add column path_hash bigint not null default 0;

update logical_files set path_hash = conv(substring(md5(path), 1, 8), 16, 10);


CREATE INDEX `logical_files_system_id_path_hash` ON `logical_files` 
(`system_id`, `path_hash`);

DROP INDEX FKBB45CEC1BBBF083F on logical_files;

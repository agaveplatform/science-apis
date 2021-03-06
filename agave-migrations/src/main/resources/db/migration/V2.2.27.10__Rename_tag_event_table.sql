###############################################################
# Migration: V2.2.27.10__Rename_tag_event_table.sql
#
# Renaming tag_event table to tagevents for consistency with other
# history tables. This fixes a bug in the V2.1.9.2__Rename_tag_event_table.sql
# migration where the schema was hard coded inot the query
# definition.
# 									   
# Database changes:
#
# Table changes:
# - tag_event
# + tagevents
#
# Index changes:
#
# Column changes:
# 
# Data changes:
#
#################################################################

SELECT Count(*)
INTO @exists
FROM information_schema.tables 
WHERE table_type = 'BASE TABLE'
    AND table_name = 'tag_event';

SET @query = If(@exists>0,
    'RENAME TABLE tag_event TO tagevents',
    'SELECT \'id\' event');

PREPARE stmt FROM @query;

EXECUTE stmt;

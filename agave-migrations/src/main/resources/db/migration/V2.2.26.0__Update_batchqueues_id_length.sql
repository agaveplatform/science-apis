###############################################################
# Migration: V2.2.26.0__Update_batchqueues_id_length.sql
#
# Updating batchqueues.id column to a biginto supporting at least
# as many records as systems.id.
# 									   
# Database changes:
#
# Table changes:
# - batchqueues
#
# Index changes:
#
# Column changes:
# ~ batchqueues.id BIGINT
# 
# Data changes:
#
#################################################################

# Convert id in batchqueues table from INT(11) to BIGINT(20)

# ------------------------------------------------------------

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'batchqueues' AND column_name = 'id' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `batchqueues` CHANGE `id` `id` BIGINT(22)  NOT NULL  AUTO_INCREMENT;"
    "SELECT 1"
	)); 
PREPARE stmt FROM @s; 
EXECUTE stmt; 
DEALLOCATE PREPARE stmt;

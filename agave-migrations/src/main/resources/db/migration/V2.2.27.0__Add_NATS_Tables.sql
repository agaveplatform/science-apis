###############################################################
# Migration: V2.2.27.0__Add_NATS_Tables.sql
#
# Adding tables for NATS
# 									   
# Database changes:
#
# Table changes:
#
#
# Index changes:
#
# Column changes:
#
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

###############################################################
# Migration: V2.2.28.1__Alter_transfertasks_datetime_to_timestamp.sql
#
# Converting all DATETIME columns to timestamps in transfertasks
# table.
#
# Database changes:
#
# Table changes:
# + transfertasks
#
# Index changes:
#
# Column changes:
# + created
# + last_updated
# Data changes:
#
###############################################################

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'transfertasks' AND column_name = 'created' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `transfertasks` MODIFY COLUMN `created` TIMESTAMP NOT NULL;", "SELECT 1" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'transfertasks' AND column_name = 'last_updated' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `transfertasks` MODIFY COLUMN `last_updated` TIMESTAMP NOT NULL;", "SELECT 1" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'transfertasks' AND column_name = 'start_time' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `transfertasks` MODIFY COLUMN `start_time` TIMESTAMP DEFAULT NULL;", "SELECT 1" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'transfertasks' AND column_name = 'end_time' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `transfertasks` MODIFY COLUMN `end_time` TIMESTAMP DEFAULT NULL;", "SELECT 1" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

/********************************************************************/
/**                                                                **/
/** For reference, here is the unchecked SQL we executed 		   **/
/**                                                                **/
/********************************************************************/
--ALTER TABLE `tags` MODIFY COLUMN `name` VARCHAR(128) NOT NULL;


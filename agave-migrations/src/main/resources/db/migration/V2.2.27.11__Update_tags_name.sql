###############################################################
# Migration: V2.2.27.11__Update_tags_name.sql
#
# Expanding the name field to 128 characters
#
# Database changes:
#
# Table changes:
#
# Index changes:
#
# Column changes:
# + tags.name
#
# Data changes:
#
#################################################################

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'tags' AND column_name = 'name' AND table_schema = DATABASE() ) > 0,
    "ALTER TABLE `tags` MODIFY COLUMN `name` VARCHAR(128) NOT NULL;", "SELECT 1" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

/********************************************************************/
/**                                                                **/
/** For reference, here is the unchecked SQL we executed 		   **/
/**                                                                **/
/********************************************************************/
--ALTER TABLE `tags` MODIFY COLUMN `name` VARCHAR(128) NOT NULL;
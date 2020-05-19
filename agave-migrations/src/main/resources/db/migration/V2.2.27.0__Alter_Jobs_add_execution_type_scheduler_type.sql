###############################################################
# Migration: V2.2.27.0__Alter_Jobs_add_execution_type_scheduler_type.sql
#
# Adding execution_type and scheduler_type columns to jobs table
#
# Database changes:
#
# Table changes:
#
# Index changes:
#
# Column changes:
# + jobs.execution_type
# + jobs.scheduler_type
#
# Data changes:
#
#################################################################

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'jobs' AND column_name = 'execution_type' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "ALTER TABLE `jobs` ADD `execution_type` VARCHAR(16);" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'jobs' AND column_name = 'scheduler_type' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "ALTER TABLE `jobs` ADD `scheduler_type` VARCHAR(16)  NOT NULL;" ));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'jobs' AND index_name = 'software_name_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `software_name_tenant_id` ON `jobs` (`software_name`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'jobs' AND index_name = 'owner_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `owner_tenant_id` ON `jobs` (`owner`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'jobs' AND index_name = 'uuid_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `uuid_tenant_id` ON `jobs` (`uuid`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

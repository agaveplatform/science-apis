###############################################################
# Migration: V2.2.27.1__Alter_Jobs_add_indexes_for_uuid_owner_software.sql
#
# Adding individual indexes for uuid, owner, and software_name by tenant_id to jobs table
#
# Database changes:
#
# Table changes:
#
# Index changes:
# + software_name_tenant_id
# + owner_tenant_id
# + uuid_tenant_id
#
# Column changes:
#
# Data changes:
#
#################################################################

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

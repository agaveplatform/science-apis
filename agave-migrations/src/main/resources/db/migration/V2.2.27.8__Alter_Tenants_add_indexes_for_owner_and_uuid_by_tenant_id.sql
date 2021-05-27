############################################################################################
# Migration: V2.2.27.8__Alter_Tenants_add_indexes_for_owner_and_uuid_by_tenant_id.sql
#
# Adding index for owner by tenant_id to tenants table
#
# Database changes:
#
# Table changes:
#
# Index changes:
# + owner_tenant_id
# + uuid_tenant_id
#
# Column changes:
#
# Data changes:
#
##############################################################################################

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'tenants' AND index_name = 'tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `tenant_id` ON `tenants` (`tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'tenants' AND index_name = 'uuid' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `uuid` ON `tenants` (`uuid`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
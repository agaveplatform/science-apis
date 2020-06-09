#################################################################################################
# Migration: V2.2.27.3__Alter_Postits_add_indexes_for_creator_postit_key_token_by_tenant_id.sql
#
# Adding index for owner by tenant_id to postits table
#
# Database changes:
#
# Table changes:
#
# Index changes:
# + creator_tenant_id
# + postit_key_tenant_id
# + token_tenant_id
#
# Column changes:
#
# Data changes:
#
#################################################################################################

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'postits' AND index_name = 'creator_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `creator_tenant_id` ON `postits` (`creator`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'postits' AND index_name = 'postit_key_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `postit_key_tenant_id` ON `postits` (`postit_key`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @s = (SELECT IF((SELECT DISTINCT COUNT(*) FROM INFORMATION_SCHEMA.statistics
    WHERE table_name = 'postits' AND index_name = 'token_tenant_id' AND table_schema = DATABASE() ) > 0, "SELECT 1",
    "CREATE INDEX `token_tenant_id` ON `postits` (`token`, `tenant_id`);"));
PREPARE stmt FROM @s;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

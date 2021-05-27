###############################################################
# Migration: V2.2.0.2__Add_systemroles_index.sql
#
# SystemDao.findUserSystemBySystemId() causes a 
# full scan of the systemroles table because no index is 
# available.  This file creates an index that can be used in
# that query.
#                                      
# Database changes:
#
# Table changes:
# 
# Index changes:
# + systemroles_system_username
#
# Column changes:
# 
# Data changes:
#
#################################################################

/** Add a non-unique index because we use an improper collation
 *  method that is case insensitive.  The table actually contains
 *  rows whose system/username differ only in username case, 
 *  which prohibits the creation of a unique index on these 2 fields. 
 * */
CREATE INDEX `systemroles_system_username` ON `systemroles` 
(`remote_system_id`, `username`);

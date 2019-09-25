###############################################################
# Migration: V2.1.9.7__Add_remotefilepermission_index.sql
#
# RemoteFilePermissionDao.getByUsernameAndlogicalFileId() causes a 
# full scan of the remotefilepermissions table because no index is 
# available.  This file creates an index that can be used in
# that query and any other query in RemoteFilePermissionDao that filters
# on the logicalFileId and username fields.
#                                      
# Database changes:
#
# Table changes:
# 
# Index changes:
# + remotefilepermission_logicalfileId_plus
#
# Column changes:
# 
# Data changes:
#
#################################################################

/** Add a multi-use index **/
CREATE INDEX `remotefilepermission_logical_file_id_plus` ON `remotefilepermissions` 
(`logical_file_id`, `username`);

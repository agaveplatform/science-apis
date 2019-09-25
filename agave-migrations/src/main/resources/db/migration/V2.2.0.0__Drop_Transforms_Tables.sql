###############################################################
# Migration: V2.2.0.0__Drop_Transforms_Tables.sql
#
# These changes are part of the decommissioning of the unused 
# transforms facility.  No new data in either table for 2 years.    
#################################################################

DROP TABLE IF EXISTS `encoding_tasks`;

DROP TABLE IF EXISTS `decoding_tasks`;

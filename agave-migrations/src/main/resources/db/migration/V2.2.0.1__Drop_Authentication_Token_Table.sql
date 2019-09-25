###############################################################
# Migration: V2.2.0.1__Drop_Authentication_Token_Table.sql
#
# These changes are part of the removal of the unused authentication
# token code.
#################################################################

DROP TABLE IF EXISTS `authentication_tokens`

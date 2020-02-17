###############################################################
# Migration: V2.2.27.0__Add_NATS_Tables.sql
#
# Adding tables for NATS
# 									   
# Database changes:
#
# Table changes:
#
#
# Index changes:
#
# Column changes:
#
# 
# Data changes:
#
#################################################################
#CREATE USER 'nss'@'localhost' IDENTIFIED BY 'password'; GRANT ALL PRIVILEGES ON *.* TO 'nss'@'localhost'; CREATE DATABASE nss_db;

DROP TABLE IF EXISTS `ServerInfo`;
CREATE TABLE IF NOT EXISTS ServerInfo (uniquerow INT DEFAULT 1, id VARCHAR(1024), proto BLOB, version INTEGER, PRIMARY KEY (uniquerow));

DROP TABLE IF EXISTS `Clients`;
CREATE TABLE IF NOT EXISTS Clients (id VARCHAR(1024), hbinbox TEXT, PRIMARY KEY (id(256)));

DROP TABLE IF EXISTS `Channels`;
CREATE TABLE IF NOT EXISTS Channels (id INTEGER, name VARCHAR(1024) NOT NULL, maxseq BIGINT UNSIGNED DEFAULT 0, maxmsgs INTEGER DEFAULT 0, maxbytes BIGINT DEFAULT 0, maxage BIGINT DEFAULT 0, deleted BOOL DEFAULT FALSE, PRIMARY KEY (id), INDEX Idx_ChannelsName (name(256)));

DROP TABLE IF EXISTS `Messages`;
CREATE TABLE IF NOT EXISTS Messages (id INTEGER, seq BIGINT UNSIGNED, timestamp BIGINT, size INTEGER, data BLOB, CONSTRAINT PK_MsgKey PRIMARY KEY(id, seq), INDEX Idx_MsgsTimestamp (timestamp));

DROP TABLE IF EXISTS `Subscriptions`;
CREATE TABLE IF NOT EXISTS Subscriptions (id INTEGER, subid BIGINT UNSIGNED, lastsent BIGINT UNSIGNED DEFAULT 0, proto BLOB, deleted BOOL DEFAULT FALSE, CONSTRAINT PK_SubKey PRIMARY KEY(id, subid));

DROP TABLE IF EXISTS `SubsPending`;
CREATE TABLE IF NOT EXISTS SubsPending (subid BIGINT UNSIGNED, `row` BIGINT UNSIGNED, seq BIGINT UNSIGNED DEFAULT 0, lastsent BIGINT UNSIGNED DEFAULT 0, pending BLOB, acks BLOB, CONSTRAINT PK_MsgPendingKey PRIMARY KEY(subid, `row`), INDEX Idx_SubsPendingSeq(seq));

DROP TABLE IF EXISTS `StoreLock`;
CREATE TABLE IF NOT EXISTS StoreLock (id VARCHAR(30), tick BIGINT UNSIGNED DEFAULT 0);

# Updates for 0.10.0
ALTER TABLE Clients ADD proto BLOB;


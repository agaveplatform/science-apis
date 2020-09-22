###############################################################
# Migration: V2.2.28.0__Create_TransferApiTasks_table_.sql
#
# Adding transferapitasks table for the new transfers-api service.
# This is the first step in deprecating and eventually replacing the
# transfertasks table.
#
# Database changes:
#
# Table changes:
# + transferapitasks
#
# Index changes:
#
# Column changes:
#
# Data changes:
#
###############################################################

DROP TABLE IF EXISTS `TransferApiTasks`;

CREATE or replace TABLE `TransferApiTasks` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `attempts` int(11) DEFAULT NULL,
    `bytes_transferred` bigint(20) NOT NULL DEFAULT 0,
    `created` TIMESTAMP NOT NULL,
    `dest` varchar(2048) NOT NULL DEFAULT '',
    `end_time` TIMESTAMP DEFAULT NULL,
    `event_id` varchar(255) DEFAULT NULL,
    `last_updated` TIMESTAMP NOT NULL,
    `owner` varchar(32) NOT NULL,
    `source` varchar(2048) NOT NULL DEFAULT '',
    `start_time` TIMESTAMP DEFAULT NULL,
    `status` varchar(16) DEFAULT NULL,
    `tenant_id` varchar(128) NOT NULL,
    `total_size` bigint(20) DEFAULT NULL,
    `transfer_rate` double DEFAULT NULL,
    `parent_task` varchar(64) DEFAULT NULL,
    `root_task` varchar(64) DEFAULT NULL,
    `uuid` varchar(64) NOT NULL DEFAULT '',
    `total_files` bigint(20) NOT NULL DEFAULT 0,
    `total_skipped` bigint(20) NOT NULL DEFAULT 0,
    `optlock` int(11) DEFAULT NULL,
    `last_attempt` TIMESTAMP DEFAULT NULL,
    `next_attempt` TIMESTAMP DEFAULT NULL,
    `total_skipped_files` bigint(20) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uuid_tenant` (`uuid`,`tenant_id`),
    KEY `parent_task_tenant_id` (`parent_task`,`tenant_id`),
    KEY `root_task_tenant_id` (`root_task`,`tenant_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


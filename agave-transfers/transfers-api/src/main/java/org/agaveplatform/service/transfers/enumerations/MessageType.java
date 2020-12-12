package org.agaveplatform.service.transfers.enumerations;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.agaveplatform.service.transfers.model.TransferTask;

public interface MessageType {
    public static final String 	TRANSFERTASK_CREATED = "transfertask.created";
    public static final String 	TRANSFERTASK_ASSIGNED = "transfertask.assigned";
    public static final String  TRANSFERTASK_CANCELED_SYNC = "transfertask.canceled.sync";
    public static final String  TRANSFERTASK_CANCELED_COMPLETED = "transfertask.canceled.completed";
    public static final String  TRANSFERTASK_CANCELED_ACK = "transfertask.canceled.ack";
    public static final String 	TRANSFERTASK_CANCELLED = "transfertask.canceled";
    public static final String 	TRANSFERTASK_PAUSED = "transfertask.paused";
    public static final String 	TRANSFERTASK_PAUSED_SYNC = "transfertask.paused.sync";
    public static final String 	TRANSFERTASK_PAUSED_COMPLETED = "transfertask.paused.completed";
    public static final String 	TRANSFERTASK_PAUSED_ACK = "transfertask.paused.ack";
    public static final String 	TRANSFER_COMPLETED = "transfer.completed";
    public static final String 	TRANSFERTASK_COMPLETED = "transfertask.completed";
    public static final String 	TRANSFERTASK_ERROR = "transfertask.error";
    public static final String 	TRANSFERTASK_PARENT_ERROR = "transfertask.parent.error";
    public static final String 	TRANSFERTASK_FAILED = "transfertask.failed";
    public static final String  TRANSFERTASK_INTERUPTED = "transfertask.interupted";
    public static final String 	NOTIFICATION = "notification";
    public static final String 	NOTIFICATION_TRANSFERTASK = "notification.transfertask";
    public static final String 	NOTIFICATION_CANCELLED = "notification.cancelled";
    public static final String 	NOTIFICATION_COMPLETED = "notification.completed";
    public static final String 	TRANSFER_SFTP = "transfer.sftp";
    public static final String 	TRANSFER_HTTP = "transfer.http";
    public static final String 	TRANSFER_GRIDFTP = "transfer.gridftp";
    public static final String 	TRANSFER_FTP = "transfer.ftp";
    public static final String 	TRANSFER_IRODS = "transfer.irods";
    public static final String 	TRANSFER_IRODS4 = "transfer.irods4";
    public static final String 	TRANSFER_LOCAL = "transfer.local";
    public static final String 	TRANSFER_AZURE = "transfer.azure";
    public static final String 	TRANSFER_S3 = "transfer.s3";
    public static final String 	TRANSFER_SWIFT = "transfer.swift";
    public static final String 	TRANSFER_HTTPS = "transfer.https";
    public static final String 	FILETRANSFER_SFTP = "filetransfer.sftp";
    public static final String 	TRANSFERTASK_DB_QUEUE = "transfertask.db.queue";
    public static final String 	TRANSFERTASK_DELETED = "transfertask.deleted";
    public static final String 	TRANSFERTASK_DELETED_SYNC = "transfertask.deleted.sync";
    public static final String 	TRANSFERTASK_DELETED_COMPLETED = "transfertask.deleted.completed";
    public static final String 	TRANSFERTASK_DELETED_ACK = "transfertask.deleted.ack";
    public static final String 	TRANSFERTASK_UPDATED = "transfertask.updated";
    public static final String 	TRANSFERTASK_PROCESS_UNARY = "transfertask.process.unary";
    public static final String 	TRANSFER_STREAMING = "transfer.streaming";
    public static final String 	TRANSFER_UNARY = "transfer.unary";
    public static final String  TRANSFERTASK_HEALTHCHECK = "transfertask.healthcheck";
    public static final String  TRANSFER_FAILED = "transfer.failed";
    public static final String  TRANSFER_RETRY = "transfer.retry";
    public static final String  TRANSFER_ALL = "transfer.all";
    public static final String  TRANSFERTASK_NOTIFICATION = "transfertask.notification";
    public static final String  UrlCopy = "transfertask.UrlCopy";
}
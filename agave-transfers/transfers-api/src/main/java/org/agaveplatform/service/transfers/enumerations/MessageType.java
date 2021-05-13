package org.agaveplatform.service.transfers.enumerations;

import org.iplantc.service.transfer.AbstractRemoteTransferListener;


/**
 * These are the internal message types used by the concrete {@link AbstractRemoteTransferListener}
 * classes to listen for transfer task events and process their assigned work. These do not
 * correspond 1-1 with {@link TransferStatusType} or {@link TransferTaskEventType}, as they cover all
 * possible internal messages that may need to be created for the internal workings of the service.
 */

public interface MessageType {                                                                      // type         Ack
    public static final String 	TRANSFERTASK_CREATED = "transfertask.created";                      // subscribe, Explicit,
    public static final String 	TRANSFERTASK_ASSIGNED = "transfertask.assigned";                    // subscribe, Explicit
    public static final String  TRANSFERTASK_CANCELED_SYNC = "transfertask.canceled-sync";          // subscribe, Explicit
    public static final String  TRANSFERTASK_CANCELED_COMPLETED = "transfertask.canceled-completed";// subscribe, Explicit
    public static final String  TRANSFERTASK_CANCELED_ACK = "transfertask.canceled-ack";            // subscribe, Explicit
    public static final String 	TRANSFERTASK_CANCELED = "transfertask.canceled";                    // push, None
    public static final String 	TRANSFERTASK_PAUSED = "transfertask.paused";                        // push, None
    public static final String 	TRANSFERTASK_PAUSED_SYNC = "transfertask.paused-sync";              // subscribe, None
    public static final String 	TRANSFERTASK_PAUSED_COMPLETED = "transfertask.paused-completed";    // subscribe, None
    public static final String 	TRANSFERTASK_PAUSED_ACK = "transfertask.paused-ack";                // subscribe, None
    public static final String 	TRANSFER_COMPLETED = "transfer.completed";                          // subscribe, Explicit
    public static final String  TRANSFERTASK_FINISHED = "transfertask.finished";                    // subscribe, Explicit
    public static final String 	TRANSFERTASK_ERROR = "transfertask.error";                          // subscribe, Explicit
    public static final String 	TRANSFERTASK_PARENT_ERROR = "transfertask.parent-error";            // subscribe, Explicit
    public static final String 	TRANSFERTASK_FAILED = "transfertask.failed";                        // subscribe, Explicit
    public static final String  TRANSFERTASK_INTERUPTED = "transfertask.interupted";                // subscribe, Explicit
    public static final String 	NOTIFICATION = "notification.send";                                      // subscribe, Explicit
    public static final String 	NOTIFICATION_TRANSFERTASK = "notification.transfertask";            // subscribe, Explicit
    public static final String 	NOTIFICATION_CANCELED = "notification.cancelled";                   // subscribe, Explicit
    public static final String 	NOTIFICATION_COMPLETED = "notification.completed";                  // subscribe, Explicit
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
//    public static final String 	FILETRANSFER_SFTP = "filetransfer.sftp";
    public static final String 	TRANSFERTASK_DB_QUEUE = "transfertask.db-queue";                    // subscribe, Explicit
    public static final String 	TRANSFERTASK_DELETED = "transfertask.deleted";                      // push, Explicit
    public static final String 	TRANSFERTASK_DELETED_SYNC = "transfertask.deleted-sync";            // subscribe, Explicit
    public static final String 	TRANSFERTASK_DELETED_COMPLETED = "transfertask.deleted-completed";  // subscribe, Explicit
    public static final String 	TRANSFERTASK_DELETED_ACK = "transfertask.deleted-ack";              // subscribe, Explicit
    public static final String 	TRANSFERTASK_UPDATED = "transfertask.updated";                      // subscribe, Explicit
    public static final String 	TRANSFERTASK_PROCESS_UNARY = "transfertask.process-unary";          // subscribe, Explicit
    public static final String 	TRANSFER_STREAMING = "transfer.streaming";                          // subscribe, Explicit
    public static final String 	TRANSFER_UNARY = "transfer.unary";                                  // subscribe, Explicit
    public static final String  TRANSFERTASK_HEALTHCHECK = "transfertask.healthcheck";              // subscribe, Explicit
    public static final String  TRANSFERTASK_HEALTHCHECK_PARENT = "transfertask.healthcheck-parent";// subscribe, Explicit
    public static final String  TRANSFER_FAILED = "transfer.failed";                                // subscribe, Explicit
    public static final String  TRANSFER_RETRY = "transfer.retry";                                  // subscribe, Explicit
    public static final String  TRANSFER_ALL = "transfer.all";                                      // subscribe, Explicit
    public static final String  TRANSFERTASK_NOTIFICATION = "transfertask.notification";            // subscribe, Explicit
    //public static final String  UrlCopy = "transfertask.UrlCopy";
}

package org.agaveplatform.service.transfers.enumerations;

import org.iplantc.service.transfer.AbstractRemoteTransferListener;


/**
 * These are the internal message types used by the concrete {@link AbstractRemoteTransferListener}
 * classes to listen for transfer task events and process their assigned work. These do not
 * correspond 1-1 with {@link TransferStatusType} or {@link TransferTaskEventType}, as they cover all
 * possible internal messages that may need to be created for the internal workings of the service.
 */

public interface MessageType {                                                                      // type         Ack
    String 	TRANSFERTASK_CREATED = "transfertask.created";                      // subscribe, Explicit,
    String 	TRANSFERTASK_ASSIGNED = "transfertask.assigned";                    // subscribe, Explicit
    String  TRANSFERTASK_CANCELED_SYNC = "transfertask.canceled-sync";          // subscribe, Explicit
    String  TRANSFERTASK_CANCELED_COMPLETED = "transfertask.canceled-completed";// subscribe, Explicit
    String  TRANSFERTASK_CANCELED_ACK = "transfertask.canceled-ack";            // subscribe, Explicit
    String 	TRANSFERTASK_CANCELED = "transfertask.canceled";                    // push, None
    String 	TRANSFERTASK_PAUSED = "transfertask.paused";                        // push, None
    String 	TRANSFERTASK_PAUSED_SYNC = "transfertask.paused-sync";              // subscribe, None
    String 	TRANSFERTASK_PAUSED_COMPLETED = "transfertask.paused-completed";    // subscribe, None
    String 	TRANSFERTASK_PAUSED_ACK = "transfertask.paused-ack";                // subscribe, None
    String 	TRANSFER_COMPLETED = "transfer.completed";                          // subscribe, Explicit
    String  TRANSFERTASK_FINISHED = "transfertask.finished";                    // subscribe, Explicit
    String 	TRANSFERTASK_ERROR = "transfertask.error";                          // subscribe, Explicit
    String 	TRANSFERTASK_PARENT_ERROR = "transfertask.parent-error";            // subscribe, Explicit
    String 	TRANSFERTASK_FAILED = "transfertask.failed";                        // subscribe, Explicit
    String  TRANSFERTASK_INTERUPTED = "transfertask.interupted";                // subscribe, Explicit
    String 	NOTIFICATION = "notification.send";                                      // subscribe, Explicit
    String 	NOTIFICATION_TRANSFERTASK = "notification.transfertask";            // subscribe, Explicit
    String 	NOTIFICATION_CANCELED = "notification.cancelled";                   // subscribe, Explicit
    String 	NOTIFICATION_COMPLETED = "notification.completed";                  // subscribe, Explicit
    String 	TRANSFER_SFTP = "transfer.sftp";
    String 	TRANSFER_HTTP = "transfer.http";
    String 	TRANSFER_GRIDFTP = "transfer.gridftp";
    String 	TRANSFER_FTP = "transfer.ftp";
    String 	TRANSFER_IRODS = "transfer.irods";
    String 	TRANSFER_IRODS4 = "transfer.irods4";
    String 	TRANSFER_LOCAL = "transfer.local";
    String 	TRANSFER_AZURE = "transfer.azure";
    String 	TRANSFER_S3 = "transfer.s3";
    String 	TRANSFER_SWIFT = "transfer.swift";
    String 	TRANSFER_HTTPS = "transfer.https";
//    public static final String 	FILETRANSFER_SFTP = "filetransfer.sftp";
String 	TRANSFERTASK_DB_QUEUE = "transfertask.db-queue";                    // subscribe, Explicit
    String 	TRANSFERTASK_DELETED = "transfertask.deleted";                      // push, Explicit
    String 	TRANSFERTASK_DELETED_SYNC = "transfertask.deleted-sync";            // subscribe, Explicit
    String 	TRANSFERTASK_DELETED_COMPLETED = "transfertask.deleted-completed";  // subscribe, Explicit
    String 	TRANSFERTASK_DELETED_ACK = "transfertask.deleted-ack";              // subscribe, Explicit
    String 	TRANSFERTASK_UPDATED = "transfertask.updated";                      // subscribe, Explicit
    String 	TRANSFERTASK_PROCESS_UNARY = "transfertask.process-unary";          // subscribe, Explicit
    String 	TRANSFER_STREAMING = "transfer.streaming";                          // subscribe, Explicit
    String 	TRANSFER_UNARY = "transfer.unary";                                  // subscribe, Explicit
    String  TRANSFERTASK_HEALTHCHECK = "transfertask.healthcheck";              // subscribe, Explicit
    String  TRANSFERTASK_HEALTHCHECK_PARENT = "transfertask.healthcheck-parent";// subscribe, Explicit
    String  TRANSFER_FAILED = "transfer.failed";                                // subscribe, Explicit
    String  TRANSFER_RETRY = "transfer.retry";                                  // subscribe, Explicit
    String  TRANSFER_ALL = "transfer.all";                                      // subscribe, Explicit
    String  TRANSFERTASK_NOTIFICATION = "transfertask.notification";            // subscribe, Explicit
    //public static final String  UrlCopy = "transfertask.UrlCopy";
    
    static String[] values() {
        return new String[]{
            TRANSFERTASK_CREATED,
            TRANSFERTASK_ASSIGNED,
            TRANSFERTASK_CANCELED_SYNC,
            TRANSFERTASK_CANCELED_COMPLETED,
            TRANSFERTASK_CANCELED_ACK,
            TRANSFERTASK_CANCELED,
            TRANSFERTASK_PAUSED,
            TRANSFERTASK_PAUSED_SYNC,
            TRANSFERTASK_PAUSED_COMPLETED,
            TRANSFERTASK_PAUSED_ACK,
            TRANSFER_COMPLETED,
            TRANSFERTASK_FINISHED,
            TRANSFERTASK_ERROR,
            TRANSFERTASK_PARENT_ERROR,
            TRANSFERTASK_FAILED,
            TRANSFERTASK_INTERUPTED,
            NOTIFICATION,
            NOTIFICATION_TRANSFERTASK,
            NOTIFICATION_CANCELED,
            NOTIFICATION_COMPLETED,
            TRANSFER_SFTP,
            TRANSFER_HTTP,
            TRANSFER_GRIDFTP,
            TRANSFER_FTP,
            TRANSFER_IRODS,
            TRANSFER_IRODS4,
            TRANSFER_LOCAL,
            TRANSFER_AZURE,
            TRANSFER_S3,
            TRANSFER_SWIFT,
            TRANSFER_HTTPS,
            TRANSFERTASK_DB_QUEUE,
            TRANSFERTASK_DELETED,
            TRANSFERTASK_DELETED_SYNC,
            TRANSFERTASK_DELETED_COMPLETED,
            TRANSFERTASK_DELETED_ACK,
            TRANSFERTASK_UPDATED,
            TRANSFERTASK_PROCESS_UNARY,
            TRANSFER_STREAMING,
            TRANSFER_UNARY,
            TRANSFERTASK_HEALTHCHECK,
            TRANSFERTASK_HEALTHCHECK_PARENT,
            TRANSFER_FAILED,
            TRANSFER_RETRY,
            TRANSFER_ALL,
            TRANSFERTASK_NOTIFICATION
        };
    }
}

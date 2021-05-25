package org.agaveplatform.service.transfers.enumerations;

import org.iplantc.service.transfer.AbstractRemoteTransferListener;


/**
 * These are the internal message types used by the concrete {@link AbstractRemoteTransferListener}
 * classes to listen for transfer task events and process their assigned work. These do not
 * correspond 1-1 with {@link TransferStatusType} or {@link TransferTaskEventType}, as they cover all
 * possible internal messages that may need to be created for the internal workings of the service.
 */

public interface MessageType {                                                                      // type         Ack
    String 	TRANSFERTASK_CREATED = "transfertask_created";                      // subscribe, Explicit,
    String 	TRANSFERTASK_ASSIGNED = "transfertask_assigned";                    // subscribe, Explicit
    String  TRANSFERTASK_CANCELED_SYNC = "transfertask_canceled-sync";          // subscribe, Explicit
    String  TRANSFERTASK_CANCELED_COMPLETED = "transfertask_canceled-completed";// subscribe, Explicit
    String  TRANSFERTASK_CANCELED_ACK = "transfertask_canceled-ack";            // subscribe, Explicit
    String 	TRANSFERTASK_CANCELED = "transfertask_canceled";                    // push, None
    String 	TRANSFERTASK_PAUSED = "transfertask_paused";                        // push, None
    String 	TRANSFERTASK_PAUSED_SYNC = "transfertask_paused-sync";              // subscribe, None
    String 	TRANSFERTASK_PAUSED_COMPLETED = "transfertask_paused-completed";    // subscribe, None
    String 	TRANSFERTASK_PAUSED_ACK = "transfertask_paused-ack";                // subscribe, None
    String 	TRANSFER_COMPLETED = "transfer_completed";                          // subscribe, Explicit
    String  TRANSFERTASK_FINISHED = "transfertask_finished";                    // subscribe, Explicit
    String 	TRANSFERTASK_ERROR = "transfertask_error";                          // subscribe, Explicit
    String 	TRANSFERTASK_PARENT_ERROR = "transfertask_parent-error";            // subscribe, Explicit
    String 	TRANSFERTASK_FAILED = "transfertask_failed";                        // subscribe, Explicit
    String  TRANSFERTASK_INTERUPTED = "transfertask_interupted";                // subscribe, Explicit
    String 	NOTIFICATION = "notification_send";                                      // subscribe, Explicit
    String 	NOTIFICATION_TRANSFERTASK = "notification_transfertask";            // subscribe, Explicit
    String 	NOTIFICATION_CANCELED = "notification_cancelled";                   // subscribe, Explicit
    String 	NOTIFICATION_COMPLETED = "notification_completed";                  // subscribe, Explicit
    String 	TRANSFER_SFTP = "transfer_sftp";
    String 	TRANSFER_HTTP = "transfer_http";
    String 	TRANSFER_GRIDFTP = "transfer_gridftp";
    String 	TRANSFER_FTP = "transfer_ftp";
    String 	TRANSFER_IRODS = "transfer_irods";
    String 	TRANSFER_IRODS4 = "transfer_irods4";
    String 	TRANSFER_LOCAL = "transfer_local";
    String 	TRANSFER_AZURE = "transfer_azure";
    String 	TRANSFER_S3 = "transfer_s3";
    String 	TRANSFER_SWIFT = "transfer_swift";
    String 	TRANSFER_HTTPS = "transfer_https";
//    public static final String 	FILETRANSFER_SFTP = "filetransfer_sftp";
String 	TRANSFERTASK_DB_QUEUE = "transfertask_db-queue";                    // subscribe, Explicit
    String 	TRANSFERTASK_DELETED = "transfertask_deleted";                      // push, Explicit
    String 	TRANSFERTASK_DELETED_SYNC = "transfertask_deleted-sync";            // subscribe, Explicit
    String 	TRANSFERTASK_DELETED_COMPLETED = "transfertask_deleted-completed";  // subscribe, Explicit
    String 	TRANSFERTASK_DELETED_ACK = "transfertask_deleted-ack";              // subscribe, Explicit
    String 	TRANSFERTASK_UPDATED = "transfertask_updated";                      // subscribe, Explicit
    String 	TRANSFERTASK_PROCESS_UNARY = "transfertask_process-unary";          // subscribe, Explicit
    String 	TRANSFER_STREAMING = "transfer_streaming";                          // subscribe, Explicit
    String 	TRANSFER_UNARY = "transfer_unary";                                  // subscribe, Explicit
    String  TRANSFERTASK_HEALTHCHECK = "transfertask_healthcheck";              // subscribe, Explicit
    String  TRANSFERTASK_HEALTHCHECK_PARENT = "transfertask_healthcheck-parent";// subscribe, Explicit
    String  TRANSFER_FAILED = "transfer_failed";                                // subscribe, Explicit
    String  TRANSFER_RETRY = "transfer_retry";                                  // subscribe, Explicit
    String  TRANSFER_ALL = "transfer_all";                                      // subscribe, Explicit
    String  TRANSFERTASK_NOTIFICATION = "transfertask_notification";            // subscribe, Explicit
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

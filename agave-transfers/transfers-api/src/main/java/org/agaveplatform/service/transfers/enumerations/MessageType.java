package org.agaveplatform.service.transfers.enumerations;

public enum MessageType {
	TRANSFERTASK_CREATED("transfertask.created"),
	TRANSFERTASK_ASSIGNED("transfertask.assigned"),
	TRANSFERTASK_CANCEL_SYNC("transfertask.cancel.sync"),
	TRANSFERTASK_CANCEL_COMPLETE("transfertask.cancel.complete"),
	TRANSFERTASK_CANCEL_COMPLETED("transfertask.cancel.completed"),
	TRANSFERTASK_CANCEL_ACK("transfertask.cancel.ack"),
	TRANSFERTASK_CANCELLED("transfertask.cancelled"),
	TRANSFERTASK_PAUSED("transfertask.paused"),
	TRANSFERTASK_PAUSED_SYNC("transfertask.paused.sync"),
	TRANSFERTASK_PAUSED_COMPLETE("transfertask.paused.complete"),
	TRANSFERTASK_PAUSED_ACK("transfertask.paused.ack"),
	TRANSFER_COMPLETED("transfer.completed"),
	TRANSFER_COMPLETE("transfer.complete"),
	TRANSFERTASK_COMPLETED("transfertask.completed"),
	TRANSFERTASK_ERROR("transfertask.error"),
	TRANSFERTASK_PARENT_ERROR("transfertask.parent.error"),
	TRANSFERTASK_FAILED("transfertask.failed"),
	TRANSFERTASK_INTERUPT("transfertask.interupt"),
	NOTIFICATION("notification"),
	NOTIFICATION_TRANSFERTASK("notification.transfertask"),
	NOTIFICATION_CANCELLED("notification.cancelled"),
	NOTIFICATION_COMPLETED("notification.completed"),
	TRANSFER_SFTP("transfer.sftp"),
	TRANSFER_HTTP("transfer.http"),
	TRANSFER_GRIDFTP("transfer.gridftp"),
	TRANSFER_FTP("transfer.ftp"),
	TRANSFER_IRODS("transfer.irods"),
	TRANSFER_IRODS4("transfer.irods4"),
	TRANSFER_LOCAL("transfer.local"),
	TRANSFER_AZURE("transfer.azure"),
	TRANSFER_S3("transfer.s3"),
	TRANSFER_SWIFT("transfer.swift"),
	TRANSFER_HTTPS("transfer.https"),
	FILETRANSFER_SFTP("filetransfer.sftp"),
	TRANSFERTASK_DB_QUEUE("transfertask.db.queue"),
	TRANSFERTASK_DELETED("transfertask.deleted"),
	TRANSFERTASK_UPDATED("transfertask.updated"),
	TRANSFERTASK_PROCESS_UNARY("transfertask.process.unary"),
	TRANSFER_STREAMING("transfer.streaming"),
	TRANSFER_UNARY("transfer.unary");

	private String eventChannel;

	private MessageType(String eventChannel) {
		setEventChannel(eventChannel);
	}

	/**
	 * @return the eventChannel
	 */
	public String getEventChannel() {
		return eventChannel;
	}

	/**
	 * @param eventChannel the description to set
	 */
	public void setEventChannel(String eventChannel) {
		this.eventChannel = eventChannel;
	}
}

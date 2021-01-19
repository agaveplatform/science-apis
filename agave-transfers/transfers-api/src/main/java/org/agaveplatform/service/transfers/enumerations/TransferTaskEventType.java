package org.agaveplatform.service.transfers.enumerations;

/**
 * These are the user-facing notification events to which a user can subscribe for a transfer task. These do not
 * line up 1-1 with the internal {@link MessageType} events used to process the state machine, but do honor the
 * published event subscription contract in the platform for which the users are familiar. These also do not
 * line up 1-1 with {@link TransferStatusType}, as multiple {@link TransferStatusType} may map to a single
 * {@link TransferTaskEventType}, such as a {@link org.agaveplatform.service.transfers.model.TransferTask} failing
 * for multiple reasons would result in the same {@link TransferTaskEventType#FAILED} notification being sent.
 *
 * @author dooley
 *
 */
public enum TransferTaskEventType {

	CREATED("This transfer task was created"),
	UPDATED("This transfer task was updated"),
	DELETED("This transfer task was deleted"),

	STARTED("This transfer task started transferring data"),
	RELAY_READ_STARTED("Read of remote source started"),
	RELAY_READ_COMPLETED("Read of remote source completed"),
	RELAY_WRITE_STARTED("Write to remote destination started"),
	RELAY_WRITE_COMPLETED("Write to remote destination completed"),
	STREAM_COPY_STARTED("Streaming copy started"),
	STREAM_COPY_COMPLETED("Streaming copy completed"),

	RESTARTED("This transfer task was restarted"),
	CANCELLED("This transfer task was cancelled"),
	COMPLETED("This transfer task completed successfully"),
	FAILED("This transfer task failed"),

	RETRY("This transfer task is being retried"),
	PAUSED("This transfer task was paused"),
	RESUME("This transfer task was resumed"),
	RESET("This transfer task was reset to the beginning"),

	CHECKSUM_STARTED("This transfer task began calculating checksum(s) for the transferred data"),
	CHECKSUM_COMPLETED("This transfer finished calculating checksum(s) for the transferred data"),
	CHECKSUM_FAILED("This transfer finished calculating checksum(s) for the transferred data"),

	COMPRESSION_STARTED("This transfer task began compressing transferred data"),
	COMPRESSION_COMPLETED("This transfer finished compressing transferred data"),
	COMPRESSION_FAILED("This transfer finished compressing transferred data"),

	BUILDING_MANIFEST("This transfer task is building a manifest of all files and folders to be transferred");

	private String description;

	private TransferTaskEventType(String description) {
		setDescription(description);
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

}
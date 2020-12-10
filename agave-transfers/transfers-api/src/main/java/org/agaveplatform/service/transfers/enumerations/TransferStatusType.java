package org.agaveplatform.service.transfers.enumerations;

import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.transfer.model.enumerations.ITransferStatus;

import java.util.List;

/**
 * These represent the states comprising the state machine of a{@link TransferTask}.
 */
public enum TransferStatusType implements ITransferStatus
{
	CREATED,
	CANCELLED, CANCELING_WAITING, CANCELLED_ERROR,
	DELETED, DELETED_WAITING, DELETED_ERROR,
	PROCESSING_DIRECTORY,
	ASSIGNED,
	TRANSFERRING,
	READ_STARTED, READ_IN_PROGRESS, READ_COMPLETED, WRITE_STARTED, WRITE_IN_PROGRESS, WRITE_COMPLETED,
	STREAM_COPY_STARTED, STREAM_COPY_IN_PROGRESS, STREAM_COPY_COMPLETED,
	RETRYING,
	FAILED,
	COMPLETED, COMPLETED_WITH_ERRORS,
	PAUSED, PAUSE_WAITING, QUEUED,
	ERROR;

	/**
	 * Turns {@link #values()} into a comma separated string
	 * @return the values joined by ", "
	 */
	public static String supportedValuesAsString() {
		return StringUtils.join(values(), ", ");
	}

	/**
	 * Returns {@link TransferStatusType} that represent active statuses
	 * @return immutable list of the active {@link TransferStatusType}
	 */
	public static List<TransferStatusType> getActive() {
		return List.of(CREATED,
				PROCESSING_DIRECTORY,
				ASSIGNED,
				TRANSFERRING,
				READ_STARTED, READ_IN_PROGRESS, READ_COMPLETED, WRITE_STARTED, WRITE_IN_PROGRESS, WRITE_COMPLETED,
				STREAM_COPY_STARTED, STREAM_COPY_IN_PROGRESS, STREAM_COPY_COMPLETED,
				RETRYING,
				PAUSED, PAUSE_WAITING, QUEUED);
	}

	/**
	 * Returns {@link TransferStatusType} that represent stopped statuses. These are tasks that were cancelled
	 * or failed due to outside interference.
	 * @return immutable list of the active {@link TransferStatusType}
	 */
	public static List<TransferStatusType> getStopped() {
		return List.of(CANCELLED, CANCELING_WAITING, CANCELLED_ERROR,
				DELETED, DELETED_WAITING, DELETED_ERROR,
				COMPLETED_WITH_ERRORS, FAILED, ERROR);
	}

	/**
	 * Checks that the status is one of the stopped states.
	 * @return true if this {@link TransferStatusType} is present in {@link #getStopped()}, false otherwise
	 * @see #getStopped()
	 */
	public boolean isCancelled() {
		return getStopped().contains(this);
	}

	/**
	 * Checks that the status is one of the active states.
	 * @return true if this {@link TransferStatusType} is present in {@link #getActive()}, false otherwise
	 * @see #getActive()
	 */
	public boolean isActive() {
		return getActive().contains(this);
	}
}

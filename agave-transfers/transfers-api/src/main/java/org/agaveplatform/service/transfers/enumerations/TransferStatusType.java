package org.agaveplatform.service.transfers.enumerations;


import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * These represent the states comprising the state machine of a{@link TransferTask}.
 */
public enum TransferStatusType
{
	ASSIGNED, CANCELLED, COMPLETED, COMPLETED_WITH_ERRORS, FAILED, PAUSED, PAUSE_WAITING, QUEUED, RETRYING, TRANSFERRING, ERROR, CREATED, CANCELING_WAITING, CANCELLED_ERROR;

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
		return List.of(ASSIGNED, PAUSED, PAUSE_WAITING, QUEUED, RETRYING, TRANSFERRING, ERROR, CREATED, CANCELING_WAITING);
	}

	/**
	 * Returns {@link TransferStatusType} that represent stopped statuses. These are tasks that were cancelled
	 * or failed due to outside interference.
	 * @return immutable list of the active {@link TransferStatusType}
	 */
	public static List<TransferStatusType> getStopped() {
		return List.of(CANCELLED, FAILED);
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

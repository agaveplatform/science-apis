package org.agaveplatform.service.transfers.enumerations;


import java.util.Arrays;
import java.util.List;

public enum TransferStatusType
{
	CANCELLED, COMPLETED, FAILED, PAUSED, QUEUED, RETRYING, TRANSFERRING;

	public static String supportedValuesAsString()
	{
		return CANCELLED + ", " + COMPLETED + ", " + FAILED + ", " +
				PAUSED + ", " + QUEUED + ", " + RETRYING + ", " + TRANSFERRING;
	}

	public static List<TransferStatusType> getActiveStatusValues()
	{
		return Arrays.asList(PAUSED, QUEUED, RETRYING, TRANSFERRING);
	}

	/**
	 * Checks that the status is cancelled or failed.
	 * @return true if cancelled or failed.
	 */
	public boolean isCancelled()
	{
		return this == CANCELLED || this == FAILED;
	}

	/**
	 * Checks that the status is not in a terminal state
	 * @return true if not in a terminal state
	 */
	public boolean isActive()
	{
		return ! (this == CANCELLED || this == FAILED || this == COMPLETED);
	}
}

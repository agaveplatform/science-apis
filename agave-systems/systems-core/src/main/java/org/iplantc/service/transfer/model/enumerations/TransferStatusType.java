package org.iplantc.service.transfer.model.enumerations;

import java.util.Arrays;
import java.util.List;

public enum TransferStatusType
{
	CANCELLED, COMPLETED, FAILED, PAUSED, QUEUED, RETRYING, TRANSFERRING, ASSIGNED, COMPLETED_WITH_ERRORS, PAUSE_WAITING, ERROR, CREATED, CANCELING_WAITING, CANCELED_ERROR;

	public static String supportedValuesAsString()
	{
		return CANCELLED + ", " + COMPLETED + ", " + FAILED + ", " + 
				PAUSED + ", " + QUEUED + ", " + RETRYING + ", " + TRANSFERRING;
	}
	
	public static List<TransferStatusType> getActiveStatusValues()
	{
		return Arrays.asList(PAUSED, QUEUED, RETRYING, TRANSFERRING);
	}

	public boolean isCancelled()
	{
		return (this.equals(TransferStatusType.CANCELLED) ||
				this.equals(TransferStatusType.FAILED) );
	}
}

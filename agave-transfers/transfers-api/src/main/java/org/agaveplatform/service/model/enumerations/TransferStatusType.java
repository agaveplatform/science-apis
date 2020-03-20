package org.agaveplatform.service.model.enumerations;


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

	public static List<org.agaveplatform.service.model.enumerations.TransferStatusType> getActiveStatusValues()
	{
		return Arrays.asList(PAUSED, QUEUED, RETRYING, TRANSFERRING);
	}

	public boolean isCancelled()
	{
		return (this.equals(org.iplantc.service.transfer.model.enumerations.TransferStatusType.CANCELLED) ||
				this.equals(org.iplantc.service.transfer.model.enumerations.TransferStatusType.FAILED) );
	}
}

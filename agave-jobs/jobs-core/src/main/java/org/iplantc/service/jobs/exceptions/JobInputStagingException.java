package org.iplantc.service.jobs.exceptions;

public class JobInputStagingException extends Exception {

	public JobInputStagingException() {}

	public JobInputStagingException(String message)
	{
		super(message);
	}

	public JobInputStagingException(Throwable cause)
	{
		super(cause);
	}

	public JobInputStagingException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

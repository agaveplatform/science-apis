package org.agaveplatform.service.transfers.exception;

/**
 * Exception indicating the max retry attempts have been reached.
 * @author dooley
 */
@SuppressWarnings("serial")
public class MaxTransferTaskAttemptsExceededException extends Exception {

	/**
	 *
	 */
	public MaxTransferTaskAttemptsExceededException() {}

	/**
	 * @param message
	 */
	public MaxTransferTaskAttemptsExceededException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public MaxTransferTaskAttemptsExceededException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MaxTransferTaskAttemptsExceededException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

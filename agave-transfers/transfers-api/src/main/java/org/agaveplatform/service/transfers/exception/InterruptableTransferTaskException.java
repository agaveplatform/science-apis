package org.agaveplatform.service.transfers.exception;

/**
 * @author ertanner
 *
 */
@SuppressWarnings("serial")
public class InterruptableTransferTaskException extends Throwable {

	/**
	 *
	 */
	public InterruptableTransferTaskException()	{}

	/**
	 * @param message
	 */
	public InterruptableTransferTaskException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public InterruptableTransferTaskException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InterruptableTransferTaskException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

package org.iplantc.service.common.exceptions;

/**
 * @author deshpande
 * 
 */
public class CommonResourceException extends RuntimeException {

	private static final long	serialVersionUID	= -7845904825208032472L;
	
	
	private int status = 200;
	
	/**
	 * 
	 */
	public CommonResourceException()
	{}

	/**
	 * @param arg0
	 */
	public CommonResourceException(int status, String arg0)
	{
		super(arg0);
		this.status = status;
	}

	/**
	 * @param arg0
	 */
	public CommonResourceException(int status, Throwable arg0)
	{
		super(arg0);
		this.status = status;
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public CommonResourceException(int status, String arg0, Throwable arg1)
	{
		super(arg0, arg1);
		this.status = status;
	}
	
	/**
	 * @return the status
	 */
	public int getStatus()
	{
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(int status)
	{
		this.status = status;
	}

}
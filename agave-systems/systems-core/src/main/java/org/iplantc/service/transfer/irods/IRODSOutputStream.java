package org.iplantc.service.transfer.irods;

import org.apache.log4j.Logger;
import org.iplantc.service.transfer.RemoteOutputStream;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.IOException;

public class IRODSOutputStream extends RemoteOutputStream<IRODS> {
	
	private static final Logger log = Logger.getLogger(IRODSOutputStream.class);
	
	protected IRODSOutputStream() {}

	public IRODSOutputStream(IRODS client, String remotePath, boolean passive,
			boolean append) throws IOException, RemoteDataException
	{
		this.client = client;
		this.outFile = remotePath;
		try 
		{
			log.debug(Thread.currentThread().getName() + Thread.currentThread().getId()  + " opening output stream connection for thread");
			this.output = client.getRawOutputStream(remotePath);
		}
		catch (IOException | RemoteDataException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteDataException("Failed to obtain remote output stream for " + remotePath, e);
		}
	}

	@Override
	public void abort()
	{
		try { output.close(); } catch (Exception ignored) {}
		log.debug(Thread.currentThread().getName() + Thread.currentThread().getId()  
				+ " aborting output stream connection for thread");
		// We need to explicity give the user who just created this file 
		// ownership on that file because irods won't do this as that
		// would actually be a reasonable thing to do.
		try { 
			client.setOwnerPermission(client.username, outFile, false); 
		} catch (Exception e) {
			log.error("Failed to set permissions on " + outFile + " after stream was closed", e);
		}
		
		try { client.disconnect(); } catch (Exception ignored) {}
		
	}

	@Override
	public void close() throws IOException
	{	
		abort();
		log.debug(Thread.currentThread().getName() + Thread.currentThread().getId()  
				+ " closing output stream connection for thread");
	}


	@Override
	public void write(byte[] b) throws IOException
	{
		this.output.write(b);
	}

	@Override
	public void write(byte[] b, int from, int length) throws IOException
	{
		this.output.write(b, from, length);
	}

	@Override
	public void write(int b) throws IOException
	{
		this.output.write(b);
	}

	@Override
	public void flush() throws IOException
	{
		this.output.flush();
	}

}
package org.iplantc.service.transfer;

import org.globus.ftp.RestartData;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Interface to establish a random access {@link InputStream} to a path on a {@link RemoteSystem} with a
 * {@link RemoteDataClient}.
 *
 * @param <T> Concrete implementation of {@link RemoteDataClient}
 *
 * @see org.iplantc.service.transfer.ftp.FTPInputStream
 * @see org.iplantc.service.transfer.gridftp.GridFTPInputStream
 * @see org.iplantc.service.transfer.irods.IRODSInputStream
 * @see org.iplantc.service.transfer.irods4.IRODS4InputStream
 * @see org.iplantc.service.transfer.sftp.MaverickSFTPInputStream
 * @see org.iplantc.service.transfer.s3.S3InputStream
 */
public abstract class RemoteInputStream<T> extends InputStream {
	
	protected InputStream		input;

	protected T					client;
	
	protected String			targetFile;

	protected RemoteInputStream(){}

	public RemoteInputStream(T client, String file, boolean passive)
			throws IOException, RemoteDataException
	{
		this(client, file, passive, null);
	}

	public RemoteInputStream(T client, String file, boolean passive,
			RestartData restart) throws IOException, RemoteDataException
	{
		this.client = client;
		this.targetFile = file;
	}
	
	/**
     * Returns the total size of input data.
     *
     * @return -1 if size is unknown.
     */
    public long getSize() {
	return -1;
    }
    
    public int read() throws IOException {
	throw new IOException("Not implemented.");
    }

    /**
     * Aborts transfer. When the underlying {@link StorageProtocolType} involves a stateful protocol and more
	 * is needed to abort a transfer than close a socket, this method gives us an out to both close the socket
	 * and cleanup.
     * <p>
	 * <i>Does nothing by default.</i>
     */
    public void abort() {}
    
    /**
	 * Informs subclasses whether the stream is already wrapped inside a buffered stream. This prevents
	 * double buffering and a large performance hit.
     * 
     * @return true if the stream buffers input, false otherwise.
     */
    public boolean isBuffered()
    {
    	return input instanceof BufferedInputStream;
    }
}

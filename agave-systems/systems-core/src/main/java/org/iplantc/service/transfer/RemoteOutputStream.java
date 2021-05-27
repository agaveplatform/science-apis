/**
 * 
 */
package org.iplantc.service.transfer;

import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface to establish a random access {@link OutputStream} to a path on a {@link RemoteSystem} with a
 * {@link RemoteDataClient}.
 *
 * @param <T> Concrete implementation of {@link RemoteDataClient}
 *
 * @see org.iplantc.service.transfer.ftp.FTPOutputStream
 * @see org.iplantc.service.transfer.gridftp.GridFTPOutputStream
 * @see org.iplantc.service.transfer.irods.IRODSOutputStream
 * @see org.iplantc.service.transfer.irods4.IRODS4OutputStream
 * @see org.iplantc.service.transfer.sftp.MaverickSFTPOutputStream
 * @see org.iplantc.service.transfer.s3.S3OutputStream
 */

public abstract class RemoteOutputStream<T> extends OutputStream {
	
	protected OutputStream		output;

	protected T					client;
	
	protected String			outFile	= "";

	protected RemoteOutputStream() {}
	
	public RemoteOutputStream(T client, String file, boolean passive,
			boolean append) throws IOException, RemoteDataException {
		this.client = client;
		this.outFile = file;
	}
	
	public RemoteOutputStream(T client, String file, boolean passive,
			int type, boolean append) throws IOException, RemoteDataException {
		this.client = client;
		this.outFile = file;
	}

	/**
	 * Aborts transfer. When the underlying {@link StorageProtocolType} involves a stateful protocol and more
	 * is needed to abort a transfer than close a socket, this method gives us an out to both close the socket
	 * and cleanup.
	 * <p>
	 * <i>Does nothing by default.</i>
	 */
    public void abort() {}

	@Override
    public void write(int b) throws IOException {
    	throw new IOException("Not implemented.");
    }

    /** Allow users of subclasses to determine if the
     * stream is already wrapped inside buffered stream.
     * 
     * @return true if the stream buffers output, false otherwise.
     */
    public boolean isBuffered()
    {
    	return output instanceof BufferedOutputStream;
    }
}

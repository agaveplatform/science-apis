package org.iplantc.service.io.queue;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.TransferTaskImpl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

/**
 * Class to encapsulate staging of files directly uploaded to the api to the remote system.
 * Files are first cached on the server, then transfered in a single trigger UploadJob
 * execution. This allows the service to respond immediately without blocking the client
 * for the transfer to the remote system.
 *
 * @author dooley
 *
 */

public class UploadRunnable implements Runnable
{
	private static final Logger log = Logger.getLogger(StagingJob.class);

	protected LogicalFile destinationLogicalFile = null;
	protected File localFile = null;

	protected String originalFileOwner = null;
	protected String uploadUsername = null;

	protected boolean rangeCopyOperation = false;
	protected long rangeStart = 0;

	public UploadRunnable(LogicalFile destinationLogicalFile, File localFile, String sourceFileItemOwner, String uploadUsername) {
		this.destinationLogicalFile = destinationLogicalFile;
		this.localFile = localFile;
		this.originalFileOwner = sourceFileItemOwner;
		this.uploadUsername = uploadUsername;
	}

	/**
	 * Creates and authenticates a {@link RemoteDataClient} for the given logical file.
	 * @param logicalFile the logical file for which to get the remote data client.
	 * @return a pre authenticated {@link RemoteDataClient}
	 * @throws RemoteDataException if unable to establish an authenticated connection
	 */
	protected RemoteDataClient getRemoteDataClient(LogicalFile logicalFile) throws RemoteDataException {
		try {
			RemoteDataClient remoteDataClient = new RemoteDataClientFactory().getInstance(logicalFile.getSystem(), logicalFile.getInternalUsername());
			remoteDataClient.authenticate();

			return remoteDataClient;
		} catch (RemoteCredentialException | RemoteDataException | IOException e) {
			throw new RemoteDataException("Unable to obtain a client to the remote system.", e);
		}
	}

	/**
	 * Returns an instance of {@link Local} referencing the local file system
	 * @return instance of {@link Local} referencing the local file system
	 */
	protected RemoteDataClient getLocalDataClient() {
		return new Local(null, "/", "/");
	}

	/**
	 * Performs the last leg of the file upload operation by copying the cached file on disk to the remote system.
	 */
	public void run()
	{
		RemoteDataClient remoteDataClient = null;
		RemoteDataClient localDataClient = null;

		TransferTaskImpl transferTask = null;
		LogicalFile destinationLogicalFile = getDestinationLogicalFile();
		String destUrl = null;
		String srcUrl = null;
		try {
			String agavePath = destinationLogicalFile.getAgaveRelativePathFromAbsolutePath();

			srcUrl = "file:///" + localFile.getPath();
			destUrl = "agave://" + destinationLogicalFile.getSystem().getSystemId() + "/" + agavePath;

			// get the data clients for this transfer
			localDataClient = getLocalDataClient();
			remoteDataClient = getRemoteDataClient(destinationLogicalFile);

			log.debug("Starting background file upload task for " + localFile.getName() + " to " + destUrl);

			// create a new transfer task for tracking
			transferTask = new TransferTaskImpl(srcUrl, destUrl, uploadUsername, null, null);
			TransferTaskDao.persist(transferTask);

			// create a URLCopy instance to manage the transfer.
			URLCopy urlCopy = getUrlCopy(localDataClient, remoteDataClient);

			// adjust for range vs standard copy
			if (isRangeCopyOperation()) {
				transferTask = urlCopy.copyRange(localFile.getPath(), 0, localFile.length(), agavePath, rangeStart, transferTask);
			} else {
				transferTask = urlCopy.copy(localFile.getPath(), agavePath, transferTask);
			}

			// transfer is complete. throw an upload event saying an upload just happened
			destinationLogicalFile.addContentEvent(new FileEvent(FileEventType.UPLOAD,
					FileEventType.UPLOAD.getDescription(), uploadUsername));

			// update our logical file status and create the proper completed event
			updateLogicalFileStatusAndSwallowException(destinationLogicalFile, StagingTaskStatus.STAGING_COMPLETED,
					"Your scheduled transfer of " + localFile.getName() +
							" completed staging. You can access the raw file on " + destinationLogicalFile.getSystem().getName() + " at " +
							destinationLogicalFile.getPath() + " or via the API at " +
							destinationLogicalFile.getPublicLink() + ".",
					uploadUsername);

			log.info("Completed staging file " + destinationLogicalFile.getAgaveRelativePathFromAbsolutePath() + " for user " + originalFileOwner);

			// set it to
			setDestinationLogicalFile(destinationLogicalFile);
		}
		catch (ClosedByInterruptException e) {
			String msg = String.format("Failed to transfer uploaded file %s to remote destination %s " +
					"due to thread interruption.", localFile.getPath(), transferTask.getDest());

			updateLogicalFileStatusAndSwallowException(destinationLogicalFile,
						StagingTaskStatus.STAGING_FAILED,
						msg + " No further action can be taken to recover. Please attempt the upload again.",
						uploadUsername);

			log.error(msg);
		}
		catch (StaleObjectStateException e) {
			log.debug("Just avoided a file upload race condition from worker");
		}
		catch (RemoteDataException e) {
			String msg = String.format("Failed to transfer uploaded file %s to remote destination %s " +
					"due to an error communicating with the remote system.", localFile.getPath(), destUrl);

			updateLogicalFileStatusAndSwallowException(destinationLogicalFile, StagingTaskStatus.STAGING_FAILED,
					msg + " No further action can be taken to recover. Please attempt the upload again.",
					uploadUsername);

			log.error(msg + e.getMessage());
		}
		catch (Throwable e) {
			String msg = String.format("Failed to transfer uploaded file %s to remote destination %s " +
					"due to an unexpected error.", localFile.getPath(), destUrl);

			updateLogicalFileStatusAndSwallowException(destinationLogicalFile, StagingTaskStatus.STAGING_FAILED,
					msg + " No further action can be taken to recover. Please attempt the upload again.",
					uploadUsername);

			log.error(msg, e);
		}
		finally {
			if (localDataClient != null) localDataClient.disconnect();
			if (remoteDataClient != null) remoteDataClient.disconnect();
			if (localFile != null) {
				FileUtils.deleteQuietly(localFile.getParentFile());
			}
		}
	}

	/**
	 * Mockable method to create a new {@link URLCopy} instance for managing a data transfer
	 * @param localDataClient the local data client
	 * @param remoteDataClient the remote data client
	 * @return a new {@link URLCopy} instance for managing a data transfer between the two data clients
	 */
	protected URLCopy getUrlCopy(RemoteDataClient localDataClient, RemoteDataClient remoteDataClient) {
		return new URLCopy(localDataClient, remoteDataClient);
	}

	/**
	 * Mockable method to udpate a logical file without relying on the static {@link LogicalFileDao#persist(LogicalFile)}
	 * method.
	 * @param logicalFile the logical file to update.
	 * @param status the new status
	 * @param message the event message to be sent. Defaults to {@link StagingTaskStatus#getDescription()}
	 * @param eventOwner the owner of the event causing the status change
	 */
	protected void updateLogicalFileStatusAndSwallowException(LogicalFile logicalFile, StagingTaskStatus status, String message, String eventOwner) {
		if (logicalFile == null) return;

		// use default status description by default
		if (message == null || message.isEmpty()) {
			message = status.getDescription();
		}

		try {
			LogicalFileDao.updateTransferStatus(logicalFile, status.name(), message, eventOwner);
		} catch (Throwable t) {
			log.error(String.format("Failure to update logical file %s status to %s while submitting transfer request: %s",
					logicalFile.getUuid(), status.name(), t.getMessage()));
		}
	}

	public LogicalFile getDestinationLogicalFile() {
		return destinationLogicalFile;
	}

	public void setDestinationLogicalFile(LogicalFile destinationLogicalFile) {
		this.destinationLogicalFile = destinationLogicalFile;
	}

	public File getLocalFile() {
		return localFile;
	}

	public void setLocalFile(File localFile) {
		this.localFile = localFile;
	}

	public String getOriginalFileOwner() {
		return originalFileOwner;
	}

	public void setOriginalFileOwner(String originalFileOwner) {
		this.originalFileOwner = originalFileOwner;
	}

	public String getUploadUsername() {
		return uploadUsername;
	}

	public void setUploadUsername(String uploadUsername) {
		this.uploadUsername = uploadUsername;
	}

	public boolean isRangeCopyOperation() {
		return rangeCopyOperation;
	}

	public void setRangeCopyOperation(boolean rangeCopyOperation) {
		this.rangeCopyOperation = rangeCopyOperation;
	}

	public long getRangeStart() {
		return rangeStart;
	}

	public void setRangeStart(long rangeStart) {
		this.rangeStart = rangeStart;
	}

}
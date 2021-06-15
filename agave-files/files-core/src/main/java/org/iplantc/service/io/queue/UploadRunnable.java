package org.iplantc.service.io.queue;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.manager.LogicalFileManager;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.quartz.SchedulerException;

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
	 * Create File Upload notification event and set proper completed event for destination logical file
	 */
	public void run()
	{
		LogicalFile destinationLogicalFile = getDestinationLogicalFile();
		File localFile = getLocalFile();

		try {
			String strFileUploadWebhookUrl = "";

			log.debug("Subscribing to " + strFileUploadWebhookUrl + " to send file upload events when transfer is completed for " + localFile.getName());

			//Create notification subscribed to webhook that will send out FILE_UPLOAD event
			createSingleNotification(destinationLogicalFile, FileEventType.STAGING_COMPLETED, strFileUploadWebhookUrl, true);

			// update our logical file status and create the proper completed event
			updateLogicalFileStatusAndSwallowException(destinationLogicalFile, StagingTaskStatus.STAGING_COMPLETED,
					"Your scheduled transfer of " + localFile.getName() +
							" completed staging. You can access the raw file on " + destinationLogicalFile.getSystem().getName() + " at " +
							destinationLogicalFile.getPath() + " or via the API at " +
							destinationLogicalFile.getPublicLink() + ".",
					getUploadUsername());

			log.info("Completed staging file " + destinationLogicalFile.getAgaveRelativePathFromAbsolutePath() + " for user " + getOriginalFileOwner());

			// set it to
			setDestinationLogicalFile(destinationLogicalFile);
		}
		catch (StaleObjectStateException e) {
			log.debug("Just avoided a file upload race condition from worker");
		}
		catch (Throwable e) {
			String agavePath = destinationLogicalFile.getAgaveRelativePathFromAbsolutePath();
			String destUrl = "agave://" + destinationLogicalFile.getSystem().getSystemId() + "/" + agavePath;

			String msg = String.format("Failed to transfer uploaded file %s to remote destination %s " +
					"due to an unexpected error.", localFile.getPath(), destUrl);

			updateLogicalFileStatusAndSwallowException(destinationLogicalFile, StagingTaskStatus.STAGING_FAILED,
					msg + " No further action can be taken to recover. Please attempt the upload again.",
					getUploadUsername());

			log.error(msg, e);
		}
		finally {
			if (getLocalFile() != null) {
				FileUtils.deleteQuietly(getLocalFile().getParentFile());
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

	protected Notification createSingleNotification(LogicalFile destLogicalFile, FileEventType eventType, String url, boolean persistent)
			throws NotificationException
	{
		return LogicalFileManager.addNotification(destLogicalFile, eventType, url, persistent, destLogicalFile.getOwner());
	}

}
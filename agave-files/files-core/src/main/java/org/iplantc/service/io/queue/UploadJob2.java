package org.iplantc.service.io.queue;

import java.io.File;
import java.nio.channels.ClosedByInterruptException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.manager.FileEventProcessor;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.TransferTask;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

/**
 * Class to encapsulate staging of files directly uploaded to the api to the remote system.
 * Files are first cached on the server, then transfered in a single trigger UploadJob
 * execution. This allows the service to respond immediately without blocking the client
 * for the transfer to the remote system.
 *
 * @author dooley
 *
 */

public class UploadJob2 implements InterruptableJob
{
	private static final Logger log = Logger.getLogger(StagingJob.class);

	protected RemoteDataClient remoteDataClient = null;
	protected LogicalFile logicalFile = null;
	protected String cachedFile = null;
	protected URLCopy urlCopy = null;

	public UploadJob2() {}

	/* (non-Javadoc)
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException
	{
		try
		{
			JobDataMap dataMap = context.getJobDetail().getJobDataMap();  // Note the difference from the previous example
			System.out.println(dataMap.toString());

			doUpload(dataMap);
		}
		catch(Throwable e) {
			log.error("Failed to process cached file " + context.get("cachedFile") + " while transferring to remote destination.", e);
		}
	}

	/**
	 * Perform the actual upload.
	 *
	 * @param dataMap
	 * @throws JobExecutionException
	 */
	public void doUpload(JobDataMap dataMap) throws JobExecutionException
	{
		RemoteDataClient remoteDataClient = null;
		String owner = null;
		String createdBy = null;
		String cachedFile = null;
		try {
			try {
				Long logicalFileId = dataMap.getLong("logicalFileId");
				cachedFile = dataMap.getString("cachedFile");
				owner = dataMap.getString("owner");
				createdBy = dataMap.getString("createdBy");
				String tenantCode = dataMap.getString("tenantId");

				String uploadSource = dataMap.getString("sourceUrl");
				String uploadDest = dataMap.getString("destUrl");
				boolean isRangeCopyOperation = dataMap.getBooleanFromString("isRangeCopyOperation");
				long rangeIndex = dataMap.getLong("rangeIndex");
				long rangeSize = dataMap.getLong("rangeSize");

				TenancyHelper.setCurrentTenantId(tenantCode);
				TenancyHelper.setCurrentEndUser(createdBy);

				logicalFile = LogicalFileDao.findById(logicalFileId);

				if (logicalFile == null) {
					log.info("Aborting upload task from " + uploadSource + " to " + uploadDest + " because logicalFile has been deleted.");
					return;
				}

				remoteDataClient = logicalFile.getSystem().getRemoteDataClient(logicalFile.getInternalUsername());
				remoteDataClient.authenticate();

				String agavePath = logicalFile.getAgaveRelativePathFromAbsolutePath();

				TransferTask transferTask = new TransferTask(
						uploadSource,
						uploadDest,
						createdBy,
						null,
						null);

				TransferTaskDao.persist(transferTask);

				urlCopy = new URLCopy(new Local(null, "/", "/"), remoteDataClient);

				if (isRangeCopyOperation) {
					transferTask = urlCopy.copyRange(cachedFile, 0, new File(cachedFile).length(), agavePath, rangeIndex, transferTask);
				} else {
					transferTask = urlCopy.copy(cachedFile, agavePath, transferTask);
				}

				if (logicalFile.getOwner() != null && remoteDataClient.isPermissionMirroringRequired()) {
					try {
						remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
						remoteDataClient.setOwnerPermission(owner, logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
					} catch (RemoteDataException e) {
						if (! (e.getCause() instanceof org.irods.jargon.core.exception.InvalidUserException)) {
							throw new PermissionException(e);
						}
					}
				}

				FileEventProcessor eventProcessor = new FileEventProcessor();
				eventProcessor.processContentEvent(logicalFile, new FileEvent(FileEventType.UPLOAD,
						FileEventType.UPLOAD.getDescription(),
						createdBy));

				log.info("Completed staging file " + logicalFile.getAgaveRelativePathFromAbsolutePath() + " for user " + owner);

				// file will be untouched after staging, so just mark as completed
				// update the file task
				logicalFile.setStatus(StagingTaskStatus.STAGING_COMPLETED.name());
				//Agave will treat all files as "raw". The functionality to transform (encode) files has been decommissioned.
				logicalFile.setNativeFormat("raw");

				logicalFile.addContentEvent(new FileEvent(FileEventType.STAGING_COMPLETED,
						"Your scheduled transfer of " + uploadSource +
								" completed staging. You can access the raw file on " + logicalFile.getSystem().getName() + " at " +
								logicalFile.getPath() + " or via the API at " +
								logicalFile.getPublicLink() + ".",
						createdBy));

				LogicalFileDao.persist(logicalFile);
			}
			catch (ClosedByInterruptException e) {
				if (logicalFile != null) {
					LogicalFileDao.updateTransferStatus(logicalFile,
							StagingTaskStatus.STAGING_FAILED.name(),
							"Failed to transfer uploaded file to the remote system. "
									+ "The server was stopped prior to the transfer completing "
									+ "and the file was no longer available. No further action "
									+ "can be taken to recover. Please attempt the upload again.",
							createdBy);
				}

				log.error("Failed to transfer uploaded file "
						+ dataMap.getString("cachedFile") + " to remote destination for "
						+ " when the worker was interrupted.");

				Thread.currentThread().interrupt();

			}
			catch (StaleObjectStateException e) {
				log.debug("Just avoided a file upload race condition from worker");
			}
			catch (RemoteDataException e) {
				if (logicalFile != null) {
					LogicalFileDao.updateTransferStatus(logicalFile, StagingTaskStatus.STAGING_FAILED, createdBy);
					try {
						if (logicalFile.getOwner() != null && remoteDataClient.isPermissionMirroringRequired()) {
							remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
							remoteDataClient.setOwnerPermission(owner, logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
						}
					} catch (Exception e1) {}
				}
				log.error("File upload worker failed to transfer the cached file " +
						dataMap.getString("cachedFile") + " to remote destination.", e);
			}
			catch (Throwable e) {
				if (logicalFile != null) {
					LogicalFileDao.updateTransferStatus(logicalFile, StagingTaskStatus.STAGING_FAILED, createdBy);

					try {
						if (logicalFile.getOwner() != null && remoteDataClient.isPermissionMirroringRequired()) {
							remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
							remoteDataClient.setOwnerPermission(owner, logicalFile.getAgaveRelativePathFromAbsolutePath(), true);
						}
					} catch (Exception e1) {}
				}
				log.error("File upload worker failed unexpectedly while attempting to transfer cached file " +
						dataMap.getString("cachedFile") + " to remote destination.", e);
			}
		} catch (Throwable e) {
			log.error("File upload worker failed unexpectedly while attempting to transfer cached "
					+ "file to remote destination.", e);
		}
		finally {
			try { remoteDataClient.disconnect(); } catch (Exception e) {}
			if (StringUtils.isNotEmpty(cachedFile)) {
				FileUtils.deleteQuietly(new File(cachedFile).getParentFile());
			}
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		try {
			if (logicalFile != null) {
				LogicalFileDao.updateTransferStatus(logicalFile, StagingTaskStatus.STAGING_FAILED, logicalFile.getOwner());
			}
		}
		catch (Exception e) {
			log.error("Failed to updated logical file status to STAGING_FAILED due to "
					+ "worker interrupt during upload file staging.", e);
		}
		finally {
			Thread.currentThread().interrupt();
		}

	}
}
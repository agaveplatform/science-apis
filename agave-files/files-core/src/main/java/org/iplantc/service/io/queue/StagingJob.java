package org.iplantc.service.io.queue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.dao.QueueTaskDao;
import org.iplantc.service.io.exceptions.FileEventProcessingException;
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.exceptions.TaskException;
import org.iplantc.service.io.manager.FileEventProcessor;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.io.util.ServiceUtils;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.systems.util.ApiUriUtil;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.InvalidTransferException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemotePermissionException;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

/**
 * Handles the staging of data into the user's StorageSystem. This differs
 * from a transfer task in that the flow is always inward to the StorageSystem
 * so only one RemoteSystem need be defined.
 *
 * @author dooley
 */
//@DisallowConcurrentExecution
public class StagingJob extends AbstractJobWatch<StagingTask> {
    private static final Logger log = Logger.getLogger(StagingJob.class);

    private TransferTaskImpl rootTask = null;
    private URLCopy urlCopy;
    private LogicalFile file = null;

    public StagingJob() {
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.io.queue.WorkerWatch#doExecute()
     */
    @Override
    public void doExecute() throws JobExecutionException {
        RemoteDataClient destClient = null;
        RemoteDataClient sourceClient = null;

        try {
            this.queueTask.setStatus(StagingTaskStatus.STAGING);

            QueueTaskDao.persist(this.queueTask);

            // Tracing.
            if (log.isDebugEnabled()) {
                String msg = "Changed status of task " + queueTask.getId() +
                        " to " + queueTask.getStatusAsString() + ".";
                log.debug(msg);
            }

            file = this.queueTask.getLogicalFile();

            URI sourceUri = new URI(file.getSourceUri());

            // instantiate a client appropriate for the source uri
            destClient = ServiceUtils.getDestinationRemoteDataClient(file);

            // fetch a client for the source
            sourceClient = getSourceRemoteDataClient(this.queueTask.getOwner(), sourceUri);

            String destUri = "agave://" + file.getSystem().getSystemId() + "/" + file.getAgaveRelativePathFromAbsolutePath();

			rootTask = new TransferTaskImpl(
                    file.getSourceUri(),
                    destUri,
                    file.getOwner(),
                    null,
                    null);
            TransferTaskDao.persist(rootTask);

            // Tracing.
            if (log.isDebugEnabled()) {
                String msg = "Associated new transfer task with queue task " + queueTask.getId() +
                        ": " + rootTask.toString();
                log.debug(msg);
            }

            file.addContentEvent(new FileEvent(
                    FileEventType.STAGING,
                    "Transfer in progress",
                    queueTask.getOwner(),
                    rootTask));
            file.setStatus(StagingTaskStatus.STAGING);

            if (log.isDebugEnabled())
                log.debug("Attempt " + this.queueTask.getRetryCount() +
                        " to stage file " + file.getAgaveRelativePathFromAbsolutePath() +
                        " (" + file.getUuid() +
                        ") for user " + file.getOwner());

            LogicalFileDao.persist(file);

            urlCopy = new URLCopy(sourceClient, destClient);
            // will close connections on its own

            try {
                String src;
                if (ApiUriUtil.isInternalURI(sourceUri)) {
                    // this strips double paths and lends itself to unhandled decoding
                    src = ApiUriUtil.getPath(sourceUri);
                    // decode the path for use in the RemoteDataClient instances and
                    // resolve the relative vs absolute path issue
                    src = UrlPathEscaper.decode(StringUtils.removeStart(src, "/"));
                    // if double slashes were there, ensure it starts with a slash
                    if (StringUtils.startsWith(sourceUri.getPath(), "//")) src = "/" + src;

                } else {
                    src = sourceUri.getRawPath();
                }

                if (sourceUri.getRawQuery() != null) {
                    src += "?" + sourceUri.getRawQuery();
                }

                // Note that the copy command may create a target subdirectory.  This new directory
                // was anticipated by the logical file processing code in {@link FileManagementResource#adjustDestinationPath}.
                // This linkage is a hidden dependency between the two classes.
                try {
                    String resolvedDestPath = file.getAgaveRelativePathFromAbsolutePath();
                    rootTask = urlCopy.copy(src, resolvedDestPath, rootTask);

                    // check to see if the destination path was adjusted due to a streaming transfer receiving an
                    // existing directory as the destPath. if so, the new root TransferTask#dest value will be longer,
                    // so we need to update the logical file accordingly.
                    if (!StringUtils.equals(destUri, rootTask.getDest())) {
                        String appendedPath = StringUtils.replace(rootTask.getDest(), destUri, "");
                        file.setPath(file.getPath() + appendedPath);
                    }
                } catch (RemotePermissionException|InvalidTransferException e) {
                    // hard fail the transfer task if it cannot be retried
                    this.queueTask.setRetryCount(Settings.MAX_STAGING_RETRIES);
                    throw e;
                } catch (Exception e) {
                    String msg = "Copy from \"" + src + "\" to \"" +
                            file.getAgaveRelativePathFromAbsolutePath() +
                            "\" FAILED for transfer task: " + rootTask.toString();
                    log.error(msg, e);
                    throw e;
                }

                log.info("Completed staging file " + this.queueTask.getLogicalFile().getAgaveRelativePathFromAbsolutePath() + " for user " + file.getOwner());

                // update the staging task as done
                this.queueTask.setStatus(StagingTaskStatus.STAGING_COMPLETED);
                try {
                    QueueTaskDao.persist(this.queueTask);
                } catch (Exception e) {
                    String msg = "Unable to persist status change for queue task " + this.queueTask.getId() + ".";
                    log.error(msg, e);
                    throw e;
                }

                // file will be untouched after staging, so just mark as completed
                // update the file task
                file.setStatus(StagingTaskStatus.STAGING_COMPLETED.name());
                //Agave will treat all files as "raw". The functionality to transform (encode) files has been decommissioned.
                file.setNativeFormat("raw");

                file.addContentEvent(new FileEvent(FileEventType.STAGING_COMPLETED,
                        "Your scheduled transfer of " + this.queueTask.getLogicalFile().getSourceUri() +
                                " completed staging. You can access the raw file on " + file.getSystem().getName() + " at " +
                                this.queueTask.getLogicalFile().getPath() + " or via the API at " +
                                file.getPublicLink() + ".",
                        queueTask.getOwner()));

                try {
                    LogicalFileDao.persist(file);
                } catch (Exception e) {
                    String msg = "Unable to persist multiple status and other changes for logical file " +
                            file.getAgaveRelativePathFromAbsolutePath() +
                            " (" + file.getUuid() + ").";
                    log.error(msg, e);
                    throw e;
                }
            } catch (RemotePermissionException|InvalidTransferException e ) {
                String message = "Unable to complete transfer of " + this.queueTask.getLogicalFile().getSourceUri() +
                        " to agave://" + this.queueTask.getLogicalFile().getSystem().getSystemId() + "//" +
                        this.queueTask.getLogicalFile().getPath() + ". " + e.getMessage();

                log.error(message, e);

                file.setStatus(StagingTaskStatus.STAGING_FAILED);
                file.addContentEvent(new FileEvent(FileEventType.STAGING_FAILED,
                        "Your scheduled transfer of " + this.queueTask.getLogicalFile().getSourceUri() +
                                " failed with the following error message: " + e.getMessage() +
                                ". Please check the source url, " + sourceUri + " and make sure it is a " +
                                " valid URI or path on your default system to which you have permissions. " +
                                "If you feel there was an error and this problem persists, please " +
                                "contact your platform administrator for assistance.",
                        queueTask.getOwner()));

                try {
                    LogicalFileDao.persist(file);
                } catch (Exception e1) {
                    String msg = "Unable to persist status and changes for logical file " +
                            file.getAgaveRelativePathFromAbsolutePath() +
                            " (" + file.getUuid() + ").";
                    log.error(msg, e1);
                    throw e1;
                }

                this.queueTask.setStatus(StagingTaskStatus.STAGING_FAILED);

                try {
                    QueueTaskDao.persist(this.queueTask);
                } catch (Exception e1) {
                    String msg = "During retry processing, unable to persist queue task " + this.queueTask.getId() + ".";
                    log.error(msg, e);
                    throw e1;
                }
            } catch (Throwable e) {

                // if the transfer failed, retry as many times as defined in
                // the service config file
                if (this.queueTask.getRetryCount() < Settings.MAX_STAGING_RETRIES) {
                    log.info("Failed attempt " + this.queueTask.getRetryCount() + " to stage file " + this.queueTask.getLogicalFile().getPath() + " for user " + file.getOwner());
                    this.queueTask.setStatus(StagingTaskStatus.STAGING_QUEUED);
                    this.queueTask.setRetryCount(this.queueTask.getRetryCount() + 1);
                } else {
                    String message = "Failed attempt " + this.queueTask.getRetryCount() + " to stage file " + this.queueTask.getLogicalFile().getPath()
                            + " for user " + file.getOwner() + ". The maximum number of retries has been reached.";

                    log.error(message, e);

                    file.setStatus(StagingTaskStatus.STAGING_FAILED);
                    file.addContentEvent(new FileEvent(FileEventType.STAGING_FAILED,
                            "Your scheduled transfer of " + this.queueTask.getLogicalFile().getSourceUri() +
                                    " failed after " + Settings.MAX_STAGING_RETRIES +
                                    " attempts with the following error message: " + e.getMessage() +
                                    ". Please check the source url, " + sourceUri + " and make sure it is a " +
                                    " valid URI or path on your default system to which you have permissions. " +
                                    "If you feel there was an error and this problem persists, please " +
                                    "contact your platform administrator for assistance.",
                            queueTask.getOwner()));

                    try {
                        LogicalFileDao.persist(file);
                    } catch (Exception e1) {
                        String msg = "After exhausting retries, unable to persist status and changes for logical file " +
                                file.getAgaveRelativePathFromAbsolutePath() +
                                " (" + file.getUuid() + ").";
                        log.error(msg, e1);
                        throw e1;
                    }

                    this.queueTask.setStatus(StagingTaskStatus.STAGING_FAILED);
                }

                try {
                    QueueTaskDao.persist(this.queueTask);
                } catch (Exception e1) {
                    String msg = "During retry processing, unable to persist queue task " + this.queueTask.getId() + ".";
                    log.error(msg, e);
                    throw e1;
                }
            }
        } catch (SystemUnknownException e) {
            String message = "Unsupported protocol for queued file " + this.queueTask.getLogicalFile().getPath();

            log.error(message, e);
            file.setStatus(StagingTaskStatus.STAGING_FAILED);
            file.addContentEvent(new FileEvent(FileEventType.STAGING_FAILED,
                    "Your scheduled transfer of " + this.queueTask.getLogicalFile().getSourceUri() +
                            " failed to because an unrecognized protocol " +
                            " was provided. Please check the source url, " + this.queueTask.getLogicalFile().getPath() +
                            " and make sure it is a valid URI or path on your default system" +
                            " to which you have permissions. If you feel there was an error and this " +
                            "problem persists, please contact your platform administrator for assistance.",
                    queueTask.getOwner()));
            try {
                LogicalFileDao.persist(file);
            } catch (Exception e1) {
                String msg = "While processing SystemUnknownException, unable to persist status and changes for logical file " +
                        file.getAgaveRelativePathFromAbsolutePath() +
                        " (" + file.getUuid() + ").";
                log.error(msg, e1);
                throw e1;
            }

            this.queueTask.setStatus(StagingTaskStatus.STAGING_FAILED);
            try {
                QueueTaskDao.persist(this.queueTask);
            } catch (Exception e1) {
                String msg = "While processing SystemUnknownException, unable to persist queue task " + this.queueTask.getId() + ".";
                log.error(msg, e);
                throw e1;
            }

        } catch (SystemUnavailableException e) {
            log.info("Staging task paused while waiting for system availability. " + e.getMessage(), e);
            file.setStatus(StagingTaskStatus.STAGING_QUEUED.name());
            try {
                LogicalFileDao.persist(file);
            } catch (Exception e1) {
                String msg = "While processing SystemUnavailableException, unable to persist status and changes for logical file " +
                        file.getAgaveRelativePathFromAbsolutePath() +
                        " (" + file.getUuid() + ").";
                log.error(msg, e1);
                throw e1;
            }

            this.queueTask.setStatus(StagingTaskStatus.STAGING_QUEUED);
            this.queueTask.setLastUpdated(new Date());
            try {
                QueueTaskDao.persist(this.queueTask);
            } catch (Exception e1) {
                String msg = "While processing SystemUnavailableException, unable to persist queue task " + this.queueTask.getId() + ".";
                log.error(msg, e);
                throw e1;
            }

        } catch (StaleObjectStateException | UnresolvableObjectException e) {
            // What nonsense.
            log.warn("Just avoided a file staging race condition from worker", e);
        } catch (Throwable e) {
            // Who knows where this came from.
            log.error(e.getClass().getSimpleName() + " caught", e);

            try {
                if (this.queueTask != null) {
                    LogicalFile file = this.queueTask.getLogicalFile();
                    String message = "Failed to submit file " + file.getPath() + " to the transform queue for owner " + file.getOwner();
                    log.info(message);

                    if (this.queueTask.getRetryCount() < Settings.MAX_STAGING_RETRIES) {
                        try {
                            file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
                            LogicalFileDao.persist(file);
                        } catch (Throwable t) {
                            log.error("Failed to update status of logical file " + file.getUuid()
                                    + " to STAGING_FAILED after error with staging task " + this.queueTask.getId(), t);
                        }

                        try {
                            FileEvent event = new FileEvent(FileEventType.STAGING_FAILED,
                                    "Your scheduled transfer of " + file.getSourceUri() +
                                            " failed to transfer on attempt " + (this.queueTask.getRetryCount() + 1) +
                                            " with the following error message: " + e.getMessage() +
                                            ". This transfer will be attempted " + (Settings.MAX_STAGING_RETRIES - this.queueTask.getRetryCount()) +
                                            " more times before being being abandonded.",
                                    queueTask.getOwner());
                            FileEventProcessor.processAndSaveContentEvent(file, event);
                        } catch (LogicalFileException | FileEventProcessingException e1) {
                            log.error("Failed to send notification of failed staging task " + this.queueTask.getId(), e1);
                        }

                        // increment the retry counter and throw it back into queue
                        this.queueTask.setRetryCount(this.queueTask.getRetryCount() + 1);
                        this.queueTask.setStatus(StagingTaskStatus.STAGING_QUEUED);
                        try {
                            QueueTaskDao.persist(this.queueTask);
                        } catch (Exception e1) {
                            String msg = "While processing " + e.getClass().getSimpleName() +
                                    ", unable to persist next attempt for queue task " + this.queueTask.getId() + ".";
                            log.error(msg, e1);
                            throw e1;
                        }
                    } else {
                        try {
                            file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
                            LogicalFileDao.persist(file);
                        } catch (Throwable t) {
                            log.error("Failed to update status of logical file " + file.getUuid()
                                    + " to STAGING_FAILED after error with staging task " + this.queueTask.getId(), t);
                        }

                        try {
                            FileEvent event = new FileEvent(FileEventType.STAGING_FAILED,
                                    "Your scheduled transfer of " + file.getSourceUri() +
                                            " failed after " + Settings.MAX_STAGING_RETRIES + " attempts with the following message: " +
                                            e.getMessage() + ". If you feel there was an error and this problem persists, please " +
                                            "contact your platform administrator for assistance.",
                                    queueTask.getOwner());
                            FileEventProcessor.processAndSaveContentEvent(file, event);
                        } catch (LogicalFileException | FileEventProcessingException e1) {
                            log.error("Failed to send notification of failed staging task " + this.queueTask.getId(), e1);
                        }

                        this.queueTask.setStatus(StagingTaskStatus.STAGING_FAILED);
                        try {
                            QueueTaskDao.persist(this.queueTask);
                        } catch (Exception e1) {
                            String msg = "While processing " + e.getClass().getSimpleName() +
                                    ", unable to persist exhausted attempt for queue task " + this.queueTask.getId() + ".";
                            log.error(msg, e1);
                            throw e1;
                        }
                    }
                } else {
                    log.error("Failed to submit unknown file", e);
                }
            } catch (Throwable t) {
                log.error("Failed to roll back failed staging task", e);
            }

            try {
                if (rootTask != null) {
                    if (rootTask.getRootTask() != null) {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getRootTask().getId());
                    } else if (rootTask.getParentTask() != null) {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getParentTask().getId());
                    } else {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getId());
                    }
                }
            } catch (Throwable t) {
                log.error("Task cancellation failed.", t);
            }
        } finally {
            try {
                sourceClient.disconnect();
            } catch (Throwable ignored) {
            }
            try {
                destClient.disconnect();
            } catch (Throwable ignored) {
            }
            setTaskComplete(true);
            releaseJob();
        }
    }

    /**
     * Validates a URI and returns an authenticated {@link RemoteDataClient}. Exceptions
     * will be thrown rather than null returned if a system was not available.
     *
     * @param requestingUser user requesting the data transfer
     * @param singleRawInputUri the URI of a file item for which a remote data client will be returned
     * @return
     * @throws AgaveNamespaceException
     * @throws AuthenticationException
     * @throws SystemUnavailableException
     * @throws SystemUnknownException
     * @throws RemoteDataException
     */
    private RemoteDataClient getSourceRemoteDataClient(String requestingUser, URI singleRawInputUri)
            throws AgaveNamespaceException, AuthenticationException, SystemUnavailableException, SystemUnknownException,
            RemoteDataException {
        RemoteSystem system = null;
        RemoteDataClient remoteDataClient = null;

        if (ApiUriUtil.isInternalURI(singleRawInputUri)) {
            try {
                system = ApiUriUtil.getRemoteSystem(requestingUser, singleRawInputUri);
            } catch (PermissionException e) {
                String msg = "Unable to get remote system for user \"" + requestingUser + "\": " + singleRawInputUri.toString();
                log.error(msg, e);
                throw new AuthenticationException(msg, e);
            }

            if (system == null) {
                String msg = "No system was found for user " + requestingUser + " satisfying the source URI.";
                log.error(msg);
                throw new SystemUnknownException(msg);
            } else if (!system.isAvailable()) {
                String msg = "The source system is currently unavailable.";
                log.error(msg);
                throw new SystemUnavailableException(msg);
            } else if (system.getStatus() != SystemStatusType.UP) {
                String msg = system.getStatus().getExpression();
                log.error(msg);
                throw new SystemUnavailableException(msg);
            } else {
                try {
                    remoteDataClient = system.getRemoteDataClient(null);
                    remoteDataClient.authenticate();
                } catch (IOException e) {
                    String msg = "Failed to connect to the remote source system";
                    log.error(msg, e);
                    throw new RemoteDataException(msg, e);
                } catch (RemoteCredentialException e) {
                    String msg = "Failed to authenticate to remote source system ";
                    log.error(msg, e);
                    throw new AuthenticationException("Failed to authenticate to remote source system ", e);
                } catch (Exception e) {
                    String msg = "System-acquired remote client authentication error: " + e.getMessage();
                    log.error(msg, e);
                    throw e;
                }
            }
        } else {
            try {
                remoteDataClient = new RemoteDataClientFactory().getInstance(
                        requestingUser, null, singleRawInputUri);

                if (remoteDataClient == null) {
                    String msg = "No system was found for user " + requestingUser + " satisfying the source URI: " + singleRawInputUri.toString();
                    log.error(msg);
                    throw new SystemUnknownException(msg);
                } else {
                    remoteDataClient.authenticate();
                }
            } catch (SystemUnknownException e) {
                String msg = "Authentication failed for user " + remoteDataClient.getUsername() +
                        " on host " + remoteDataClient.getHost() + " because of unknown system.";
                log.error(msg, e);
                throw e;
            } catch (FileNotFoundException e) {
                String msg = "No source system was found for user " + requestingUser + " satisfying the URI.";
                log.error(msg, e);
                throw new SystemUnknownException(msg);
            } catch (IOException e) {
                String msg = "Failed to connect to the remote source system";
                log.error(msg, e);
                throw new RemoteDataException(msg, e);
            } catch (PermissionException | RemoteCredentialException e) {
                String msg = "Failed to authenticate to remote source system. ";
                log.error(msg, e);
                throw new AuthenticationException(msg, e);
            } catch (Exception e) {
                String msg = "Remote client authentication error: " + singleRawInputUri.toString();
                log.error(msg, e);
                throw e;
            }
        }

        return remoteDataClient;
    }

    /**
     * @return the rootTask
     */
    public synchronized TransferTaskImpl getRootTask() {
        return rootTask;
    }

    /**
     * @param rootTask the rootTask to set
     */
    public synchronized void setRootTask(TransferTaskImpl rootTask) {
        this.rootTask = rootTask;
    }

    /**
     * @param stopped the stopped to set
     * @throws UnableToInterruptJobException
     */
    @Override
    public synchronized void setStopped(boolean stopped)
            throws UnableToInterruptJobException {
        if (getUrlCopy() != null) {
            getUrlCopy().setKilled(true);
        }

        super.setStopped(true);
    }

    /**
     * @return the urlCopy
     */
    public synchronized URLCopy getUrlCopy() {
        return urlCopy;
    }

    /**
     * @param urlCopy the urlCopy to set
     */
    public synchronized void setUrlCopy(URLCopy urlCopy) {
        this.urlCopy = urlCopy;
    }


    @Override
    public void interrupt() throws UnableToInterruptJobException {
        if (getQueueTask() != null) {
            try {
                this.queueTask = (StagingTask) QueueTaskDao.merge(getQueueTask());
                this.queueTask.setStatus(StagingTaskStatus.STAGING_QUEUED);
                QueueTaskDao.persist(this.queueTask);
            } catch (Throwable e) {
                log.error("Failed to roll back transfer task during interrupt.");
            }

            try {
                LogicalFile file = getQueueTask().getLogicalFile();
                file.setStatus(StagingTaskStatus.STAGING_FAILED.name());
                file.addContentEvent(new FileEvent(FileEventType.STAGING_FAILED,
                        "Transfer was interrupted by the worker thread.",
                        queueTask.getOwner()));
                LogicalFileDao.persist(file);
            } catch (Throwable e) {
                log.error("Failed to roll back transfer task during interrupt.");
            }

            if (getRootTask() != null) {
                try {
                    rootTask = TransferTaskDao.merge(getRootTask());

                    if (rootTask.getRootTask() != null) {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getRootTask().getId());
                    } else if (rootTask.getParentTask() != null) {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getParentTask().getId());
                    } else {
                        TransferTaskDao.cancelAllRelatedTransfers(rootTask.getId());
                    }
                } catch (Exception e1) {
                    log.error("Task cancellation failed.", e1);
                }
            }
        }

        releaseJob();
    }

    @Override
    public synchronized Long selectNextAvailableQueueTask() throws TaskException {
        return QueueTaskDao.getNextStagingTask(Settings.getQueuetaskTenantIds());
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.WorkerWatch#getJob()
     */
    @Override
    public synchronized StagingTask getQueueTask() {
        if (this.queueTask == null && queueTaskId != null) {
            this.queueTask = QueueTaskDao.getStagingTaskById(this.queueTaskId);
        }
        if (log.isTraceEnabled() && (queueTask == null))
            log.trace("Null staging task returned for queueTaskId " + queueTaskId + ".");
        return this.queueTask;
    }

    @Override
    protected void rollbackStatus() {
        //
    }

    @Override
    public synchronized void setQueueTaskId(Long queueTaskId) {
        this.queueTaskId = queueTaskId;
    }

    @Override
    protected void releaseJob() {
        JobProducerFactory.releaseStagingJob(this.queueTaskId);
    }
}
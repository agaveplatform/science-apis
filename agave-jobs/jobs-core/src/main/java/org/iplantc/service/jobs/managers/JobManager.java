/**
 *
 */
package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.StaleStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.exceptions.UnknownSoftwareException;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobDependencyException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.exceptions.JobTerminationException;
import org.iplantc.service.jobs.managers.killers.JobKiller;
import org.iplantc.service.jobs.managers.killers.JobKillerFactory;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobEventType;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.queue.ZombieJobWatch;
import org.iplantc.service.jobs.queue.factory.AbstractJobProducerFactory;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.joda.time.DateTime;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * @author dooley
 *
 */
public class JobManager {
    private static final Logger log = Logger.getLogger(JobManager.class);

    /**
     * Returns the {@link ExecutionSystem} for the given {@code job}.
     * @param job the job for which to return the {@link ExecutionSystem}
     * @return a valid {@link ExecutionSystem} or null of it no longer exists.
     * @throws SystemUnavailableException if the system is no longer available
     */
    public ExecutionSystem getJobExecutionSystem(Job job) throws SystemUnavailableException {
        RemoteSystem jobExecutionSystem = new SystemDao().findBySystemId(job.getSystem());
        if (jobExecutionSystem == null) {
            throw new SystemUnavailableException("Job execution system " + job.getSystem() +
                    " is not currently available");
        } else {
            return (ExecutionSystem) jobExecutionSystem;
        }
    }

    /**
     * Returns the {@link Software} for the given {@code job}.
     * @param job the job for which the software object will be returned
     * @return a valid {@link Software} object or null of it no longer exists.
     */
    public Software getJobSoftware(Job job) {
        return SoftwareDao.getSoftwareByUniqueName(job.getSoftwareName());
    }

    /**
     * Removes the job work directory in the event staging fails too many times.
     *
     * @param job the job whose staged data will be deleted
     * @throws JobException if unable to update the job or delete the data
     * @throws SystemUnavailableException if the job's {@link ExecutionSystem} is not available
     */
    public static Job deleteStagedData(Job job) throws JobException, SystemUnavailableException {
        ExecutionSystem system = new JobManager().getJobExecutionSystem(job);

        log.debug("Cleaning up staging directory for failed job " + job.getUuid());
        job = JobManager.updateStatus(job, JobStatusType.STAGING_INPUTS, "Cleaning up remote work directory.");

        ExecutionSystem remoteExecutionSystem = null;
        RemoteDataClient remoteExecutionDataClient = null;
        String remoteWorkPath = null;
        try {
            // copy to remote execution work directory
            remoteExecutionSystem = (ExecutionSystem) new SystemDao().findBySystemId(job.getSystem());
            remoteExecutionDataClient = remoteExecutionSystem.getRemoteDataClient(job.getInternalUsername());
            remoteExecutionDataClient.authenticate();

            Software software = SoftwareDao.getSoftwareByUniqueName(job.getSoftwareName());

            if (!StringUtils.isEmpty(software.getExecutionSystem().getScratchDir())) {
                remoteWorkPath = software.getExecutionSystem().getScratchDir();
            } else if (!StringUtils.isEmpty(software.getExecutionSystem().getWorkDir())) {
                remoteWorkPath = software.getExecutionSystem().getWorkDir();
            }

            if (!StringUtils.isEmpty(remoteWorkPath)) {
                if (!remoteWorkPath.endsWith("/")) remoteWorkPath += "/";
            } else {
                remoteWorkPath = "";
            }

            remoteWorkPath += job.getOwner() +
                    "/job-" + job.getUuid() + "-" + Slug.toSlug(job.getName());

            if (remoteExecutionDataClient.doesExist(remoteWorkPath)) {
                remoteExecutionDataClient.delete(remoteWorkPath);
                log.debug("Successfully deleted remote work directory " + remoteWorkPath + " for failed job " + job.getUuid());
                job = JobManager.updateStatus(job, JobStatusType.STAGING_INPUTS, "Completed cleaning up remote work directory.");
            } else {
                log.debug("Skipping deleting remote work directory " + remoteWorkPath + " for failed job " + job.getUuid() + ". Directory not present.");
                job = JobManager.updateStatus(job, JobStatusType.STAGING_INPUTS, "Completed cleaning up remote work directory.");
            }

            return job;
        } catch (RemoteDataException | IOException | RemoteCredentialException e) {
            throw new JobException(e.getMessage(), e);
        } finally {
            try {
                if (remoteExecutionDataClient != null) remoteExecutionDataClient.disconnect();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Kills a running job by updating its status and using the remote scheduler command and local id to stop it forcefully.
     *
     * @param job the job to kill
     * @throws JobTerminationException if the termination attempt failed
     * @throws JobException if unable to update the job record
     */
    public static Job kill(Job job) throws JobTerminationException, JobException {
        if (!JobStatusType.hasQueued(job.getStatus()) || job.getStatus() == JobStatusType.ARCHIVING) {
            // if it's not in queue, just update the status
//			JobDao.refresh(job);
            job = JobManager.updateStatus(job, JobStatusType.STOPPED, "Job cancelled by user.");

            for (JobEvent event : job.getEvents()) {
                if (event.getTransferTask() != null) {
                    if (event.getTransferTask().getStatus() == TransferStatusType.PAUSED ||
                            event.getTransferTask().getStatus() == TransferStatusType.QUEUED ||
                            event.getTransferTask().getStatus() == TransferStatusType.RETRYING ||
                            event.getTransferTask().getStatus() == TransferStatusType.TRANSFERRING) {
                        try {
                            TransferTaskDao.cancelAllRelatedTransfers(event.getTransferTask().getId());
                        } catch (Exception e) {
                            log.error("Failed to cancel transfer task " +
                                    event.getTransferTask().getUuid() + " while stopping job " +
                                    job.getUuid(), e);
                        }
                    }
                }
            }

            return job;
        } else if (!job.isRunning()) {
            // nothing to be done for jobs that are not running
            return job;
        } else {
            JobKiller killer = null;

            int retries = 0;
            while (retries < Settings.MAX_SUBMISSION_RETRIES) {
                try {
                    log.debug("Attempt " + (retries + 1) + " to kill job " + job.getUuid() +
                            " and clean up assets");

                    killer = new JobKillerFactory().getInstance(job);
                    killer.attack();

                    log.debug("Successfully killed remaining processes of job " + job.getUuid());

                    job = JobManager.updateStatus(job, JobStatusType.FAILED,
                            "Successfully killed remote job process.");

                    return job;
                } catch (SystemUnavailableException e) {

                    String message = "Failed to kill job " + job.getUuid()
                            + " identified by id " + job.getLocalJobId() + " on " + job.getSystem()
                            + ". The system is currently unavailable.";

                    log.debug(message);

                    job = JobManager.updateStatus(job, job.getStatus(), "Failed to kill job "
                            + " identified by id " + job.getLocalJobId() + " on " + job.getSystem()
                            + " Response from " + job.getSystem() + ": " + e.getMessage());

                    throw new JobTerminationException(message, e);
                } catch (RemoteExecutionException e) {

                    job = killer.getJob();

                    String message = "Failed to kill job " + job.getUuid()
                            + " identified by id " + job.getLocalJobId() + " on " + job.getSystem()
                            + " Response from " + job.getSystem() + ": " + e.getMessage();

                    log.debug(message);

                    job = JobManager.updateStatus(job, JobStatusType.FAILED, "Failed to kill job "
                            + " identified by id " + job.getLocalJobId() + " on " + job.getSystem()
                            + " Response from " + job.getSystem() + ": " + e.getMessage());

                    throw new JobTerminationException(message, e);
                } catch (JobTerminationException e) {

                    retries++;

                    job = killer.getJob();

                    String message = "Failed to kill job " + job.getUuid() +
                            " on attempt " + retries + ". Response from " + job.getSystem() + ": " + e.getMessage();

                    log.debug(message);

                    job = JobManager.updateStatus(job, job.getStatus(), message);

                    if (retries == Settings.MAX_SUBMISSION_RETRIES) {

                        message = "Failed to kill job " + job.getUuid() +
                                " after " + retries + "  attempts. Terminating job.";

                        log.debug(message);

                        job = JobManager.updateStatus(job, JobStatusType.FAILED, message);

                        return job;
                    }
                }

            }

            // Occasionally the status check will have run or the job will actually complete
            // prior to this being called. That will invalidate the current object. Here we
            // refresh with job prior to updating the status so we don't get a stale state
            // exception
//			JobDao.refresh(job);
            job = JobManager.updateStatus(job, JobStatusType.STOPPED);
//			job.setStatus(JobStatusType.STOPPED,  JobStatusType.STOPPED.getDescription());
//			job.setLastUpdated(new DateTime().toDate());
//			job.setEndTime(job.getLastUpdated());
//			JobDao.persist(job);

            for (JobEvent event : job.getEvents()) {
                if (event.getTransferTask() != null) {
                    if (event.getTransferTask().getStatus() == TransferStatusType.PAUSED ||
                            event.getTransferTask().getStatus() == TransferStatusType.QUEUED ||
                            event.getTransferTask().getStatus() == TransferStatusType.RETRYING ||
                            event.getTransferTask().getStatus() == TransferStatusType.TRANSFERRING) {
                        try {
                            TransferTaskDao.cancelAllRelatedTransfers(event.getTransferTask().getId());
                        } catch (Exception e) {
                            log.error("Failed to cancel transfer task " +
                                    event.getTransferTask().getUuid() + " while stopping job " +
                                    job.getUuid(), e);
                        }
                    }
                }
            }

            return job;
        }
    }

    /**
     * Sets {@link Job#setVisible(Boolean)} to true and updates the timestamp. A {@link JobEvent} with status
     * {@link JobEventType#RESTORED} is created.
     *
     * @param jobId the db id of the job to restore.
     * @param invokingUsername the username of the user requesting the job to be restored
     * @throws JobException if unable to update the job record
     */
    public static Job restore(long jobId, String invokingUsername) throws JobTerminationException, JobException {
        Job job = null;

        try {
            job = JobDao.getById(jobId);

            if (job == null) {
                throw new JobException("Unable to restore job. If this persists, please contact your tenant administrator.");
            } else if (!job.isVisible()) {
                try {
                    job.setVisible(Boolean.TRUE);

                    job.addEvent(new JobEvent(
                            JobEventType.RESTORED.name(),
                            "Job was restored by " + invokingUsername,
                            invokingUsername));

                    JobDao.persist(job);

                    return job;
                } catch (Throwable e) {
                    throw new JobException("Failed to restore job " + job.getUuid() + ".", e);
                }
            } else {
                throw new JobException("Job is already visible.");
            }
        } catch (UnresolvableObjectException e) {
            throw new JobException("Unable to restore job. If this persists, please contact your tenant administrator.", e);
        }
    }

    /**
     * Sets the job's visibility attribute to false and
     * updates the timestamp. A {@link JobEventType#DELETED} event
     * is thrown.
     *
     * @param jobId jhe db id of the job to hide
	 * @param invokingUsername username of the user making the request
     * @throws JobException if unable to hide the job
     */
    public static Job hide(long jobId, String invokingUsername) throws JobTerminationException, JobException {
        // Get the job and process it even if it's already hidden
        // so that we get another opportunity to kill it.
        Job job = JobDao.getById(jobId);

		if (job == null) {
			throw new JobException("Unable to restore job. If this persists, please contact your tenant administrator.");
		}

        // If the job is running, try to stop it and cancel all queued work.
        // Remember, there are built-in race conditions with polling quartz
        // job, so we can never be sure when the job state changes from
        // underneath us.
        if (job.isRunning())
            try {
                kill(job);
            } catch (Exception e) {
                String msg = "Unable to kill job " + job.getUuid() + ". Aborting hide operation.";
                log.error(msg, e);
                throw new JobException(msg, e);
            }

        // We assume the job is in some state that won't cause a polling quartz job
        // to start processing it because (1) the above kill operation put it into
        // such as state, or (2) the kill operation threw an exception and we
        // never get here, or (3) the job was already in such a state when this method
        // was called.
        try {
            // Update the job fields even if the job is already hidden.
            job.setVisible(Boolean.FALSE);
            Date jobHiddenDate = new DateTime().toDate();
            job.setLastUpdated(jobHiddenDate);

            job.addEvent(new JobEvent(
                    JobEventType.DELETED.name(),
                    "Job was deleted by user " + invokingUsername + ".",
                    invokingUsername));

            JobDao.persist(job);
        } catch (Throwable e) {
            String msg = "Failed to hide job " + job.getUuid() + ".";
            log.error(msg, e);
            throw new JobException(msg, e);
        }

        return job;
    }

    /**
     * Updates the status of a job, updates the timestamps as appropriate based on the status, and writes a new
     * {@link JobEvent} with the default {@link JobStatusType#getDescription()} to the job's history.
     *
     * @param job the job to update
     * @param status the status to assign to the job
     * @return Updated job object
     * @throws JobException if the update was unable to complete
     * @throws StaleStateException when a concurrency issue prevents the job update
     */
    public static Job updateStatus(Job job, JobStatusType status)
            throws JobException {
        return updateStatus(job, status, status.getDescription());
    }

    /**
     * Updates the status of a job, its timestamps, and writes a new
     * JobEvent to the job's history with the given status and message.
     *
     * @param job the job to update
     * @param status the status to assign to the job
     * @param eventMessage the message to use in the resulting {@link JobEvent}
     * @return Updated job object
     * @throws JobException if the update was unable to complete
     * @throws StaleStateException when a concurrency issue prevents the job update
     */
    public static Job updateStatus(Job job, JobStatusType status, String eventMessage)
            throws JobException {
        job.setStatus(status, eventMessage);

        Date date = new DateTime().toDate();
        job.setLastUpdated(date);
        if (status.equals(JobStatusType.QUEUED)) {
            if (job.getSubmitTime() == null) {
                job.setSubmitTime(new DateTime().toDate());
            }
        } else if (status.equals(JobStatusType.RUNNING)) {
            if (job.getStartTime() == null) {
                job.setStartTime(new DateTime().toDate());
            }
        } else if (status.equals(JobStatusType.FINISHED)
                || status.equals(JobStatusType.KILLED)
                || status.equals(JobStatusType.STOPPED)
                || status.equals(JobStatusType.FAILED)) {
            if (job.getEndTime() == null) {
                job.setEndTime(new DateTime().toDate());
            }
        } else if (status.equals(JobStatusType.STAGED)) {
            // nothing to do here?
        }

        JobDao.persist(job, false);

        return job;
    }

    /**
     * This method attempts to archive a job's output by retrieving the .agave.archive shadow file from the
     * {@link ExecutionSystem} in the remote job directory and staging everything listed in that file not in there to
     * the user-supplied {@link Job#getArchivePath()} on the {@link Job#getArchiveSystem()}.
     *
     * @param job the job to archive
     * @throws SystemUnavailableException if the system is not available to fetch the data to/from
     * @throws SystemUnknownException if the execution or archive system has been deleted
     * @throws JobException if the job cannot be updated or archiving fails
     */
    public static void archive(Job job)
            throws SystemUnavailableException, SystemUnknownException, JobException {
        // flag to ignore deleting of archiving folder in the event of a race condition
        boolean skipCleanup = false;

        ExecutionSystem executionSystem = (ExecutionSystem) new SystemDao().findBySystemId(job.getSystem());

        if (executionSystem == null || !executionSystem.isAvailable() || !executionSystem.getStatus().equals(SystemStatusType.UP)) {
            throw new SystemUnavailableException("Job execution system " + job.getSystem() + " is not available.");
        }

        log.debug("Beginning archive inputs for job " + job.getUuid() + " " + job.getName());
//		JobManager.updateStatus(job, JobStatusType.ARCHIVING);

        RemoteDataClient archiveDataClient = null;
        RemoteDataClient executionDataClient = null;
        RemoteSystem remoteArchiveSystem = null;

        // we should be able to archive from anywhere. Given that we can stage in condor
        // job data from remote systems, we should be able to stage it out as well. At
        // this point we are guaranteed that the worker running this bit of code has
        // access to the job output folder. The RemoteDataClient abstraction will handle
        // the rest.
        File archiveFile = null;
        try {
            try {
                executionDataClient = executionSystem.getRemoteDataClient(job.getInternalUsername());
                executionDataClient.authenticate();
            } catch (Exception e) {
                throw new JobException("Failed to authenticate to the execution system "
                        + executionSystem.getSystemId());
            }

            // copy remote archive file to temp space
            String remoteArchiveFile = job.getWorkPath() + File.separator + ".agave.archive";

            String localArchiveFile = FileUtils.getTempDirectoryPath() + File.separator +
                    "job-" + job.getUuid() + "-" + System.currentTimeMillis();

            // pull remote .archive file and parse it for a list of paths relative
            // to the job.workDir to exclude from archiving. Generally this will be
            // the application binaries, but the app itself may have added or removed
            // things from this file, so we need to process it anyway.
            List<String> jobFileList = new ArrayList<String>();
            try {
                if (executionDataClient.doesExist(remoteArchiveFile)) {
                    executionDataClient.get(remoteArchiveFile, localArchiveFile);

                    // read it in to find the original job files
                    archiveFile = new File(localArchiveFile);
                    if (archiveFile.exists()) {
                        if (archiveFile.isFile()) {
                            jobFileList.addAll(FileUtils.readLines(archiveFile));
                        } else {
                            archiveFile = new File(localArchiveFile, ".agave.archive");
                            if (archiveFile.exists() && archiveFile.isFile()) {
                                jobFileList.addAll(FileUtils.readLines(archiveFile));
                            }
                        }
                    }
                } else {
                    log.debug("No archive file found for job " + job.getUuid() + " on system " +
                            executionSystem.getSystemId() + " at " + remoteArchiveFile +
                            ". Entire job directory will be archived.");
                    JobManager.updateStatus(job, JobStatusType.ARCHIVING,
                            "No archive file found. Entire job directory will be archived.");
                }
            } catch (Exception e) {
                log.debug("Unable to parse archive file for job " + job.getUuid() + " on system " +
                        executionSystem.getSystemId() + " at " + remoteArchiveFile +
                        ". Entire job directory will be archived.");
                JobManager.updateStatus(job, JobStatusType.ARCHIVING,
                        "Unable to parse job archive file. Entire job directory will be archived.");
            }

            remoteArchiveSystem = job.getArchiveSystem();

            if (remoteArchiveSystem == null) {
                throw new SystemUnknownException("Unable to archive job output. No archive system could be found.");
            } else if (!remoteArchiveSystem.isAvailable() || !remoteArchiveSystem.getStatus().equals(SystemStatusType.UP)) {
                throw new SystemUnavailableException("Unable to archive job output from system " +
                        remoteArchiveSystem.getSystemId() + ". The system is currently unavailable.");
            } else {
                try {
                    archiveDataClient = remoteArchiveSystem.getRemoteDataClient(job.getInternalUsername());
                    archiveDataClient.authenticate();
                } catch (Exception e) {
                    throw new JobException("Failed to authenticate to the archive system "
                            + remoteArchiveSystem.getSystemId(), e);
                }
            }

            try {
                if (!archiveDataClient.doesExist(job.getArchivePath())) {
                    archiveDataClient.mkdirs(job.getArchivePath());
                    if (archiveDataClient.isPermissionMirroringRequired() && StringUtils.isEmpty(job.getInternalUsername())) {
                        archiveDataClient.setOwnerPermission(job.getOwner(), job.getArchivePath(), true);
                    }
                }
            } catch (Exception e) {
                throw new JobException("Failed to create archive directory "
                        + job.getArchivePath() + " on " + remoteArchiveSystem.getSystemId(), e);
            }

            // read in remote job work directory listing
            List<RemoteFileInfo> outputFiles = null;
            try {
                outputFiles = executionDataClient.ls(job.getWorkPath());
            } catch (Exception e) {
                throw new JobException("Failed to retrieve directory listing of "
                        + job.getWorkPath() + " from " + executionSystem.getSystemId(), e);
            }

            // iterate over the work folder and archive everything that wasn't
            // listed in the archive file. We use URL copy here to abstract the
            // third party transfer we would like to do. If possible, URLCopy will
            // do a 3rd party transfer. When not possible, such as when we're going
            // cross-protocol, it will proxy the transfer.
            TransferTaskImpl rootTask = new TransferTaskImpl(
                    "agave://" + job.getSystem() + "/" + job.getWorkPath(),
                    "agave://" + job.getArchiveSystem().getSystemId() + "/" + job.getArchivePath(),
                    job.getOwner(),
                    null,
                    null);
            TransferTaskDao.persist(rootTask);

            job.addEvent(new JobEvent(
                    job.getStatus(),
                    "Archiving " + rootTask.getSource() + " to " + rootTask.getDest(),
                    rootTask,
                    job.getOwner()));

            JobDao.persist(job);

            for (RemoteFileInfo outputFile : outputFiles) {
                JobDao.refresh(job);

                if (job.getStatus() != JobStatusType.ARCHIVING) break;

                if (StringUtils.equals(outputFile.getName(), ".") || StringUtils.equals(outputFile.getName(), ".."))
                    continue;

                String workFileName = job.getWorkPath() + File.separator + outputFile.getName();
                String archiveFileName = job.getArchivePath() + File.separator + outputFile.getName();
                if (!jobFileList.contains(outputFile.getName())) {
                    final URLCopy urlCopy = new URLCopy(executionDataClient, archiveDataClient);
                    TransferTaskImpl childTransferTask = new TransferTaskImpl(
                            "agave://" + job.getSystem() + "/" + workFileName,
                            "agave://" + job.getArchiveSystem().getSystemId() + "/" + archiveFileName,
                            job.getOwner(),
                            rootTask,
                            rootTask);
                    try {
                        TransferTaskDao.persist(childTransferTask);
                        urlCopy.copy(workFileName, archiveFileName, childTransferTask);
                        rootTask.updateSummaryStats(childTransferTask);
                        TransferTaskDao.persist(rootTask);
                    } catch (TransferException e) {
                        throw new JobException("Failed to archive file " + workFileName +
                                " to " + childTransferTask.getDest() +
                                " due to an error persisting the transfer record.", e);
                    } catch (Exception e) {
                        throw new JobException("Failed to archive file " + workFileName +
                                " to " + childTransferTask.getDest() +
                                " due to an error during transfer ", e);
                    }
                }
            }

            try {
                if (job.getStatus() == JobStatusType.ARCHIVING) {
                    rootTask.setStatus(TransferStatusType.COMPLETED);
                } else {
                    rootTask.setStatus(TransferStatusType.FAILED);
                }

                rootTask.setEndTime(Instant.now());

                TransferTaskDao.persist(rootTask);
            } catch (Exception ignored) {
            }

            // if it all worked as expected, then delete the job work directory
            try {
//			    if (!skipCleanup) {
                executionDataClient.delete(job.getWorkPath());
                JobManager.updateStatus(job, JobStatusType.ARCHIVING_FINISHED,
                        "Job archiving completed successfully.");
//    			}
            } catch (Exception e) {
                log.error("Archiving of job " + job.getUuid() + " completed, "
                        + "but an error occurred deleting the remote work directory "
                        + job.getUuid(), e);
            }
        } catch (StaleObjectStateException e) {
            skipCleanup = true;
            log.error(e);
            throw e;
        } catch (SystemUnavailableException | JobException | SystemUnknownException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException("Failed to archive data due to internal failure.", e);
        } finally {
            // clean up the local archive file
            FileUtils.deleteQuietly(archiveFile);
            try {
                if (archiveDataClient.isPermissionMirroringRequired() && StringUtils.isEmpty(job.getInternalUsername())) {
                    archiveDataClient.setOwnerPermission(job.getOwner(), job.getArchivePath(), true);
                }
            } catch (Exception ignored) {
            }
            try {
                archiveDataClient.disconnect();
            } catch (Exception ignored) {
            }
            try {
                executionDataClient.disconnect();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Takes an existing {@link Job} and sanitizes it for resubmission. During this process
     * the {@link Job#getArchivePath()}, {@link Job#getArchiveSystem()}, etc. will be updated. In the
     * event that {@link SoftwareParameter} or {@link SoftwareInput} are no longer valid, the
     * job will fail to validate. This is a good thing as it ensures reproducibility. In
     * situations where reproducibility is not critical, the {@code ignoreInputConflicts} and
     * {@code ignoreParameterConflicts} flags can be set to true to update any hidden field
     * defaults or inject them if not previously present.
     *
     * @param originalJob the job to resubmit
     * @param newJobOwner the owner of the new job
     * @param newJobInternalUsername the internal username of the new job
     * @param ignoreInputConflicts if true, ignore hidden input conflicts and update accordingly
     * @param ignoreParameterConflicts if true, ignore hidden parameter conflicts and update accordingly
     * @return a validated {@link Job} representing the resubmitted job with a unique id.
     * @throws JobProcessingException
     * @throws JsonProcessingException
     * @throws IOException
     */
    public static Job resubmitJob(Job originalJob, String newJobOwner, String newJobInternalUsername,
                                  boolean ignoreInputConflicts, boolean ignoreParameterConflicts)
            throws JobProcessingException, JsonProcessingException, IOException {
        boolean preserveNotifications = false;

        JobRequestProcessor processor =
                new JobResubmissionRequestProcessor(newJobOwner,
                        newJobInternalUsername,
                        ignoreInputConflicts,
                        ignoreParameterConflicts,
                        preserveNotifications);

        JsonNode originalJobJson = new ObjectMapper().readTree(originalJob.toJSON());

        return processor.processJob(originalJobJson);
    }

    /**
     * Takes a JsonNode representing a job request and parses it into a job object.
     *
     * @param json a JsonNode containing the job request
     * @return validated job object ready for submission
     * @throws JobProcessingException when the job request is not valid
     */
    public static Job processJob(JsonNode json, String username, String internalUsername)
            throws JobProcessingException {
        JobRequestProcessor processor = new JobRequestProcessor(username, internalUsername);
        return processor.processJob(json);
    }

    /**
     * Takes a Form representing a job request and parses it into a job object. This is a
     * stripped down, unstructured version of the other processJob method.
     *
     * @param jobRequestMap a Map containing the job request as key value pairs
     * @return validated job object ready for submission
     * @throws JobProcessingException if the job request is not valid
     */
    public static Job processJob(Map<String, Object> jobRequestMap, String username, String internalUsername)
            throws JobProcessingException {
        JobRequestProcessor processor = new JobRequestProcessor(username, internalUsername);
        return processor.processJob(jobRequestMap);
    }

    /**
     * Finds queue on the given executionSystem that supports the given number of nodes and
     * memory per node given.
     *
     * @param executionSystem the {@link ExecutionSystem} for which to search for matching queues.
     * @param nodes a positive integer value or -1 for no limit
     * @param memory memory in GB or -1 for no limit
     * @param requestedTime time in hh:mm:ss format
     * @return a BatchQueue matching the given parameters or null if no match can be found
     */
    public static BatchQueue selectQueue(ExecutionSystem executionSystem, Long nodes, Double memory, String requestedTime) {
        return selectQueue(executionSystem, nodes, memory, (long) -1, requestedTime);
    }

    /**
     * Finds queue on the given executionSystem that supports the given number of nodes and
     * memory per node given.
     *
     * @param nodes a positive integer value or -1 for no limit
     * @param processors positive integer value or -1 for no limit
     * @param memory memory in GB or -1 for no limit
     * @param requestedTime time in hh:mm:ss format
     * @return a BatchQueue matching the given parameters or null if no match can be found
     */
    public static BatchQueue selectQueue(ExecutionSystem executionSystem, Long nodes, Double memory, Long processors, String requestedTime) {
        if (validateBatchSubmitParameters(executionSystem.getDefaultQueue(), nodes, processors, memory, requestedTime)) {
            return executionSystem.getDefaultQueue();
        } else {
            BatchQueue[] queues = executionSystem.getBatchQueues().toArray(new BatchQueue[]{});
            Arrays.sort(queues);
            for (BatchQueue queue : queues) {
                if (queue.isSystemDefault())
                    continue;
                else if (validateBatchSubmitParameters(queue, nodes, processors, memory, requestedTime))
                    return queue;
            }
        }

        return null;
    }


    /**
     * Validates that the queue supports the number of nodes, processors per node, memory and
     * requestedTime provided. If any of these values are null or the given values exceed the queue
     * limits, it returns false.
     *
     * @param queue the BatchQueue to check against
     * @param nodes a positive integer value or -1 for no limit
     * @param processors positive integer value or -1 for no limit
     * @param memory memory in GB or -1 for no limit
     * @param requestedTime time in hh:mm:ss format
     * @return true if all the values are non-null and within the limits of the queue
     */
    public static boolean validateBatchSubmitParameters(BatchQueue queue, Long nodes, Long processors, Double memory, String requestedTime) {
        if (queue == null ||
                nodes == null || nodes == 0 || nodes < -1 ||
                processors == null || processors == 0 || processors < -1 ||
                memory == null || memory == 0 || memory < -1 ||
                StringUtils.isEmpty(requestedTime) || StringUtils.equals("00:00:00", requestedTime)) {
            return false;
        }

        if (queue.getMaxNodes() > 0 && queue.getMaxNodes() < nodes) {
            return false;
        }

        if (queue.getMaxProcessorsPerNode() > 0 && queue.getMaxProcessorsPerNode() < processors) {
            return false;
        }

        if (queue.getMaxMemoryPerNode() > 0 && queue.getMaxMemoryPerNode() < memory) {
            return false;
        }

        if (queue.getMaxRequestedTime() != null &&
                TimeUtils.compareRequestedJobTimes(queue.getMaxRequestedTime(), requestedTime) == -1) {
            return false;
        }

        return true;
    }

    /**
     * Returns a map of all inputs needed to run the job comprised of the user-supplied
     * inputs as well as the default values for hidden and unspecified, but required inputs.
     * This is needed during staging and job submission because the original job submission
     * may not contain all the inputs actually needed to run the job depending on whether
     * or not there are hidden fields in the app description.
     *
     * @param job the job from which to get the input map
     * @return a {@link Map} of the inputs needed to run this job along with default values
     * @throws JobException if unable to resolve the inputs
     */
    public static Map<String, String[]> getJobInputMap(Job job) throws JobException {
        try {
            Map<String, String[]> map = new HashMap<String, String[]>();

            JsonNode jobInputJson = job.getInputsAsJsonObject();
            Software software = SoftwareDao.getSoftwareByUniqueName(job.getSoftwareName());
            if (software != null) {
                for (SoftwareInput input : software.getInputs()) {
                    if (jobInputJson.has(input.getKey())) {
                        JsonNode inputJson = jobInputJson.get(input.getKey());
                        String[] inputValues = null;
                        if (inputJson == null || inputJson.isNull() || (inputJson.isArray() && inputJson.size() == 0)) {
                            // no inputs, don't even include it in the map
                            continue;
                        } else if (inputJson.isArray()) {
                            // should be an array of
                            inputValues = ServiceUtils.getStringValuesFromJsonArray((ArrayNode) inputJson, false);
                        } else {
                            inputValues = new String[]{inputJson.textValue()};
                        }

                        map.put(input.getKey(), inputValues);
                    } else if (!input.isVisible()) {
                        String[] inputValues = ServiceUtils.getStringValuesFromJsonArray(input.getDefaultValueAsJsonArray(), false);
                        map.put(input.getKey(), inputValues);
                    }
                }

                return map;
            } else {
                throw new UnknownSoftwareException("No app found for job " + job.getUuid() + " with id " + job.getSoftwareName());
            }
            // TODO: throw UnknownSoftwareException for better insight upstream? wouldn't it already be caught here?
        } catch (Throwable e) {
            throw new JobException("Unable to parse job and app inputs", e);
        }
    }

    /**
     * Determines whether the job has completed archiving and can thus refer to the archive location for requests
     * for its output data.
     *
     * @param job the job to query about archving status
     * @return true if all job output was archived, false otherwise
     */
    public static boolean isJobDataFullyArchived(Job job) {
        if (job.isArchiveOutput()) {
            if (job.getStatus() == JobStatusType.ARCHIVING_FINISHED) {
                return true;
            } else if (job.getStatus() == JobStatusType.FINISHED) {
                for (JobEvent event : job.getEvents()) {
                    if (StringUtils.equalsIgnoreCase(event.getStatus(), JobStatusType.ARCHIVING_FINISHED.name())) {
                        return true;
                    }
                }
            }
        }

        // anything else means the job failed, hasn't reached a point of
        // archiving, is in process, or something happened.
        return false;
    }

    /**
     * Rolls a {@link Job} back to the previously active state based on its current {@link JobStatusType}.
     *
     * @param job the job to roll back to previous {@link JobStatusType}
     * @param requestedBy the principal requesting the job be reset
     * @throws JobException if unable to update the job
     */
    public static Job resetToPreviousState(Job job, String requestedBy)
            throws JobException, JobDependencyException {
        if (job == null) {
            throw new JobException("Job cannot be null");
        }

        ZombieJobWatch zombieJob = new ZombieJobWatch();
        Job updatedJob = null;
        try {
            updatedJob = zombieJob.rollbackJob(job, requestedBy);

            JobEvent event = new JobEvent("RESET", "Job was manually reset to " +
                    updatedJob.getStatus().name() + " by " + requestedBy, requestedBy);
            event.setJob(updatedJob);
            updatedJob.addEvent(event);

            JobDao.persist(updatedJob);

            releaseLocalJobLock(updatedJob);

            return updatedJob;
        } catch (JobException e) {
            throw new JobException("Failed to reset job to previous state.", e);
        } catch (JobDependencyException e) {
            throw new JobException("Unable to reset inactive jobs to previous state.");
        }
    }

    /**
     * Looks for the job in the currently active quartz jobs for each job phase
     * and interrupts the job if present.
     *
     * @param lockedJob the job for which to release the lock
     * @return true if the job was interrupted, false otherwise;
     */
    public static boolean releaseLocalJobLock(Job lockedJob) {
        StdSchedulerFactory factory = new org.quartz.impl.StdSchedulerFactory();
        try {
            // loop through all schedulers looking for one processing the current job
            for (Scheduler scheduler : factory.getAllSchedulers()) {
                List<JobExecutionContext> currentJobs = scheduler.getCurrentlyExecutingJobs();
                if (currentJobs != null) {
                    for (JobExecutionContext context : currentJobs) {
                        if (context.getMergedJobDataMap().containsKey("uuid") &&
                                StringUtils.equals(lockedJob.getUuid(), context.getMergedJobDataMap().getString("uuid"))) {
                            try {
                                log.debug("Found worker " + context.getJobDetail().getKey().getName() +
                                        " in the " + scheduler.getSchedulerName() + " processing job " +
                                        lockedJob.getUuid() + ". Attempting to interrupt job.");
                                scheduler.interrupt(context.getJobDetail().getKey());
                                log.debug("Interrupted the worker currently processing job " + lockedJob.getUuid());
                            } catch (Exception e) {
                                log.error("Failed to interrupt the worker running job " + lockedJob.getUuid(), e);
                            }

                            return true;
                        }
                    }
                }
            }

            log.debug("No worker found processing job " + lockedJob.getUuid() + ". Releasing local lock on the job.");

        } catch (SchedulerException e) {
            log.error("Failed to query container quartz schedulers for job prior to rollback. " +
                    "No existing worker currently processing job " + lockedJob.getUuid() +
                    " will be interrupted. This may lead to temporary concurrency conflicts. " +
                    "If in doubt, restart this container.", e);
        } finally {
            log.debug("Releasing local lock on job " + lockedJob.getUuid() + " after interrupt.");
            AbstractJobProducerFactory.releaseJob(lockedJob.getUuid());
        }

        return false;
    }
}

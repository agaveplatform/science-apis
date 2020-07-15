/**
 * 
 */
package org.iplantc.service.jobs.managers.monitors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.launchers.AbstractJobLauncher;
import org.iplantc.service.jobs.managers.launchers.StartupScriptJobMacroResolver;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.scripts.SubmitScript;
import org.iplantc.service.jobs.model.scripts.SubmitScriptFactory;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class to handle the common actions needed for a {@link JobMonitor} to properly function. Most
 * concrete {@link JobMonitor} classes should extend this class unless there is a good reason to not to do so.
 *
 * @see DefaultJobMonitor
 * @see CondorJobMonitor
 *
 * @author dooley
 *
 */
public abstract class AbstractJobMonitor implements JobMonitor {

    private static final Logger log = Logger.getLogger(AbstractJobMonitor.class);
    
    private AtomicBoolean stopped = new AtomicBoolean(false);
    
    protected Job job;

    protected JobManager jobManager = null;

	public AbstractJobMonitor(Job job) {
		this.job = job;
	}
	
	/* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#isStopped()
     */
    @Override
    public synchronized boolean isStopped() {
        return stopped.get();
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#setStopped(boolean)
     */
    @Override
    public synchronized void setStopped(boolean stopped) {
        this.stopped.set(stopped);
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.monitors.JobMonitor#getJob()
     */
    @Override
    public synchronized Job getJob() {
        return this.job;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#checkStopped()
     */
    @Override
    public void checkStopped() throws ClosedByInterruptException {
        if (isStopped()) {
            throw new ClosedByInterruptException();
        }
    }

    /**
     * Basic getter for job manager instance. Useful for testing
     * @return JobManager instance
     */
    protected JobManager getJobManager() {
        if (jobManager == null) {
            jobManager = new JobManager();
        }

        return jobManager;
    }

    /* (non-Javadoc)
	 * @see org.iplantc.service.jobs.managers.monitors.JobMonitor#getRemoteSubmissionClient()
	 */
	@Override
	public RemoteSubmissionClient getRemoteSubmissionClient() throws SystemUnavailableException, AuthenticationException
    {
		ExecutionSystem system = getExecutionSystem();
		return system.getRemoteSubmissionClient(getJob().getInternalUsername());
	}

	public ExecutionSystem getExecutionSystem() throws SystemUnavailableException
	{
		return getJobManager().getJobExecutionSystem(getJob());
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.managers.monitors.JobMonitor#getRemoteDataClient()
	 */
	@Override
	public RemoteDataClient getAuthenticatedRemoteDataClient() throws RemoteDataException, IOException, AuthenticationException, SystemUnavailableException, RemoteCredentialException
	{
		RemoteDataClient remoteDataClient = getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername());
        remoteDataClient.authenticate();
        return remoteDataClient;
	}

	/**
     * Queries job directory to get the most recently modified log file and returns that date.
     * <strong>note:</strong> this is being deprecated in favor of querying the scheduler for
     * individual job info.
     *
     * @return returns the {@link Date} of the most recently modified log file for the job.
     * @deprecated
     */
    protected Date fetchEndDateFromLogFiles()
    {
        RemoteDataClient remoteDataClient = null;
        RemoteFileInfo stdOutFileInfo;
        RemoteFileInfo stdErrFileInfo;
        Date errDate = null;
        Date outDate = null;
        Date logDate = new DateTime().toDate();
        try
        {   log.debug("Attempting to fetch completion time for job " + job.getUuid() + " from logfile timestamps");
            remoteDataClient = getAuthenticatedRemoteDataClient();

            // get the output filenames from the SubmitScript for the job.
            SubmitScript script = SubmitScriptFactory.getScript(job);
            String stdOut = job.getWorkPath() + "/" + script.getStandardOutputFile();
            String stdErr = job.getWorkPath() + "/" + script.getStandardErrorFile();

            if (remoteDataClient.doesExist(stdErr)) {
                stdErrFileInfo = remoteDataClient.getFileInfo(stdErr);
                errDate = stdErrFileInfo.getLastModified();
            }

            if (remoteDataClient.doesExist(stdOut)) {
                stdOutFileInfo = remoteDataClient.getFileInfo(stdOut);
                outDate = stdOutFileInfo.getLastModified();
            }
        } catch (Throwable e) {
            log.error("Failed to retrieve completion timestamp for job " + job.getUuid() +
                    " from logfile timestamps.", e);
        }
        finally {
        	try {
                if (remoteDataClient != null) {
                    remoteDataClient.disconnect();
                }
            } catch (Exception ignored) {}
        	remoteDataClient = null;
        }

        if (errDate != null && outDate != null) {
            if (errDate.compareTo(outDate) >= 0) {
                logDate = errDate;
            } else {
                logDate = outDate;
            }
        } else if (errDate != null) {
            logDate = errDate;
        } else if (outDate != null) {
            logDate = outDate;
        }

        if (job.getStartTime() != null && logDate.after(job.getStartTime())) {
            return logDate;
        } else {
            return new DateTime().toDate();
        }
    }
    
    /**
     * Returns the command to run on the remote host prior to the wrapper script is run so that the
     * {@link ExecutionSystem#getStartupScript()} is sourced prior to job submission. The
     * {@link ExecutionSystem#getStartupScript()} can be templatized with job macros, so this method will
     * resolve those prior to building the command.
     *
     * Unlike the comparable {@link AbstractJobLauncher#getStartupScriptCommand(String)} method, if no startup
     * script is defined, an empty string is returned. This is to avoid blowing up the log file for long-running jobs.
     * We also write the output to /dev/null rather than the job's {@code .agave.log}  file for the same reason.
     *
	 * @return the resolved command, or empty if the {@link ExecutionSystem#getStartupScript()} is not defined.
     * @throws JobMacroResolutionException when the startup script cannot be resolved. This is usually due ot the system not being available
     * @see StartupScriptJobMacroResolver
	 */
	public String getStartupScriptCommand() throws JobMacroResolutionException {
		String resolvedstartupScript = new StartupScriptJobMacroResolver(getJob()).resolve();

		return resolvedstartupScript == null ? "" :
                String.format("echo $(source %s 2>&1) >> /dev/null ; ", resolvedstartupScript);
	}

    /**
     * Filters the scheduler specific job query command replacing the {@code ${AGAVE_JOB_LOCALJOB_ID}} macro with the
     * recorded {@link Job#getLocalJobId()} for the job under question.
     * @return teh filtered job query command for the scheduler associated with the job.
     * @throws SystemUnavailableException if the remote system is unavailable
     */
	public String getBatchQueryCommand() throws SystemUnavailableException {
	    return StringUtils.replace("${AGAVE_JOB_LOCALJOB_ID}",
                getJob().getLocalJobId(),
                getExecutionSystem().getScheduler().getBatchQueryCommand());
    }

    /**
     * Updates the job record in the db and locally with the given status and logs any error that happens.
     *
     * @param status the new {@link JobStatusType}
     * @param errorMessage the event message to include in the job update event raised after this
     * @throws JobException if unable to persist the job status
     */
    protected void updateJobStatus(JobStatusType status, String errorMessage) throws JobException {
        // ignore if the job is null
        if (getJob() == null) return;

        try {
            // update the job and the reference object here
            this.job = getJobManager().updateStatus(this.job, status, errorMessage);
        } catch(Throwable e) {
            throw new JobException("Failed to updated job " + this.job.getUuid() + " status to " + this.job.getStatus(), e);
        }
    }

    /**
     * Forwards the job status through {@link JobStatusType#CLEANING_UP} and, if archiving is disabled,
     * {@link JobStatusType#FINISHED}. This is called when the monitor detects the remote process is
     * no longer running.
     * @throws JobException if unable to persist the job status
     */
    protected void updateStatusOfFinishedJob() throws JobException {
        Date logDate = new DateTime().toDate();
        this.job.setEndTime(logDate);
        updateJobStatus(JobStatusType.CLEANING_UP, "Job completion detected by job monitor.");

        if (!this.job.isArchiveOutput()) {
            log.debug("Job " + this.job.getUuid() + " will skip archiving at user request.");
            updateJobStatus(JobStatusType.FINISHED, "Job completed. Skipping archiving at user request.");
            log.debug("Job " + this.job.getUuid() + " finished.");
        }
    }
}

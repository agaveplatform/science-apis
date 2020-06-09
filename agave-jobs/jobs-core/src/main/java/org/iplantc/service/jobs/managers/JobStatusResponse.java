package org.iplantc.service.jobs.managers;

import org.iplantc.service.jobs.managers.monitors.parsers.JobStatusResponseParser;
import org.iplantc.service.jobs.managers.monitors.parsers.RemoteSchedulerJobStatus;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

/**
 * Structure contains structured information about a job running on a remote system. Monitoring apps can use
 * the {@link #getStatus()} method to track the statuses on the remote and get a consistent mapping of the remote
 * status to the Agave job lifecycle.
 * @author dooley
 *
 */
public class JobStatusResponse<T extends RemoteSchedulerJobStatus> {
	protected String remoteJobId;
	protected String exitCode;
	protected T remoteSchedulerJobStatus;

	/**
	 * Creates a new {@link JobStatusResponse} instance setting the {@code remoteJobId} and
	 * {@code schedulerResponseText} to match the job being checked.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param remoteSchedulerJobStatus the job status on the remote system
	 * @param exitCode the exit code of the job.
	 */
	public JobStatusResponse(String remoteJobId, T remoteSchedulerJobStatus, String exitCode) {
		setRemoteJobId(remoteJobId);
		setRemoteSchedulerJobStatus(remoteSchedulerJobStatus);
		setExitCode(exitCode);
	}

	/**
	 * Fetches the mapped {@link JobStatusType} corresponding to the remote scheduler's job status. Since there
	 * is a mismatch between the individual job lifecycles documented by the {@link SchedulerType} supported by
	 * Agave, the mapping of remote job statuses to Agave {@link JobStatusType} is handled by the
	 * {@link JobStatusResponseParser} for each scheduler. We use this method as a convenience wrapper to get
	 * the {@link JobStatusType} for the response to use in job management decisions made by the monitoring
	 * services.
	 *
	 * @return the {@link Enum} implementation of JobStatusResponse for the remote scheduler.
	 * @see #getStatus()
	 */
	public JobStatusType getStatus() {
		return this.remoteSchedulerJobStatus.getMappedJobStatusType();
	}

	/**
	 * @return the jobId
	 */
	public String getRemoteJobId() {
		return remoteJobId;
	}

	/**
	 * @param remoteJobId the remote job id to set
	 */
	public void setRemoteJobId(String remoteJobId) {
		this.remoteJobId = remoteJobId;
	}

	/**
	 * The exit code of the job, if available.
	 * @return the exitCode of the job
	 */
	public String getExitCode() {
		return exitCode;
	}

	/**
	 * @param exitCode the exitCode of the job to set
	 */
	protected void setExitCode(String exitCode) {
		this.exitCode = exitCode;
	}

	/**
	 * @param remoteSchedulerJobStatus the remote scheduler job status to set
	 */
	private void setRemoteSchedulerJobStatus(T remoteSchedulerJobStatus) {
		this.remoteSchedulerJobStatus = remoteSchedulerJobStatus;
	}

	/**
	 * Fetches the remote job status parsed from the remote scheduler response. This should primarily be used for
	 * detailed debugging and scheduler specific retry and cleanup behavior in the individual {@link IJobMonitor}
	 * implementations. All status decisions should be made using the native {@link JobStatusType} returned by the
	 * {@link #getStatus()} method.
	 *
	 * @return the {@link Enum} implementation of JobStatusResponse for the remote scheduler.
	 * @see #getStatus()
	 */
	public T getRemoteSchedulerJobStatus() {
		return this.remoteSchedulerJobStatus;
	}
}
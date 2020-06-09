package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;
import static org.iplantc.service.jobs.model.enumerations.JobStatusType.*;

public enum LSFJobStatus implements RemoteSchedulerJobStatus<LSFJobStatus> {

	PEND("PEND", "The job is pending. That is, it has not yet been started.", QUEUED),

	PROV("PROV", "The job has been dispatched to a power-saved host that is waking up. " +
			"Before the job can be sent to the sbatchd, it is in a PROV state.", QUEUED),
	PSUSP("PSUSP", "The job has been suspended, either by its owner or the LSF administrator, while pending.", PAUSED),
	RUN("RUN", "The job is currently running.", RUNNING),
	USUSP("USUSP", "The job has been suspended, either by its owner or the LSF administrator, while running.", PAUSED),
	SSUSP("SSUSP", "The job has been suspended by LSF due to either of the following two causes:\n" +
			"- The load conditions on the execution host or hosts have exceeded a threshold according " +
			"to the loadStop vector defined for the host or queue.\n" +
			"- The run window of the job’s queue is closed. See bqueues(1), bhosts(1), and lsb.queues(5).", PAUSED),
	DONE("DONE", "The job has terminated with status of 0.", null),
	EXIT("EXIT", "The job has terminated with a non-zero status – it may have been aborted " +
			"due to an error in its execution, or killed by its owner or the LSF administrator. \n" +
			"For example, exit code 131 means that the job exceeded a configured resource usage limit and " +
			"LSF killed the job.", null),
	WAIT("WAIT", "Job is part of a chunk job and waiting to run.", QUEUED),
	ZOMBI("ZOMBI", "A job becomes ZOMBI if:\n" +
			"- A non-rerunnable job is killed by bkill while the sbatchd on the " +
			"execution host is unreachable and the job is shown as UNKWN.\n" +
			"- The host on which a rerunnable job is running is unavailable and the " +
			"job has been requeued by LSF with a new job ID, as if the job were submitted as a new job.\n" +
			"- After the execution host becomes available, LSF tries to kill the " +
			"ZOMBI job. Upon successful termination of the ZOMBI job, the job’s status is changed to EXIT.\n" +
			"With MultiCluster, when a job running on a remote execution cluster\n" +
			"becomes a ZOMBI job, the execution cluster treats the job the same way\n" +
			"as local ZOMBI jobs. In addition, it notifies the submission cluster\n" +
			"that the job is in ZOMBI state and the submission cluster requeues the job.", null),
	UNKWN("UNKWN", "mbatchd has lost contact with the sbatchd on the host on which the job runs.", null),
	UNKNOWN("", "Job status is unknown or could not be obtained.", null);

	private String description;
	private String code;
	private JobStatusType mappedJobStatusType;

	LSFJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
		this.setDescription(description);
		this.setCode(code);
		this.setMappedJobStatusType(mappedJobStatusType);
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the code
	 */
	public String getCode() {
		return code;
	}

	/**
	 * @param code the code to set
	 */
	public void setCode(String code) {
		this.code = code;
	}

	@Override
	public String toString() {
		return name() + " - " + getDescription();
	}

	/**
	 * Active statuses indicate that the job is in some sort of non-terminal state and has not been
	 * interrupted or placed in a paused or suspended state.
	 *
	 * @return list of {@link LSFJobStatus} associated with an active job status
	 */
	@Override
	public List<LSFJobStatus> getActiveStatuses() {
		ArrayList<LSFJobStatus> activeStatuses = new ArrayList<LSFJobStatus>(getQueuedStatuses());
		activeStatuses.addAll(getRunningStatuses());

		return activeStatuses;
	}

	/**
	 * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
	 *
	 * @return list of {@link LSFJobStatus} associated with an queued job status
	 */
	@Override
	public List<LSFJobStatus> getQueuedStatuses() {
		return List.of(PEND, PROV, WAIT);
	}

	/**
	 * Running statuses indicate that the job is currently running, or in process of setting up, running,
	 * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
	 * scheduler and does not overlap with {@link JobStatusType} at all.
	 *
	 * @return list of {@link LSFJobStatus} associated with an running job status
	 */
	@Override
	public List<LSFJobStatus> getRunningStatuses() {
		return List.of(RUN);
	}

	/**
	 * Returns whether this {@link LSFJobStatus} is the unknown type. This usually happens when a job is no longer
	 * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
	 * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
	 * the exit code, etc of the job.
	 *
	 * @return true if the status is unknown, false otherwise.
	 */
	@Override
	public List<LSFJobStatus> getUnknownStatuses() {
		return List.of(UNKNOWN, UNKWN);
	}

	/**
	 * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
	 * pre-empted, cancelled by a user, had a node fail, etc.
	 *
	 * @return list of {@link LSFJobStatus} associated with an active job status
	 */
	@Override
	public List<LSFJobStatus> getFailedStatuses() {
		return List.of(EXIT);
	}

	/**
	 * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
	 * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
	 * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
	 * moves on from the job once detected.
	 *
	 * @return list of {@link LSFJobStatus} associated with an active job status
	 */
	@Override
	public List<LSFJobStatus> getUnrecoverableStatuses() {
		return List.of(ZOMBI);
	}

	/**
	 * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
	 * being in a running state. This does not map to Agave functionality, but instead reflects the remote
	 * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
	 * is not guaranteed.
	 *
	 * @return list of {@link LSFJobStatus} associated with an active job status
	 */
	@Override
	public List<LSFJobStatus> getPausedStatuses() {
		return List.of(USUSP,SSUSP);
	}

	/**
	 * Indicates whether the status represents a running state on the remote system. This includes cleanup, teardown,
	 * staging, etc. on the remote scheduler. These statuses are all independent of Agave's {@link JobStatusType}
	 * statuses and are simply used to determine when to move the {@link Job} into the {@link JobStatusType#RUNNING}
	 * state.
	 *
	 * @return true if the status is in {@link #getActiveStatuses()}, false otherwise
	 * @throws IllegalArgumentException if the {@code satus} value is empty or not a known SlurmStatusType enumerated value.
	 * @see #getActiveStatuses()
	 */
	@Override
	public boolean isActiveStatus() {
		return getActiveStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link LSFJobStatus} is a completed state. Completed includes all statues that
	 * are not active or unknown.
	 *
	 * @return false if the job is active or unknown, true otherwise.
	 * @see #getActiveStatuses()
	 * @see #getUnknownStatuses()
	 */
	public boolean isDoneStatus() {
		return !isActiveStatus() && !isUnknownStatus();
	}

	/**
	 * Returns whether this {@link LSFJobStatus} represents a failure state of the remote job. This includes
	 * suspended jobs and jobs in transition states.
	 *
	 * @return true if the status is in {@link #getFailedStatuses()}, false otherwise
	 * @see #getFailedStatuses()
	 */
	@Override
	public boolean isFailureStatus() {
		return getFailedStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link LSFJobStatus} is a paused state. Paused jobs may be restarted in the
	 * future if the scheudler supports it. No guarantee is made to the availbility of this feature, this
	 * method simply reflects the scheduler's reporting that the job has been paused, usually by an administrator.
	 *
	 * @return true if the status is in {@link #getPausedStatuses()}, false otherwise
	 * @see #getPausedStatuses()
	 */
	@Override
	public boolean isPausedStatus() {
		return getPausedStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link LSFJobStatus}  is an unrecoverable state. This happens when the job is
	 * This includes suspended jobs.
	 *
	 * @return true if the status is in {@link #getUnrecoverableStatuses()}, false otherwise
	 * @see #getUnrecoverableStatuses()
	 */
	@Override
	public boolean isUnrecoverableStatus() {
		return getUnrecoverableStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link LSFJobStatus}  is the unknown type. This usually happens when a job is no longer
	 * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
	 * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
	 * the exit code, etc of the job.
	 *
	 * @return true if the status is in {@link #getUnknownStatuses()}, false otherwise
	 */
	@Override
	public boolean isUnknownStatus() {
		return getUnknownStatuses().contains(this);
	}

	/**
	 * Each remote scheduler job status is mapped to a {@link JobStatusType}. This method returns the mapped value
	 * corresponding to this {@link RemoteSchedulerJobStatus}.
	 *
	 * @return the {@link JobStatusType} corresponding to the scheduler status.
	 */
	@Override
	public JobStatusType getMappedJobStatusType() {
		return mappedJobStatusType;
	}

	/**
	 * Returns whether this {@link LSFJobStatus} is an queued state. Queued includes held jobs in queue.
	 *
	 * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
	 */
	@Override
	public boolean isQueuedStatus() {
		return getQueuedStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link LSFJobStatus} is a running state. Running varies from scheduler to scheduler
	 * based on the way it handles job startup, cleanup, and even reporting.
	 *
	 * @return true if the status is in {@link #getRunningStatuses()}, false otherwise
	 */
	public boolean isRunningStatus() {
		return getRunningStatuses().contains(this);
	}

	/**
	 * @param mappedJobStatusType the mappedJobStatusType to set
	 */
	private void setMappedJobStatusType(JobStatusType mappedJobStatusType) {
		this.mappedJobStatusType = mappedJobStatusType;
	}

	/**
	 * Returns the {@link LSFJobStatus} with code matching the value passed in regardless of case.
	 * This is similar to valueOf, but provides an {@link LSFJobStatus#UNKNOWN} value when
	 * no codes match.
	 * @param code the status code to match in a case-insensitive way.
	 * @return the {@link LSFJobStatus} with matching code, or {@link LSFJobStatus#UNKNOWN} if no matches.
	 */
	public static LSFJobStatus valueOfCode(String code) {
		for(LSFJobStatus status: values()) {
			if (status.code.equalsIgnoreCase(code)) {
				return status;
			}
		}

		return UNKNOWN;
	}
}
package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.jobs.model.enumerations.JobStatusType.PAUSED;
import static org.iplantc.service.jobs.model.enumerations.JobStatusType.QUEUED;

public enum SlurmJobStatus implements RemoteSchedulerJobStatus<SlurmJobStatus> {
	BOOT_FAIL("BOOT_FAIL", "Job terminated due to launch failure, typically due to a hardware failure "
			+ "(e.g. unable to boot the node or block and the job can not be requeued).", null),
	CANCELLED("CANCELLED", "Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.", null),
	DEADLINE("DEADLINE", "Job missed its deadline.", null),
	FAILED("FAILED", "Job terminated with non-zero exit code or other failure condition.", null),
	NODE_FAIL("NODE_FAIL", "Job terminated due to failure of one or more allocated nodes.", null),
	OUT_OF_MEMORY("OUT_OF_MEMORY", "Job experienced out of memory error.", null),
	PREEMPTED("PREEMPTED", "Job terminated due to preemption.", null),
	REVOKED("REVOKED", "Sibling was removed from cluster due to other cluster starting the job.", null),
	TIMEOUT("TIMEOUT", "Job terminated upon reaching its time limit.", null),

	EQW("EQW", "Job started but there was an unrecoverable error. Job will remain in this unrecoverable state until manually cleaned up.", null),

	CONFIGURING("CONFIGURING", "Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting).", QUEUED),
	PENDING("PENDING", "Job is awaiting resource allocation. Note for a job to be selected in this "
			+ "state it must have 'EligibleTime' in the requested time interval or different from "
			+ "'Unknown'. The 'EligibleTime' is displayed by the 'scontrol show job' command. "
			+ "For example jobs submitted with the '--hold' option will have 'EligibleTime=Unknown' "
			+ "as they are pending indefinitely.", QUEUED),
	REQUEUED("REQUEUED", "Job was requeued.", JobStatusType.QUEUED),

	RUNNING("RUNNING", "Job currently has an allocation.", JobStatusType.RUNNING),
	RESIZING("RESIZING", "Job is about to change size.", JobStatusType.RUNNING),
	COMPLETING("COMPLETING", "Job is in the process of completing. Some processes on some nodes may still be active.", JobStatusType.RUNNING),

	SUSPENDED("SUSPENDED", "Job has an allocation, but execution has been suspended.", PAUSED),

	COMPLETED("COMPLETED", "Job has terminated all processes on all nodes with an exit code of zero.", null),

	UNKNOWN("", "Job status is unknown or could not be obtained.", null);

	private String description;
	private String code;
	private JobStatusType mappedJobStatusType;

	SlurmJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
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
	 * @return list of {@link SlurmJobStatus} associated with an active job status
	 */
	@Override
	public List<SlurmJobStatus> getActiveStatuses() {
		ArrayList<SlurmJobStatus> activeStatuses = new ArrayList<SlurmJobStatus>(getQueuedStatuses());
		activeStatuses.addAll(getRunningStatuses());

		return activeStatuses;
	}

	/**
	 * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
	 *
	 * @return list of {@link SlurmJobStatus} associated with an queued job status
	 */
	@Override
	public List<SlurmJobStatus> getQueuedStatuses() {
		return List.of(PENDING, CONFIGURING, REQUEUED);
	}

	/**
	 * Running statuses indicate that the job is currently running, or in process of setting up, running,
	 * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
	 * scheduler and does not overlap with {@link JobStatusType} at all.
	 *
	 * @return list of {@link SlurmJobStatus} associated with an running job status
	 */
	@Override
	public List<SlurmJobStatus> getRunningStatuses() {
		return List.of(RUNNING, RESIZING, COMPLETING);
	}

	/**
	 * Returns whether this {@link SlurmJobStatus} is the unknown type. This usually happens when a job is no longer
	 * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
	 * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
	 * the exit code, etc of the job.
	 *
	 * @return true if the status is unknown, false otherwise.
	 */
	@Override
	public List<SlurmJobStatus> getUnknownStatuses() {
		return List.of(UNKNOWN);
	}

	/**
	 * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
	 * pre-empted, cancelled by a user, had a node fail, etc.
	 *
	 * @return list of {@link SlurmJobStatus} associated with an active job status
	 */
	@Override
	public List<SlurmJobStatus> getFailedStatuses() {
		return List.of(BOOT_FAIL, CANCELLED, DEADLINE, FAILED, NODE_FAIL, PREEMPTED, OUT_OF_MEMORY, REVOKED, TIMEOUT);
	}

	/**
	 * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
	 * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
	 * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
	 * moves on from the job once detected.
	 *
	 * @return list of {@link SlurmJobStatus} associated with an active job status
	 */
	@Override
	public List<SlurmJobStatus> getUnrecoverableStatuses() {
		return List.of(EQW);
	}

	/**
	 * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
	 * being in a running state. This does not map to Agave functionality, but instead reflects the remote
	 * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
	 * is not guaranteed.
	 *
	 * @return list of {@link SlurmJobStatus} associated with an active job status
	 */
	@Override
	public List<SlurmJobStatus> getPausedStatuses() {
		return List.of(SUSPENDED);
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
	 * Returns whether this {@link SlurmJobStatus} is a completed state. Completed includes all statues that
	 * are not active or unknown.
	 *
	 * @return false if the job is active or unknown, true otherwise.
	 * @see #getActiveStatuses()
	 * @see #getUnknownStatuses()
	 */
	public boolean isDoneStatus() {
		return !isActiveStatus() && !isUnknownStatus() && !isPausedStatus();
	}

	/**
	 * Returns whether this {@link SlurmJobStatus} represents a failure state of the remote job. This includes
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
	 * Returns whether this {@link SlurmJobStatus} is a paused state. Paused jobs may be restarted in the
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
	 * Returns whether this {@link SlurmJobStatus}  is an unrecoverable state. This happens when the job is
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
	 * Returns whether this {@link SlurmJobStatus}  is the unknown type. This usually happens when a job is no longer
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
	 * Returns whether this {@link SlurmJobStatus} is an queued state. Queued includes held jobs in queue.
	 *
	 * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
	 */
	@Override
	public boolean isQueuedStatus() {
		return getQueuedStatuses().contains(this);
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
	 * Returns whether this {@link SlurmJobStatus} is a running state. Running varies from scheduler to scheduler
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
	 * Returns the {@link SlurmJobStatus} with code matching the value passed in regardless of case.
	 * This is similar to valueOf, but provides an {@link SlurmJobStatus#UNKNOWN} value when
	 * no codes match.
	 * @param code the status code to match in a case-insensitive way.
	 * @return the {@link SlurmJobStatus} with matching code, or {@link SlurmJobStatus#UNKNOWN} if no matches.
	 */
	public static SlurmJobStatus valueOfCode(String code) {
		for(SlurmJobStatus status: values()) {
			if (status.code.equalsIgnoreCase(code)) {
				return status;
			}
		}

		return UNKNOWN;
	}
}
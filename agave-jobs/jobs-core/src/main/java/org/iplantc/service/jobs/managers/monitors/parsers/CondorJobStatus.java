package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.jobs.model.enumerations.JobStatusType.*;

public enum CondorJobStatus implements RemoteSchedulerJobStatus<CondorJobStatus> {

	TERMINATED("005", "Job terminated due to system or user intervention.", null),
	MISSING_INPUT("884", "Job cannot start due to missing input.", null),
	MISSING_OUTPUT("769", "Job failed to complete due to missing output.", null),
	EXECUTING("001", "Job is running.", RUNNING),
	SUBMITTED("000", "Job is submitted and waiting to run.", QUEUED),
	ABORTED  ("009", "Job was cancelled.", null),
	HELD("012", "Job was placed on hold.", PAUSED),
	FAILED("007", "Job failed.", null),
	UNKNOWN("", "Job status is unknown or could not be obtained.", null);

	private String description;
	private String code;
	private JobStatusType mappedJobStatusType;

	CondorJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
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
	 * @return list of {@link CondorJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorJobStatus> getActiveStatuses() {
		ArrayList<CondorJobStatus> activeStatuses = new ArrayList<CondorJobStatus>(getQueuedStatuses());
		activeStatuses.addAll(getRunningStatuses());

		return activeStatuses;
	}

	/**
	 * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
	 *
	 * @return list of {@link CondorJobStatus} associated with an queued job status
	 */
	@Override
	public List<CondorJobStatus> getQueuedStatuses() {
		return List.of(SUBMITTED);
	}

	/**
	 * Running statuses indicate that the job is currently running, or in process of setting up, running,
	 * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
	 * scheduler and does not overlap with {@link JobStatusType} at all.
	 *
	 * @return list of {@link CondorJobStatus} associated with an running job status
	 */
	@Override
	public List<CondorJobStatus> getRunningStatuses() {
		return List.of(EXECUTING);
	}

	/**
	 * Returns whether this {@link CondorJobStatus} is the unknown type. This usually happens when a job is no longer
	 * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
	 * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
	 * the exit code, etc of the job.
	 *
	 * @return true if the status is unknown, false otherwise.
	 */
	@Override
	public List<CondorJobStatus> getUnknownStatuses() {
		return List.of(UNKNOWN);
	}

	/**
	 * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
	 * pre-empted, cancelled by a user, had a node fail, etc.
	 *
	 * @return list of {@link CondorJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorJobStatus> getFailedStatuses() {
		return List.of(FAILED, ABORTED, MISSING_INPUT, MISSING_OUTPUT);
	}

	/**
	 * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
	 * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
	 * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
	 * moves on from the job once detected.
	 *
	 * @return list of {@link CondorJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorJobStatus> getUnrecoverableStatuses() {
		return List.of();
	}

	/**
	 * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
	 * being in a running state. This does not map to Agave functionality, but instead reflects the remote
	 * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
	 * is not guaranteed.
	 *
	 * @return list of {@link CondorJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorJobStatus> getPausedStatuses() {
		return List.of(HELD);
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
	 * Returns whether this {@link CondorJobStatus} is a completed state. Completed includes all statues that
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
	 * Returns whether this {@link CondorJobStatus} represents a failure state of the remote job. This includes
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
	 * Returns whether this {@link CondorJobStatus} is a paused state. Paused jobs may be restarted in the
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
	 * Returns whether this {@link CondorJobStatus}  is an unrecoverable state. This happens when the job is
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
	 * Returns whether this {@link CondorJobStatus}  is the unknown type. This usually happens when a job is no longer
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
	 * Returns whether this {@link CondorJobStatus} is an queued state. Queued includes held jobs in queue.
	 *
	 * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
	 */
	@Override
	public boolean isQueuedStatus() {
		return getQueuedStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link CondorJobStatus} is a running state. Running varies from scheduler to scheduler
	 * based on the way it handles job startup, cleanup, and even reporting.
	 *
	 * @return true if the status is in {@link #getRunningStatuses()}, false otherwise
	 */
	public boolean isRunningStatus() {
		return getRunningStatuses().contains(this);
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
	 * @param mappedJobStatusType the mappedJobStatusType to set
	 */
	private void setMappedJobStatusType(JobStatusType mappedJobStatusType) {
		this.mappedJobStatusType = mappedJobStatusType;
	}

	/**
	 * Returns the {@link CondorJobStatus} with code matching the value passed in regardless of case.
	 * This is similar to valueOf, but provides an {@link CondorJobStatus#UNKNOWN} value when
	 * no codes match.
	 * @param code the status code to match in a case-insensitive way.
	 * @return the {@link CondorJobStatus} with matching code, or {@link CondorJobStatus#UNKNOWN} if no matches.
	 */
	public static CondorJobStatus valueOfCode(String code) {
		String paddedCode = StringUtils.leftPad(code, 3, "0");
		for(CondorJobStatus status: values()) {
			if (status.code.equalsIgnoreCase(paddedCode)) {
				return status;
			}
		}

		return UNKNOWN;
	}
}
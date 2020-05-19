package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;

public enum PBSJobStatus implements RemoteSchedulerJobStatus<PBSJobStatus> {
    TRANSFER("T", "Job is being moved to new location.", JobStatusType.QUEUED),
    QUEUED("Q", "Job is queued.", JobStatusType.QUEUED),
    WAITING("W", "Job is waiting for its requested execution time to be reached or job specified a stagein request which failed for some reason.", JobStatusType.QUEUED),
    HELD("H", "Job is held.", JobStatusType.QUEUED),

    ARRAY_BEGUN("B", "Job arrays only. Job array has at least one subjob running.", JobStatusType.RUNNING),
    EXITING("E", "Job is exiting after having run.", JobStatusType.RUNNING),
    RUNNING("R", "Job is running.", JobStatusType.RUNNING),
    SUBJOB_EXPIRED("X", "Subjobs only. Subjob is finished (expired.)", JobStatusType.RUNNING),

    SUSPENDED("S", "Job is suspended.", JobStatusType.PAUSED),
    USER_INTERRUPT("U", "Cycle-harvesting job is suspended due to keyboard activity.", JobStatusType.PAUSED),

    FINISHED("F", "Job is finished.", null),
    MOVED("M", "Job was moved to another server.", null),

//    These are added by PBS/Torque v.2.4.16 (from Ubuntu)
    COMPLETED("C", "Job is completed after having run.", null), // distinct from F, but effectively the same for us
//    These are added by PBSPro
    UNKNOWN("", "Job status is unknown or could not be obtained.", null);

    private String description;
    private String code;
    private JobStatusType mappedJobStatusType;

    PBSJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
        this.setDescription(description);
        this.setCode(code);
        this.setMappedJobStatusType(mappedJobStatusType);
    }

    /**
     * @return the description
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    private void setDescription(String description) {
        this.description = description;
    }

    /**
     * The status code as provided by the remote scheduler. These should generally map 1-1 with the available
     * status codes from the remote scheduler.
     *
     * @return the code
     */
    @Override
    public String getCode() {
        return code;
    }

    /**
     * @param code the code to set
     */
    private void setCode(String code) {
        this.code = code;
    }

    /**
     * Active statuses indicate that the job is in some sort of non-terminal state and has not been
     * interrupted or placed in a paused or suspended state.
     *
     * @return list of {@link PBSJobStatus} associated with an active job status
     */
    @Override
    public List<PBSJobStatus> getActiveStatuses() {
        ArrayList<PBSJobStatus> activeStatuses = new ArrayList<PBSJobStatus>(getQueuedStatuses());
        activeStatuses.addAll(getRunningStatuses());

        return activeStatuses;
    }

    /**
     * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
     *
     * @return list of {@link PBSJobStatus} associated with an queued job status
     */
    @Override
    public List<PBSJobStatus> getQueuedStatuses() {
        return List.of(TRANSFER, QUEUED, WAITING, HELD, MOVED);
    }

    /**
     * Running statuses indicate that the job is currently running, or in process of setting up, running,
     * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
     * scheduler and does not overlap with {@link JobStatusType} at all.
     *
     * @return list of {@link PBSJobStatus} associated with an running job status
     */
    @Override
    public List<PBSJobStatus> getRunningStatuses() {
        return List.of(ARRAY_BEGUN, EXITING, RUNNING, SUBJOB_EXPIRED);
    }

    /**
     * Unknown statuses indicate that the job status could not be deciphered.
     *
     * @return list of {@link PBSJobStatus} associated with an unknown job status
     */
    @Override
    public List<PBSJobStatus> getUnknownStatuses() {
        return List.of(UNKNOWN);
    }

    /**
     * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
     * pre-empted, cancelled by a user
     * s
     * @return list of {@link PBSJobStatus} associated with an active job status
     */
    @Override
    public List<PBSJobStatus> getFailedStatuses() {
        return List.of();
    }

    /**
     * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
     * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
     * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
     * moves on from the job once detected.
     *
     * @return list of {@link PBSJobStatus} associated with an active job status
     */
    @Override
    public List<PBSJobStatus> getUnrecoverableStatuses() {
        return List.of();
    }

    /**
     * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
     * being in a running state. This does not map to Agave functionality, but instead reflects the remote
     * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
     * is not guaranteed.
     *
     * @return list of {@link PBSJobStatus} associated with an active job status
     */
    @Override
    public List<PBSJobStatus> getPausedStatuses() {
        return List.of(SUSPENDED, USER_INTERRUPT);
    }

    /**
     * Indicates whether the status represents a running state on the remote system. This includes cleanup, teardown,
     * staging, etc. on the remote scheduler. These statuses are all independent of Agave's {@link JobStatusType}
     * statuses and are simply used to determine when to move the {@link Job} into the {@link JobStatusType#RUNNING}
     * state.
     *
     * @return true if the status is in {@link #getActiveStatuses()}, false otherwise
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
        return !isActiveStatus() && !isUnknownStatus();
    }

    /**
     * Returns whether this {@link PBSJobStatus} represents a failure state of the remote job. This includes
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
     * Returns whether this {@link PBSJobStatus} is a paused state. Paused jobs may be restarted in the
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
     * Returns whether this {@link PBSJobStatus}  is an unrecoverable state. This happens when the job is
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
     * Returns whether this {@link PBSJobStatus}  is the unknown type. This usually happens when a job is no longer
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
     * Returns whether this {@link PBSJobStatus} is an queued state. Queued includes held jobs in queue.
     *
     * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
     */
    @Override
    public boolean isQueuedStatus() {
        return getQueuedStatuses().contains(this);
    }

    /**
     * Returns whether this {@link PBSJobStatus} is a running state. Running varies from scheduler to scheduler
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
     * Returns the {@link PBSJobStatus} with code matching the value passed in regardless of case.
     * This is similar to valueOf, but provides an {@link PBSJobStatus#UNKNOWN} value when
     * no codes match.
     * @param code the status code to match in a case-insensitive way.
     * @return the {@link PBSJobStatus} with matching code, or {@link PBSJobStatus#UNKNOWN} if no matches.
     */
    public static PBSJobStatus valueOfCode(String code) {
        for(PBSJobStatus status: values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }

        return UNKNOWN;
    }
}
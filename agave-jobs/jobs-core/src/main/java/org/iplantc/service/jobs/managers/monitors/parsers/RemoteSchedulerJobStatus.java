package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.List;

public interface RemoteSchedulerJobStatus<T> {
    /**
     * A human readable description of the status on the remote scheduler. This is used for logging, event
     * notifications, etc.
     * @return the description
     */
    public String getDescription();

    /**
     * The status code as provided by the remote scheduler. These should generally map 1-1 with the available
     * status codes from the remote scheduler.
     * @return the code
     */
    public String getCode();

    /**
     * Active statuses indicate that the job is in some sort of non-terminal state and has not been
     * interrupted or placed in a paused or suspended state.
     *
     * @return list of {@link T} associated with an active job status
     */
    public List<T> getActiveStatuses();

    /**
     * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
     *
     * @return list of {@link T} associated with an queued job status
     */
    public List<T> getQueuedStatuses();

    /**
     * Running statuses indicate that the job is currently running, or in process of setting up, running,
     * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
     * scheduler and does not overlap with {@link JobStatusType} at all.
     *
     * @return list of {@link T} associated with an running job status
     */
    public List<T> getRunningStatuses();

    /**
     * Unknown statuses indicate that the job status could not be deciphered.
     * @return list of {@link T} associated with an unknown job status
     */
    public List<T> getUnknownStatuses();

    /**
     * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
     * pre-empted, cancelled by a user
     *
     * @return list of {@link T} associated with an active job status
     */
    public List<T> getFailedStatuses();

    /**
     * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
     * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
     * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
     * moves on from the job once detected.
     *
     * @return list of {@link T} associated with an active job status
     */
    public List<T> getUnrecoverableStatuses();

    /**
     * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
     * being in a running state. This does not map to Agave functionality, but instead reflects the remote
     * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
     * is not guaranteed.
     *
     * @return list of {@link T} associated with an active job status
     */
    public List<T> getPausedStatuses();

    /**
     * Indicates whether the status represents a running state on the remote system. This includes cleanup, teardown,
     * staging, etc. on the remote scheduler. These statuses are all independent of Agave's {@link JobStatusType}
     * statuses and are simply used to determine when to move the {@link Job} into the {@link JobStatusType#RUNNING}
     * state.
     *
     * @return true if the status is in {@link #getActiveStatuses()}, false otherwise
     * @see #getActiveStatuses()
     */
    public boolean isActiveStatus();

    /**
     * Returns whether this {@link T} represents a failure state of the remote job. This includes
     * suspended jobs and jobs in transition states.
     *
     * @return true if the status is in {@link #getFailedStatuses()}, false otherwise
     * @see #getFailedStatuses()
     */
    public boolean isFailureStatus();

    /**
     * Returns whether this {@link T} is a completed state. Completed includes all statues that
     * are not active or unknown.
     *
     * @return false if the job is active or unknown, true otherwise.
     * @see #getActiveStatuses()
     * @see #getUnknownStatuses()
     */
    public boolean isDoneStatus();

    /**
     * Returns whether this {@link T} is a paused state. Paused jobs may be restarted in the
     * future if the scheudler supports it. No guarantee is made to the availbility of this feature, this
     * method simply reflects the scheduler's reporting that the job has been paused, usually by an administrator.
     *
     * @return true if the status is in {@link #getPausedStatuses()}, false otherwise
     * @see #getPausedStatuses()
     */
    public boolean isPausedStatus();

    /**
     * Returns whether this {@link T} is an unrecoverable state. This happens when the job is
     * This includes suspended jobs.
     *
     * @return true if the status is in {@link #getUnrecoverableStatuses()}, false otherwise
     * @see #getUnrecoverableStatuses()
     */
    public boolean isUnrecoverableStatus();

    /**
     * Returns whether this {@link T} is the unknown type. This usually happens when a job is no longer
     * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
     * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
     * the exit code, etc of the job.
     *
     * @return true if the status is in {@link #getUnknownStatuses()}, false otherwise
     */
    public boolean isUnknownStatus();

    /**
     * Returns whether this {@link T} is an queued state. Queued includes held jobs in queue.
     *
     * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
     */
    public boolean isQueuedStatus();

    /**
     * Returns whether this {@link T} is a running state. Running varies from scheduler to scheduler based on the
     * way it handles job startup, cleanup, and even reporting.
     *
     * @return true if the status is in {@link #getRunningStatuses()}, false otherwise
     */
    public boolean isRunningStatus();

    /**
     * Each remote scheduler job status is mapped to a {@link JobStatusType}. This method returns the mapped value
     * corresponding to this {@link RemoteSchedulerJobStatus}.
     *
     * @return the {@link JobStatusType} corresponding to the scheduler status.
     */
    public JobStatusType getMappedJobStatusType();
}

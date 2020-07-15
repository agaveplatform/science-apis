package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.jobs.model.enumerations.JobStatusType.*;

public enum SGEJobStatus implements RemoteSchedulerJobStatus<SGEJobStatus> {
	qw("qw", "Queued and waiting.", QUEUED),
	w("w", "Job waiting.", QUEUED),
    H("h", "Job hold", QUEUED),
    hqw("hqw", "Job placed on hold while queued, and waiting.", QUEUED),
    hRwq("hRqw", "Resubmitting while and placed on hold while queued, and waiting.", QUEUED),

    R("R", "Job restarted", RUNNING),
    t("t", "Job transferring and about to start", RUNNING),
	r("r", "Job running", RUNNING),
    Rr("Rr", "Job running, being resubmitted", RUNNING),
    Rt("Rt", "Job is transferring, being resubmitted", RUNNING),

    s("s", "Job suspended", PAUSED),
    Rs("Rs", "Resubmitting job suspended.", PAUSED),
    RtS("RtS", "Resubmitting job suspended.", PAUSED),
    Rts("Rts", "Resubmitting job suspended.", PAUSED),
    RT("RT", "Resubmitting job suspended.", PAUSED),
    RtT("RtT", "Resubmitting job suspended.", PAUSED),
    ts("ts", "Job suspended", PAUSED),
    S("S", "Queue suspended", PAUSED),
    tS("tS", "Queue suspended", PAUSED),
    T("T", "Queue suspended by alarm", PAUSED),
    tT("tT", "Queue suspended by alarm", PAUSED),

    Eqw("Eqw", "An error occurred with the job while queued.", FAILED),
    Ehqw("Ehqw", "An error occurred with the job while held in queue.", FAILED),
    EhRqw("EhRqw", "An error occurred with the job while resubmitting held job in queue.", FAILED),

    d("d", "Job has been marked for deletion", null),
    dr("dr", "Job has been marked for deletion", null),
    dt("dt", "Job has been marked for deletion", null),
    dT("dT", "Job has been marked for deletion", null),
    dRr("dRr", "Job has been marked for deletion", null),
    dRt("dRt", "Job has been marked for deletion", null),
    ds("ds", "Job has been marked for deletion", null),
    dRs("dRs", "Job has been marked for deletion", null),
    dRS("dRS", "Job has been marked for deletion", null),
    dRT("dRT", "Job has been marked for deletion", null),

	UNKNOWN("", "Job status is unknown or could not be obtained.", null);

    private String description;
    private String code;
    private JobStatusType mappedJobStatusType;

    SGEJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
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
     * @return list of {@link SGEJobStatus} associated with an active job status
     */
    @Override
    public List<SGEJobStatus> getActiveStatuses() {
        ArrayList<SGEJobStatus> activeStatuses = new ArrayList<SGEJobStatus>(getQueuedStatuses());
        activeStatuses.addAll(getRunningStatuses());

        return activeStatuses;
    }

    /**
     * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
     *
     * @return list of {@link SGEJobStatus} associated with an queued job status
     */
    @Override
    public List<SGEJobStatus> getQueuedStatuses() {
        return List.of(qw, w, hqw, hRwq);
    }

    /**
     * Running statuses indicate that the job is currently running, or in process of setting up, running,
     * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
     * scheduler and does not overlap with {@link JobStatusType} at all.
     *
     * @return list of {@link SGEJobStatus} associated with an running job status
     */
    @Override
    public List<SGEJobStatus> getRunningStatuses() {
        return List.of(r, t, R, Rr, Rt);
    }

    /**
     * Unknown statuses indicate that the job status could not be deciphered.
     *
     * @return list of {@link SGEJobStatus} associated with an unknown job status
     */
    @Override
    public List<SGEJobStatus> getUnknownStatuses() {
        return List.of(UNKNOWN);
    }

    /**
     * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
     * pre-empted, cancelled by a user
     *
     * @return list of {@link SGEJobStatus} associated with an active job status
     */
    @Override
    public List<SGEJobStatus> getFailedStatuses() {
        return List.of(Eqw, Ehqw, EhRqw);
    }

    /**
     * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
     * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
     * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
     * moves on from the job once detected.
     *
     * @return list of {@link SGEJobStatus} associated with an active job status
     */
    @Override
    public List<SGEJobStatus> getUnrecoverableStatuses() {
        return List.of(d, dr, dt, dT, dRr, dRt, ds, dRs, dRS, dRT);
    }

    /**
     * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
     * being in a running state. This does not map to Agave functionality, but instead reflects the remote
     * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
     * is not guaranteed.
     *
     * @return list of {@link SGEJobStatus} associated with an active job status
     */
    @Override
    public List<SGEJobStatus> getPausedStatuses() {
        return List.of(s, S, ts, tS, T, tT, Rts, Rs, RtS, RT, RtT);
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
        return !isActiveStatus() && !isUnknownStatus() && !isPausedStatus();
    }

    /**
     * Returns whether this {@link SGEJobStatus} represents a failure state of the remote job. This includes
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
     * Returns whether this {@link SGEJobStatus} is a paused state. Paused jobs may be restarted in the
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
     * Returns whether this {@link SGEJobStatus}  is an unrecoverable state. This happens when the job is
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
     * Returns whether this {@link SGEJobStatus}  is the unknown type. This usually happens when a job is no longer
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
     * Returns whether this {@link SGEJobStatus} is an queued state. Queued includes held jobs in queue.
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
     * Returns whether this {@link SGEJobStatus} is a running state. Running varies from scheduler to scheduler
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
     * Returns the {@link SGEJobStatus} with code matching the value passed in. Unlike other {@link RemoteSchedulerJobStatus}
     * classes, case sensitivity matters here due to the recycling of status codes with upper and lower cases.
     * This is similar to valueOf, but provides an {@link SGEJobStatus#UNKNOWN} value when no codes match.
     * @param code the status code to match. Case-sensitivity matters.
     * @return the {@link SGEJobStatus} with matching code, or {@link SGEJobStatus#UNKNOWN} if no matches.
     */
    public static SGEJobStatus valueOfCode(String code) {
        for(SGEJobStatus status: values()) {
            if (status.code.equals(code)) {
                return status;
            }
        }

        return UNKNOWN;
    }
}
package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;

import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.jobs.model.enumerations.JobStatusType.*;

/**
 * Condor job statuses and status codes. Values provided by tables on HTCondor Wiki:
 * <a href="https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers">https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers</a>.
 */
public enum CondorLogJobStatus implements RemoteSchedulerJobStatus<CondorLogJobStatus> {

	MISSING_INPUT("884", "Job cannot start due to missing input.", null),
	MISSING_OUTPUT("769", "Job failed to complete due to missing output.", null),

	// queued
	JOB_SUBMITTED("000", "Job is submitted and waiting to run.", JobStatusType.QUEUED),
	JOB_EVICTED_FROM_MACHINE("004", "A job was removed from a machine before it finished, usually for a policy reason. Perhaps an interactive user has claimed the computer, or perhaps another job is higher priority.", JobStatusType.QUEUED),
	SHADOW_EXCEPTION("007", "The condor_shadow, a program on the submit computer that watches over the job and performs some services for the job, failed for some catastrophic reason. The job will leave the machine and go back into the queue.", JobStatusType.QUEUED),
	JOB_WAS_RELEASED("013", "The job was in the hold state and is to be re-run.", JobStatusType.QUEUED),
	GLOBUS_SUBMIT("017", "Job submitted to Globus.", JobStatusType.QUEUED),
	GLOBUS_SUBMIT_FAILED("018", "The attempt to delegate a job to Globus failed.", JobStatusType.QUEUED),
	GLOBUS_RESOURCE_BACK_UP("019", "The Globus resource that a job wants to run on was unavailable, but is now available. This event is no longer used.", JobStatusType.QUEUED),
	DETECTED_DOWN_GLOBUS_RESOURCE("020", "The Globus resource that a job wants to run on has become unavailable. This event is no longer used.", JobStatusType.QUEUED),
	GRID_RESOURCE_BACK_UP("025", "A grid resource that was previously unavailable is now available.", JobStatusType.QUEUED),
	DETECTED_DOWN_GRID_RESOURCE("026", "The grid resource that a job is to run on is unavailable.", JobStatusType.QUEUED),
	JOB_SUBMITTED_TO_GRID_RESOURCE("027", "A job has been submitted, and is under the auspices of the grid resource.", JobStatusType.QUEUED),
	FACTORY_SUBMIT("035", "This event occurs when a user submits a cluster job using late materialization.", null),

	//running
	JOB_EXECUTING("001", "Job is running. It might occur more than once.", JobStatusType.RUNNING),
	JOB_WAS_CHECKPOINTED("003", "The job’s complete state was written to a checkpoint file. This might happen without the job being removed from a machine, because the checkpointing can happen periodically.", JobStatusType.RUNNING),
	IMAGE_SIZE_OF_JOB_UPDATED("006", "An informational event, to update the amount of memory that the job is using while running. It does not reflect the state of the job.", JobStatusType.RUNNING),
	JOB_WAS_UNSUSPENDED("011", " The job has resumed execution, after being suspended earlier.", JobStatusType.RUNNING),
	PARALLEL_NODE_EXECUTED("014", "A parallel universe program is running on a node.", JobStatusType.RUNNING),
	REMOTE_ERROR("021", "The condor_starter (which monitors the job on the execution machine) has failed.", JobStatusType.RUNNING),
	REMOTE_SYSTEM_CALL_SOCKET_LOST("022", "The condor_shadow and condor_starter (which communicate while the job runs) have lost contact.", JobStatusType.RUNNING),
	REMOTE_SYSTEM_CALL_SOCKET_REESTABLISHED("023", "The condor_shadow and condor_starter (which communicate while the job runs) have been able to resume contact before the job lease expired.", JobStatusType.RUNNING),
	REMOTE_SYSTEM_CALL_RECONNECT_FAILURE("024", "The condor_shadow and condor_starter (which communicate while the job runs) were unable to resume contact before the job lease expired.", JobStatusType.RUNNING),
	JOB_AD_INFORMATION_EVENT_TRIGGERED("028", "Extra job ClassAd attributes are noted. This event is written as a supplement to other events when the configuration parameter EVENT_LOG_JOB_AD_INFORMATION_ATTRS is set.", JobStatusType.RUNNING),
	JOB_REMOTE_STATUS_UNKNOWN("029", "No updates of the job’s remote status have been received for 15 minutes.", JobStatusType.RUNNING),
	JOB_REMOTE_STATUS_KNOWN_AGAIN("030", "An update has been received for a job whose remote status was previous logged as unknown.", JobStatusType.RUNNING),
	JOB_STAGE_IN("031", "A grid universe job is doing the stage in of input files.", JobStatusType.RUNNING),
	JOB_STAGE_OUT("032", "A grid universe job is doing the stage out of output files.", JobStatusType.RUNNING),
	JOB_CLASSAD_ATTRIBUTE_UPDATE("033", "A Job ClassAd attribute is changed due to action by the condor_schedd daemon. This includes changes by condor_prio.", JobStatusType.RUNNING),
	FACTORY_RESUMED("038", "This event occurs when job materialization for a cluster has been resumed", JobStatusType.RUNNING),
	PRE_SKIP_EVENT("034", "For DAGMan, this event is logged if a PRE SCRIPT exits with the defined PRE_SKIP value in the DAG input file. This makes it possible for DAGMan to do recovery in a workflow that has such an event, as it would otherwise not have any event for the DAGMan node to which the script belongs, and in recovery, DAGMan’s internal tables would become corrupted.", JobStatusType.RUNNING),


	// paused
	JOB_WAS_SUSPENDED("010", "The job is still on the computer, but it is no longer executing. This is usually for a policy reason, such as an interactive user using the computer.", JobStatusType.PAUSED),
	JOB_WAS_HELD("012", "The job has transitioned to the hold state. This might happen if the user applies the condor_hold command to the job.", JobStatusType.PAUSED),
	FACTORY_PAUSED("037", "This event occurs when job materialization for a cluster has been paused.", JobStatusType.PAUSED),

	// failed
	ERROR_IN_EXECUTABLE("002", "The job could not be run because the executable was bad.", null),

	// cancelled/completed
	JOB_TERMINATED("005", "The job has completed.", null),
	JOB_ABORTED  ("009", "Job was cancelled by user.", null),
	NODE_TERMINATED("015", "A parallel universe program has completed on a node.", null),
	POST_SCRIPT_TERMINATED("016", "A node in a DAGMan work flow has a script that should be run after a job. The script is run on the submit host. This event signals that the post script has completed.", null),
	CLUSTER_REMOVED("036", "Only written for clusters using late materialization. This event occurs after all the jobs in a cluster submitted using late materialization have materialized and completed, or when the cluster is removed (by condor_rm).", null),


	// misc
	GENERIC_LOG_EVENT("008", "Empty event", null),
	NONE("039", "This event should never occur in a log but may be returned by log reading code in certain situations (e.g., timing out while waiting for a new event to appear in the log).", null),
	UNKNOWN("", "Job status is unknown or could not be obtained.", null);


	private String description;
	private String code;
	private JobStatusType mappedJobStatusType;

	CondorLogJobStatus(String code, String description, JobStatusType mappedJobStatusType) {
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
	 * @return list of {@link CondorLogJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorLogJobStatus> getActiveStatuses() {
		ArrayList<CondorLogJobStatus> activeStatuses = new ArrayList<CondorLogJobStatus>(getQueuedStatuses());
		activeStatuses.addAll(getRunningStatuses());

		return activeStatuses;
	}

	/**
	 * Queued statuses indicate that the job currently waiting to run in the remote scheduler's queue.
	 *
	 * @return list of {@link CondorLogJobStatus} associated with an queued job status
	 */
	@Override
	public List<CondorLogJobStatus> getQueuedStatuses() {
		return List.of(JOB_SUBMITTED, JOB_EVICTED_FROM_MACHINE, SHADOW_EXCEPTION, JOB_WAS_RELEASED, GLOBUS_SUBMIT, GLOBUS_SUBMIT_FAILED, GLOBUS_RESOURCE_BACK_UP, DETECTED_DOWN_GLOBUS_RESOURCE, GRID_RESOURCE_BACK_UP, DETECTED_DOWN_GRID_RESOURCE, JOB_SUBMITTED_TO_GRID_RESOURCE, FACTORY_SUBMIT);
	}

	/**
	 * Running statuses indicate that the job is currently running, or in process of setting up, running,
	 * or cleaning up on the remote system. This phase of the lifecycle is completely handled by the
	 * scheduler and does not overlap with {@link JobStatusType} at all.
	 *
	 * @return list of {@link CondorLogJobStatus} associated with an running job status
	 */
	@Override
	public List<CondorLogJobStatus> getRunningStatuses() {
		return List.of(JOB_EXECUTING, JOB_WAS_CHECKPOINTED, IMAGE_SIZE_OF_JOB_UPDATED, JOB_WAS_UNSUSPENDED, PARALLEL_NODE_EXECUTED, REMOTE_ERROR, REMOTE_SYSTEM_CALL_SOCKET_LOST, REMOTE_SYSTEM_CALL_SOCKET_REESTABLISHED, REMOTE_SYSTEM_CALL_RECONNECT_FAILURE, JOB_AD_INFORMATION_EVENT_TRIGGERED, JOB_REMOTE_STATUS_UNKNOWN, JOB_REMOTE_STATUS_KNOWN_AGAIN, JOB_STAGE_IN, JOB_STAGE_OUT, JOB_CLASSAD_ATTRIBUTE_UPDATE, FACTORY_RESUMED, PRE_SKIP_EVENT);
	}

	/**
	 * Returns whether this {@link CondorLogJobStatus} is the unknown type. This usually happens when a job is no longer
	 * available for query from the remote scheduler's queue listing due to the job being archived, or rotated out of
	 * the queue listing after completion. For HPC jobs, this usually means further queries should be made to decipher
	 * the exit code, etc of the job.
	 *
	 * @return true if the status is unknown, false otherwise.
	 */
	@Override
	public List<CondorLogJobStatus> getUnknownStatuses() {
		return List.of(GENERIC_LOG_EVENT, NONE, UNKNOWN);
	}

	/**
	 * Failed statuses indicate that the job failed to run to completion in some way. It may have been killed,
	 * pre-empted, cancelled by a user, had a node fail, etc.
	 *
	 * @return list of {@link CondorLogJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorLogJobStatus> getFailedStatuses() {
		return List.of();
	}

	/**
	 * Unrecoverable statuses indicate that something went horribly wrong on the remote system and the scheduler
	 * has given up on not only running the job, but in any way cleaning up after it. These jobs can be suck in
	 * this state indefinitely without intervention from an administrator. Agave treats this as a failure and
	 * moves on from the job once detected.
	 *
	 * @return list of {@link CondorLogJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorLogJobStatus> getUnrecoverableStatuses() {
		return List.of(ERROR_IN_EXECUTABLE);
	}

	/**
	 * Paused statuses indicate that the job has been placed in a susupended state, but not cancelled, after
	 * being in a running state. This does not map to Agave functionality, but instead reflects the remote
	 * scheduler functionality. The behavior of the scheduler and code upon returning to a running state
	 * is not guaranteed.
	 *
	 * @return list of {@link CondorLogJobStatus} associated with an active job status
	 */
	@Override
	public List<CondorLogJobStatus> getPausedStatuses() {
		return List.of(JOB_WAS_SUSPENDED, JOB_WAS_HELD, FACTORY_PAUSED);
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
	 * Returns whether this {@link CondorLogJobStatus} is a completed state. Completed includes all statues that
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
	 * Returns whether this {@link CondorLogJobStatus} represents a failure state of the remote job. This includes
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
	 * Returns whether this {@link CondorLogJobStatus} is a paused state. Paused jobs may be restarted in the
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
	 * Returns whether this {@link CondorLogJobStatus}  is an unrecoverable state. This happens when the job is
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
	 * Returns whether this {@link CondorLogJobStatus}  is the unknown type. This usually happens when a job is no longer
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
	 * Returns whether this {@link CondorLogJobStatus} is an queued state. Queued includes held jobs in queue.
	 *
	 * @return true if the status is in {@link #getQueuedStatuses()}, false otherwise
	 */
	@Override
	public boolean isQueuedStatus() {
		return getQueuedStatuses().contains(this);
	}

	/**
	 * Returns whether this {@link CondorLogJobStatus} is a running state. Running varies from scheduler to scheduler
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
	 * Returns the {@link CondorLogJobStatus} with code matching the value passed in regardless of case.
	 * This is similar to valueOf, but provides an {@link CondorLogJobStatus#UNKNOWN} value when
	 * no codes match.
	 * @param code the status code to match in a case-insensitive way.
	 * @return the {@link CondorLogJobStatus} with matching code, or {@link CondorLogJobStatus#UNKNOWN} if no matches.
	 */
	public static CondorLogJobStatus valueOfCode(String code) {
		String paddedCode = StringUtils.leftPad(code, 3, "0");
		for(CondorLogJobStatus status: values()) {
			if (status.code.equalsIgnoreCase(paddedCode)) {
				return status;
			}
		}

		return UNKNOWN;
	}
}
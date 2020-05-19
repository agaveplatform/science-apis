package org.iplantc.service.jobs.managers.monitors;

import java.nio.channels.ClosedByInterruptException;

import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.*;
import org.iplantc.service.jobs.managers.JobEventProcessor;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.jobs.managers.monitors.parsers.JobStatusResponseParser;
import org.iplantc.service.jobs.managers.monitors.parsers.JobMonitorResponseParserFactory;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobEventType;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.LoginProtocolType;
import org.joda.time.DateTime;

/**
 * Monitors a remote {@link Job} by querying the {@link ExecutionSystem} for the local job id.
 *
 * @author dooley
 */
public class DefaultJobMonitor extends AbstractJobMonitor {
	private static final Logger log = Logger.getLogger(DefaultJobMonitor.class);
	
	public DefaultJobMonitor(Job job)
	{
		super(job);
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.managers.monitors.AbstractJobMonitor#monitor()
	 */
	@Override
	public Job monitor() throws RemoteJobMonitoringException, SystemUnavailableException, ClosedByInterruptException {
		// skip it all if the job is null
		if (getJob() == null) return null;

		RemoteSubmissionClient remoteSubmissionClient = null;

		// get the execution system from the getter to ensure we check availability before proceeding.
		ExecutionSystem executionSystem = getExecutionSystem();

		// if the execution system login config is local, then we cannot submit
		// jobs to this system remotely. In this case, a worker will be running
		// dedicated to that system and will submitting jobs locally. All workers
		// other that this will should pass on accepting this job.
		if (executionSystem.getLoginConfig().getProtocol().equals(LoginProtocolType.LOCAL) &&
				!Settings.LOCAL_SYSTEM_ID.equals(job.getSystem())) {
			return this.job;
		} else { // otherwise, throw it in remotely
			try {
				// increment the number of checks
				this.job.setLastUpdated(new DateTime().toDate());
				this.job.setStatusChecks(this.job.getStatusChecks() + 1);
				updateJobStatus(this.job.getStatus(), this.job.getErrorMessage());

				checkStopped();

				String queryCommand = String.format("%s%s %s",
						getStartupScriptCommand(),
						executionSystem.getScheduler().getBatchQueryCommand(),
						job.getLocalJobId());

				String schedulerResponseText = getJobStatusResponse(queryCommand);

				JobStatusResponseParser responseParser = new JobMonitorResponseParserFactory().getInstance(job);

				try {
					JobStatusResponse response = responseParser.parse(job.getLocalJobId(), schedulerResponseText);

					// if the job status has changed
					if (job.getStatus() != response.getStatus()) {
						// if the response status maps to a job status, use that
						if (response.getRemoteSchedulerJobStatus().isFailureStatus()) {
							JobStatusType nextStatus = job.isArchiveOutput() ? JobStatusType.CLEANING_UP : JobStatusType.FAILED;
							log.debug("Job " + job.getUuid() + " was found in a failure state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + nextStatus.name() + ".");

							updateJobStatus(nextStatus,
									"Job status change to failed detected by job monitor.");
						} else if (response.getRemoteSchedulerJobStatus().isDoneStatus()) {
							log.debug("Job " + job.getUuid() + " was found in a completed state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + JobStatusType.CLEANING_UP.name() + ".");

							updateJobStatus(JobStatusType.CLEANING_UP,
									"Job status change to completed detected by job monitor.");
						} else if (response.getRemoteSchedulerJobStatus().getRunningStatuses().contains(response.getRemoteSchedulerJobStatus())) {
							log.debug("Job " + job.getUuid() + " was found in a running state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + JobStatusType.RUNNING.name() + ".");

							updateJobStatus(JobStatusType.RUNNING,
									"Job status change to running detected by job monitor.");
						} else if (response.getRemoteSchedulerJobStatus().isQueuedStatus()) {
							log.debug("Job " + job.getUuid() + " was found in a queued state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + JobStatusType.QUEUED.name() + ".");

							updateJobStatus(JobStatusType.QUEUED,
									"Job status change to queued detected by job monitor.");
						} else if (response.getRemoteSchedulerJobStatus().isPausedStatus()) {
							log.debug("Job " + job.getUuid() + " was found in a paused state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + JobStatusType.PAUSED.name() + ".");

							updateJobStatus(JobStatusType.PAUSED,
									"Job status change to paused detected by job monitor.");
						} else if (response.getRemoteSchedulerJobStatus().isUnrecoverableStatus()) {
							JobStatusType nextStatus = job.isArchiveOutput() ? JobStatusType.CLEANING_UP : JobStatusType.FAILED;
							log.debug("Job " + job.getUuid() + " was found in an unrecoverable state on " +
									job.getSystem() + " as local job id " + job.getLocalJobId() +
									". Updating status to " + nextStatus.name() + ".");

							updateJobStatus(nextStatus,
									"Job status change to an unrecoverable state detected by job monitor.");
						} else {
							updateJobStatus(job.getStatus(),
									"Job " + job.getUuid() + " was found in an unknown state on " +
											job.getSystem() + " as local job id " + job.getLocalJobId() +
											". The job will remain active until a terminal state is detected.");
						}
					} else {
						log.debug("Job " + job.getUuid() + " was found in an " +
								job.getStatus().name().toLowerCase() + " on " +
								job.getSystem() + " as local job id " + job.getLocalJobId() + ".");

						updateJobStatus(job.getStatus(), job.getErrorMessage());
					}
				}
				// if the response was empty, raise an event so anyone who cares to investigate can do so
				catch (RemoteJobMonitorEmptyResponseException e) {
					JobEvent event = new JobEvent(JobEventType.EMPTY_STATUS_RESPONSE.name(),
							JobEventType.EMPTY_STATUS_RESPONSE.getDescription(), job.getOwner());
					event.setJob(job);
					JobEventProcessor eventProcessor = new JobEventProcessor(event);
					eventProcessor.process();
				} catch (RemoteJobMonitorResponseParsingException e) {
					log.debug("Unrecognized response from status check for job " + this.job.getUuid() +
							": " + schedulerResponseText, e);
					updateJobStatus(job.getStatus(), this.job.getErrorMessage());
				}

				return this.job;
			} catch (JobException e) {
				// this is a db issue that cannot be worked around. If we can't update the db, then the monitor
				// will have to be retried by a process that can. Until then, we just exit out from here.
				log.error(e.getMessage());
				return this.job;
			} catch (ClosedByInterruptException | SystemUnavailableException | StaleObjectStateException | UnresolvableObjectException e) {
				throw e;
			} catch (Throwable e) {
				throw new RemoteJobMonitoringException("Failed to query status of job " + job.getUuid(), e);
			}
		}
	}

	/**
	 * Performs the remote call to make the job status check.
	 * @param command the command to run on the {@link ExecutionSystem}
	 * @return the response from the job check command.
	 * @throws RemoteJobMonitoringException if unable to fetch the condor log file content for any reason.
	 */
	public String getJobStatusResponse(String command) throws RemoteJobMonitoringException {
		RemoteSubmissionClient remoteSubmissionClient = null;

		try {
			remoteSubmissionClient = getRemoteSubmissionClient();

			log.debug("Forking command \"" + command + "\" on " + remoteSubmissionClient.getHost() + ":" +
					remoteSubmissionClient.getPort() + " for job " + job.getUuid());

			String schedulerResponseText = remoteSubmissionClient.runCommand(command);

			log.debug("Response for job " + job.getUuid() + " monitoring command was: " + schedulerResponseText);

			return schedulerResponseText;
		} catch (Exception e) {
			throw new RemoteJobMonitoringException("Failed to retrieve status information for job " + job.getUuid() +
					" from remote system.", e);
		} finally {
			if (remoteSubmissionClient != null)
				remoteSubmissionClient.close();
		}
	}
}

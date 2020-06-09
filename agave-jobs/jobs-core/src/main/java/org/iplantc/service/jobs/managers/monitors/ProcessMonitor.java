/**
 *
 */
package org.iplantc.service.jobs.managers.monitors;

import java.nio.channels.ClosedByInterruptException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitoringException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.jobs.managers.monitors.parsers.ForkJobStatus;
import org.iplantc.service.jobs.managers.monitors.parsers.ForkJobStatusResponseParser;
import org.iplantc.service.jobs.managers.monitors.parsers.JobMonitorResponseParserFactory;
import org.iplantc.service.jobs.managers.monitors.parsers.JobStatusResponseParser;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.LoginProtocolType;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteConnectionException;
import org.joda.time.DateTime;

/**
 * Monitors a cli job by checking for the process id. This is nearly exactly the
 * same process as the {@link DefaultJobMonitor} since the command to check id is abstracted in
 * the SchedulerType enumeration class.
 *
 * @see DefaultJobMonitor
 * @author dooley
 * @deprecated
 */
public class ProcessMonitor extends AbstractJobMonitor {
    private static final Logger log = Logger.getLogger(ProcessMonitor.class);

    public ProcessMonitor(Job job) {
        super(job);
    }

    @Override
    public Job monitor() throws RemoteJobMonitoringException, SystemUnavailableException, ClosedByInterruptException {
		// skip it all if the job is null
        if (getJob() == null) return null;

		// get the execution system from the getter to ensure we check availability before proceeding.
        ExecutionSystem executionSystem = getExecutionSystem();

        // if the execution system login config is local, then we cannot submit
        // jobs to this system remotely. In this case, a worker will be running
        // dedicated to that system and will submitting jobs locally. All workers
        // other that this should pass on accepting this job.
        if (executionSystem.getLoginConfig().getProtocol() == LoginProtocolType.LOCAL &&
                ! Settings.LOCAL_SYSTEM_ID.equals(this.job.getSystem())) {
            return this.job;
        } else {// otherwise, throw it in remotely
			try {
				// increment the number of checks and lastupdated timestamp
				this.job.setStatusChecks(this.job.getStatusChecks() + 1);
				updateJobStatus(this.job.getStatus(), this.job.getErrorMessage());

				checkStopped();

				String queryCommand = String.format("%s%s %s",
						getStartupScriptCommand(),
						executionSystem.getScheduler().getBatchQueryCommand(),
						job.getLocalJobId());

				String schedulerResponseText = getJobStatusResponse(queryCommand);


				try {

					JobStatusResponseParser parser = new JobMonitorResponseParserFactory().getInstance(this.job);

					try {
						JobStatusResponse response = parser.parse(this.job.getLocalJobId(), schedulerResponseText);

						// if the status is unchanged, nothing to do here
						if (response.getStatus() == job.getStatus()) {
							log.debug("Job " + job.getUuid() + " is still " + job.getStatus().name() +
									" as local job id " + job.getLocalJobId() + " on " + job.getSystem());
						} else if (response.getStatus() != null) {
							// if the response status is finished
							if (response.getRemoteSchedulerJobStatus().isFailureStatus()) {
								log.debug("Job " + job.getUuid() + " was found in a CLEANING_UP state on " + job.getSystem() +
										" based on its runtime log file. Updating status to CLEANING_UP.");

								updateJobStatus(JobStatusType.CLEANING_UP,
										"Job completion detected by Condor monitor.");

								if (!this.job.isArchiveOutput()) {
									log.debug("Job " + this.job.getUuid() + " will skip archiving at user request.");
									updateJobStatus(JobStatusType.FINISHED, "Job completed. Skipping archiving at user request.");
									log.debug("Job " + this.job.getUuid() + " finished.");
								}
							}
							updateStatusOfFinishedJob();
							updateJobStatus(job.getStatus(), this.job.getErrorMessage());
						} else if (response.getRemoteSchedulerJobStatus().isDoneStatus()) {
							updateStatusOfFinishedJob();
						} else {
							// still running
						}
						return job;
					} catch (RemoteJobMonitorEmptyResponseException e) {
						// empty response means we could not find any trace of the job id. For a process, this
						// means it is complete.
						updateStatusOfFinishedJob();
					} catch (RemoteJobMonitorResponseParsingException e) {
						log.debug("Unrecognized response from status check for job " + this.job.getUuid() + ": " + schedulerResponseText, e);
						updateJobStatus(job.getStatus(), this.job.getErrorMessage());
					}

				} catch (Exception e) {
					log.error("Failed to updated job " + this.job.getUuid() + " status to " + this.job.getStatus(), e);
				}

				return this.job;
			} catch (ClosedByInterruptException | RemoteJobMonitoringException | UnresolvableObjectException e) {
				throw e;
			} catch (Throwable e) {
				throw new RemoteJobMonitoringException("Failed to query status of job " + job.getUuid(), e);
			}
		}
    }

	/**
	 * Performs the remote call to make the process status check.
	 * @param command the command to run on the {@link ExecutionSystem}
	 * @return the response from the job check command.
	 * @throws RemoteJobMonitoringException if unable to fetch the condor log file content for any reason.
	 */
	public String getJobStatusResponse(String command) throws RemoteJobMonitoringException {

		try (RemoteSubmissionClient remoteSubmissionClient = getRemoteSubmissionClient()) {

			log.debug("Forking command " + command + " on " + remoteSubmissionClient.getHost() + ":" +
					remoteSubmissionClient.getPort() + " for job " + job.getUuid());

			String schedulerResponseText = remoteSubmissionClient.runCommand(command);

			log.debug("Response for job " + job.getUuid() + " monitoring command was: " + schedulerResponseText);

			return schedulerResponseText;
		} catch (Exception e) {
			throw new RemoteJobMonitoringException("Failed to retrieve status information for job " + job.getUuid() +
					" from remote system.", e);
		}
	}

}

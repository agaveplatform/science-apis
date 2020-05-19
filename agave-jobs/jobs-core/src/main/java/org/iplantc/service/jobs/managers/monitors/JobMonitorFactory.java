/**
 * 
 */
package org.iplantc.service.jobs.managers.monitors;

import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.monitors.parsers.ForkJobStatusResponseParser;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;

import static org.iplantc.service.systems.model.enumerations.ExecutionType.CONDOR;

/**
 * Factory class to get {@link JobMonitor} instance based on the job {@link ExecutionType}.
 *
 * @see DefaultJobMonitor
 * @see CondorJobMonitor
 * 
 * @author dooley
 *
 */
public class JobMonitorFactory {

	protected JobManager jobManager = null;

	/**
	 * Basic getter for job manager instance. Useful for testing
	 * @return JobManager instance
	 */
	protected JobManager getJobManager() {
		if (jobManager == null) {
			jobManager = new JobManager();
		}

		return jobManager;
	}

	/**
	 * Gets an instance of a {@link JobMonitor} concrete class based on the job {@link ExecutionType}.
	 *
	 * @param job the job for which to instantiate the {@link JobMonitor}
	 * @return a job monitor capable of monitoring the remote job
	 * @throws SystemUnavailableException if the remote system is not available at this time.
	 */
	public JobMonitor getInstance(Job job) throws SystemUnavailableException
	{
		ExecutionSystem executionSystem = getJobManager().getJobExecutionSystem(job);

		switch (executionSystem.getExecutionType()) {
		case CONDOR:
			return new CondorJobMonitor(job);
		default:
			return new DefaultJobMonitor(job);
		}
	}

}

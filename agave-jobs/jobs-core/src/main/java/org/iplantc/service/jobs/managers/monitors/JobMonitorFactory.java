/**
 * 
 */
package org.iplantc.service.jobs.managers.monitors;

import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.enumerations.ExecutionType;

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
	 * Gets an instance of a {@link JobMonitor} concrete class based on the job {@link ExecutionType}.
	 *
	 * @param job the job for which to instantiate the {@link JobMonitor}
	 * @return a job monitor capable of monitoring the remote job
	 */
	public JobMonitor getInstance(Job job) {
		if (job.getExecutionType() == CONDOR) {
			return new CondorJobMonitor(job);
		} else {
			return new DefaultJobMonitor(job);
		}
	}

}

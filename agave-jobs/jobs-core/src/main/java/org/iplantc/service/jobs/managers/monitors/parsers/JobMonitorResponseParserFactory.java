/**
 * 
 */
package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.apps.exceptions.SoftwareException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;

/**
 * Factory class to get the appropraite {@link JobStatusResponseParser} for a given {@link Job}
 * 
 * @author dooley
 *
 */
public class JobMonitorResponseParserFactory {

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
	 * Wrapper for call to {@link JobManager#getJobExecutionSystem(Job)} for easier testing.
	 * @param job the job for which to return the {@link ExecutionSystem}
	 * @return the execution system
	 * @throws SystemUnavailableException when the system is not available
	 */
	protected ExecutionSystem getExecutionSystem(Job job) throws SystemUnavailableException {
		return getJobManager().getJobExecutionSystem(job);
	}

	/**
	 * Wrapper for call to {@link JobManager#getJobSoftware(Job)} for easier testing.
	 * @param job the job for which to return the {@link ExecutionSystem}
	 * @return the job software
	 * @throws SoftwareException if unable to query for software
	 */
	protected Software getSoftware(Job job) throws SoftwareException {
		return getJobManager().getJobSoftware(job);
	}

	public JobStatusResponseParser getInstance(Job job) throws SystemUnknownException, SystemUnavailableException {
//		ExecutionSystem executionSystem = getExecutionSystem(job);

//
//		Software software = null;
//		try {
//			software = getSoftware(job);
//		} catch (SoftwareException ignored) {}

//		// if the app has been deleted or is unavailable, we can still complete the monitoring task, but we'll have to
//		// assume the job is using the default scheduler type of the system.
//		ExecutionType executionType =
//				software == null ? executionSystem.getExecutionType() : software.getExecutionType();

		// now instantiate the parser for the correct execution and scheduler combination
		JobStatusResponseParser parser = null;
		switch (job.getExecutionType()) {
		case CONDOR:
			parser = new CondorJobStatusResponseParser();
			break;
		case HPC:
			switch (job.getSchedulerType()) {
				case SLURM:
				case CUSTOM_SLURM:
					parser = new SlurmJobStatusResponseParser();
					break;
				case PBS:
				case CUSTOM_PBS:
					parser = new PBSJobStatusResponseParser();
					break;
				case MOAB:
				case CUSTOM_MOAB:
				case TORQUE:
				case CUSTOM_TORQUE:
					parser = new TorqueJobStatusResponseParser();
					break;
				case SGE:
				case CUSTOM_GRIDENGINE:
					parser = new SGEJobStatusResponseParser();
					break;
				case LSF:
				case CUSTOM_LSF:
					parser = new LSFJobStatusResponseParser();
					break;
				case LOADLEVELER:
				case CUSTOM_LOADLEVELER:
					parser = new LoadLevelerJobStatusResponseParser();
					break;
				default:
					parser = new DefaultJsonStatusResponseParser();
					break;
			}
			break;
		case CLI:
		default:
			parser = new ForkJobStatusResponseParser();
			break;
		}
		
		return parser;
	}
}

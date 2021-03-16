package org.iplantc.service.jobs.managers;

import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.QuotaViolationException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;

import java.util.concurrent.ExecutorService;

public class JobQuotaCheck implements QuotaCheck {
	
	private Job job;
	private ExecutorService executionSystem;
	private JobManager jobManager;
	
	public JobQuotaCheck() {}

	/**
	 * Identifies the {@link BatchQueue} to use in the quota calculation by looking at the queue named in the job,
	 * then falling back to the {@link ExecutionSystem#getDefaultQueue()} if present.
	 * @param job the job for which the queue is requested
	 * @param executionSystem the job execution system
	 * @return job queue, defalt system queue, or null if not found
	 */
	protected BatchQueue getJobQueue(Job job, ExecutionSystem executionSystem) {
		BatchQueue jobQueue = executionSystem.getQueue(job.getBatchQueue());
		if ( jobQueue == null ) {
			if (executionSystem.getDefaultQueue() == null) {
				jobQueue = new BatchQueue("default", 10L, 4.0);
			}  else {
				jobQueue = executionSystem.getDefaultQueue();
			}
		}

		return jobQueue;
	}

	@Override
	public void check(Job job, ExecutionSystem executionSystem) throws QuotaViolationException, JobException
	{
		BatchQueue jobQueue = getJobQueue(job, executionSystem);
        
        // todo end hack for queueless condor
//        if ( jobQueue == null ) {
//        	if (system.getDefaultQueue() == null && system.getScheduler() == SchedulerType.CONDOR) {
//        		jobQueue = new BatchQueue("default", new Long(10), new Double(4));
//        	} else {
//        		
//        	}
//        } 
        
		// if the job queue is not unbounded, ensure it doesn't violate the system limits
        if (jobQueue.getMaxJobs() != -1) {
			// verify the system is not at capacity
			if (executionSystem.getMaxSystemJobs() > 0 && JobDao.countActiveJobsOnSystem(job.getSystem()) >= executionSystem.getMaxSystemJobs()) {
				throw new QuotaViolationException(job.getSystem() + " is currently at capacity for new jobs.");
			}
			// verify the system queue is not at capacity
			else if (jobQueue.getMaxJobs() > 0 && JobDao.countActiveJobsOnSystemQueue(job.getSystem(), job.getBatchQueue()) >= jobQueue.getMaxJobs()) {
				throw new QuotaViolationException("System " + executionSystem.getSystemId() + " is currently at maximum capacity for "
						+ "concurrent active jobs.");
			}
			// verify the user is not at system capacity
			else if (executionSystem.getMaxSystemJobsPerUser() > 0 && JobDao.countActiveUserJobsOnSystem(job.getOwner(), job.getSystem()) >= executionSystem.getMaxSystemJobsPerUser()) {
				throw new QuotaViolationException("User " + job.getOwner() + " has reached their quota for "
						+ "concurrent active jobs on " + executionSystem.getSystemId());
			}
			// verify the user is not at queue capacity
			else if (jobQueue.getMaxUserJobs() > 0 && JobDao.countActiveUserJobsOnSystemQueue(job.getOwner(), job.getSystem(), job.getBatchQueue()) >= jobQueue.getMaxUserJobs()) {
				throw new QuotaViolationException("User " + job.getOwner() + " has reached their quota for "
						+ "concurrent active jobs on the " + jobQueue.getName() + " queue of " + executionSystem.getSystemId());
			}
		}
	}
}

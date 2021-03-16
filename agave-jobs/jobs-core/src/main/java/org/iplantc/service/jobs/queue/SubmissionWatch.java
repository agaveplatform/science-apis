package org.iplantc.service.jobs.queue;

import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.apps.exceptions.UnknownSoftwareException;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobDependencyException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.QuotaViolationException;
import org.iplantc.service.jobs.exceptions.SchedulerException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.JobQuotaCheck;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.queue.actions.SubmissionAction;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.LoginProtocolType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.joda.time.DateTime;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionException;

/**
 * Class to pull a job from the db queue and attempt to submit it to iplant
 * resources using one of the appropriate execution factory instances.
 * 
 * @author dooley
 * 
 */
@DisallowConcurrentExecution
public class SubmissionWatch extends AbstractJobWatch
{
	private static final Logger	log	= Logger.getLogger(SubmissionWatch.class);
	
	public SubmissionWatch() {}

	public SubmissionWatch(boolean allowFailure) {
        super(allowFailure);
    }
	
	public void doExecute() throws JobExecutionException
	{
		ExecutionSystem executionSystem = null;
		// pull the oldest job with JobStatusType.PENDING from the db and submit
		// it to the remote scheduler.
		try
		{
			// verify the user is within quota to run the job before staging the data.
			// this should have been caught by the original job selection, but could change
			// due to high concurrency.
			try {
				executionSystem = getJobExecutionSystem();

				JobQuotaCheck quotaValidator = new JobQuotaCheck();
				quotaValidator.check(getJob(), executionSystem);
			} catch (QuotaViolationException e) {
				log.debug("Remote execution of job " + getJob().getUuid() + " is currently paused due to quota restrictions. " + e.getMessage());
				updateJobStatus(JobStatusType.STAGED,
					"Remote execution of job " + getJob().getUuid() + " is currently paused due to quota restrictions. " +
					e.getMessage() + ". This job will resume staging once one or more current jobs complete.");
				throw new JobExecutionException(e);
			} catch (Throwable e) {
				log.error("Failed to verify user quota for job " + getJob().getUuid() +
						". Job will be returned to queue and retried later.", e);
				getJob().setRetries(job.getRetries()+1);
				updateJobStatus(JobStatusType.STAGED,
						"Failed to verify user quota for job " + job.getUuid() +
						". Job will be returned to queue and retried later.");
				throw new JobExecutionException(e);
			}
			
			// kill jobs past their max lifetime
			Instant jobSubmissionDeadline = getJob().getSubmitTime().toInstant();
			
			if (jobSubmissionDeadline.plus(30, ChronoUnit.DAYS).isBefore(Instant.now()))
			{
				log.debug("Terminating job " + getJob().getUuid() +
						" after 30 days of attempting to submit.");
				updateJobStatus(JobStatusType.KILLED,
					"Killing job after 30 days of attempting to submit.");
				updateJobStatus(JobStatusType.FAILED,
						"Job did not submit within 30 days. Job cancelled.");
				return;
			} 

			// if the execution system login config is local, then we cannot submit
			// jobs to this system remotely. In this case, a worker will be running
			// dedicated to that system and will submitting jobs locally. All workers
			// other that this will should pass on accepting this job.
			if (executionSystem.getLoginConfig().getProtocol() == LoginProtocolType.LOCAL && 
					StringUtils.equals(Settings.LOCAL_SYSTEM_ID, job.getSystem()))
			{
				return;
			}
			else // otherwise, submit to the remote system
			{
				// mark the job as submitting so no other process claims it
				// note: we should have jpa optimistic locking enabled, so
				// no race conditions should exist at this point.
			    updateJobStatus(JobStatusType.SUBMITTING,
						"Preparing job for submission.");
				
				if (isStopped()) {
                    throw new ClosedByInterruptException();
                }
                
				setWorkerAction(new SubmissionAction(getJob()));
				
				// wrap this in a try/catch so we can update the local reference to the
				// job before handling the exception
				try {
					getWorkerAction().run();
				} finally {
				    setJob(getWorkerAction().getJob());
				}
                
                if (!isStopped() ||
						List.of(JobStatusType.RUNNING, JobStatusType.QUEUED).contains(getJob().getStatus())) {
                    getJob().setRetries(0);
                    JobDao.persist(getJob());
                }
			}
		}
		catch (JobException e) {
			try {
				if (e.getCause() instanceof UnknownSoftwareException) {
					log.debug("Submission task for job " + getJob().getUuid() + " aborted due to unknown software id " + getJob().getSoftwareName());

					updateJobStatus(JobStatusType.FAILED,
							"Submission failed due to the app associated with this job, "
									+ ", " + getJob().getSoftwareName() + ", having been deleted. No further "
									+ "action can be taken for this getJob(). The job will be terminated immediately.");
					throw new JobExecutionException("Staging task for job " + getJob().getUuid() + " aborted due to missing app.");
				} else {
					String message = "Failed to stage input data for job " + getJob().getUuid() + ". " + e.getMessage();
					log.error(message, e);
					updateJobStatus(JobStatusType.FAILED, message);
					throw new JobExecutionException(e);
				}
			} catch (Throwable t) {
				log.error("Failed to roll back job " + getJob().getUuid() + " status when staging task was aborted due to missing app.", t);
			}
		}
		catch (JobExecutionException e) {
			throw e;
		}
		catch (JobDependencyException e) {
			log.error("Failed to submit  for job " + getJob().getUuid(), e);
			updateJobStatus(JobStatusType.FAILED, e.getMessage());
			throw new JobExecutionException(e);

			log.debug("Submission task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");

			try {
				updateJobStatus(JobStatusType.PENDING,
						"Job staging reset due to worker shutdown. Staging will resume in another worker automatically.");
			} catch (Throwable t) {}
//				log.error("Failed to roll back job " + getJob().getUuid() + " status when staging task was interrupted.", t);
//			}
			throw new JobExecutionException("Staging task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");
		}
		catch (SystemUnknownException e) {
			log.debug("System for job " + getJob().getUuid() + " is missing. " + e.getMessage());
			updateJobStatus(JobStatusType.FAILED,"Submission failed due to missing execution system, " +
					getJob().getSystem() + ". No further action can be taken for this job. " +
					"The job will be terminated immediately. ");
			throw new JobExecutionException(e);
		}
		catch (SystemUnavailableException e) {
			log.debug("System for job " + getJob().getUuid() + " is currently unavailable. " + e.getMessage());
			updateJobStatus(JobStatusType.STAGED, "Remote execution of job " + getJob().getUuid() +
					" is currently paused waiting for execution system " + getJob().getSystem() +
					" to become available. If the system becomes available again within 30 days, this job " +
					"will resume staging. After 30 days it will be killed.");
			throw new JobExecutionException(e);
		}
		catch (ClosedByInterruptException e) {
            log.debug("Submission task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");
            try {
            	updateJobStatus(JobStatusType.STAGED,
                    "Job submission aborted due to worker shutdown. Job will be resubmitted automatically.");
//                JobDao.persist(job);
            } catch (Throwable t) {
                log.error("Failed to roll back job status when archive task was interrupted.", e);
            }
            throw new JobExecutionException("Submission task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");
        }
        catch (StaleObjectStateException | UnresolvableObjectException e) {
			log.debug("Job " + getJob().getUuid() + " already being processed by another submission thread. Ignoring.");
			throw new JobExecutionException("Job " + getJob().getUuid() + " already being processed by another submission thread. Ignoring.");
		}
//		catch (Throwable e) {
//			if (job == null) {
//				log.error("Failed to retrieve job information from db", e);
//			} else {
//				try {
//					log.error("Failed to submit job " + getJob().getUuid() + " due to internal errors. " + e.getMessage());
//					updateJobStatus(JobStatusType.FAILED,
//							"Failed to submit job " + getJob().getUuid() + " due to internal errors");
//				} catch (Throwable ignored) {}
//			}
//			throw new JobExecutionException(e);
//		}
		finally {
		    setTaskComplete(true);
            try { HibernateUtil.flush(); } catch (Exception ignored) {}
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
            try { HibernateUtil.disconnectSession(); } catch (Exception ignored) {}
		}
	}

	/* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.AbstractJobWatch#rollbackStatus()
     */
    @Override
    protected void rollbackStatus()
    {
        try 
        {
            if (getJob().getStatus() != JobStatusType.QUEUED && getJob().getStatus() != JobStatusType.RUNNING) {
                JobManager.updateStatus(getJob(), JobStatusType.STAGED, 
                        "Job submission reset due to worker shutdown. Staging will resume in another worker automatically.");
            }
        } catch (Throwable t) {
            log.error("Failed to roll back status of job " + 
                    getJob().getUuid() + " to STAGED upon worker failure.", t);
        }
    }
    
    @Override
    public String selectNextAvailableJob() throws JobException, SchedulerException {
        
        return JobDao.getNextQueuedJobUuid(JobStatusType.STAGED, 
                TenancyHelper.getDedicatedTenantIdForThisService(),
                org.iplantc.service.common.Settings.getDedicatedUsernamesFromServiceProperties(),
                org.iplantc.service.common.Settings.getDedicatedSystemIdsFromServiceProperties());
    }
}

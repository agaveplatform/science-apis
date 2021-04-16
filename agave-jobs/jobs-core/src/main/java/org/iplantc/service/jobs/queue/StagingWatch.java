package org.iplantc.service.jobs.queue;

import org.apache.commons.lang.StringUtils;
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
import org.iplantc.service.jobs.queue.actions.StagingAction;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionException;

import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Class to pull a job from the db queue and attempt to stage any input files to the
 * {@link org.iplantc.service.jobs.model.Job#getWorkPath()} of the {@link ExecutionSystem}. Overall staging will
 * be retried {@link Settings#MAX_SUBMISSION_RETRIES} times unless a non-recoverable exception related to a missing
 * job dependency occurs, in which case, the job will be killed.
 *
 * @author dooley
 */
@DisallowConcurrentExecution
public class StagingWatch extends AbstractJobWatch {
    private static final Logger log = Logger.getLogger(StagingWatch.class);
    private JobManager jobManager;

    public StagingWatch() {
    }

    public StagingWatch(boolean allowFailure) {
        super(allowFailure);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.WorkerWatch#selectNextAvailableJob()
     */
    @Override
    public String selectNextAvailableJob() throws JobException, SchedulerException {

        return JobDao.getNextQueuedJobUuid(JobStatusType.PENDING,
                TenancyHelper.getDedicatedTenantIdForThisService(),
                org.iplantc.service.common.Settings.getDedicatedUsernamesFromServiceProperties(),
                org.iplantc.service.common.Settings.getDedicatedSystemIdsFromServiceProperties());
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.WorkerWatch#doExecute()
     */
    public void doExecute() throws JobExecutionException {
        ExecutionSystem executionSystem = null;
        try {
            // verify the user is within quota to run the job before staging the data.
            // this should have been caught by the original job selection, but could change
            // due to high concurrency.
            try {
                executionSystem = getJobExecutionSystem();

                JobQuotaCheck quotaValidator = new JobQuotaCheck();
                quotaValidator.check(getJob(), executionSystem);
            } catch (SystemUnknownException e) {
                log.debug("System for job " + getJob().getUuid() + " is missing. " + e.getMessage());
                updateJobStatus(JobStatusType.FAILED,
                        "Job failed while staging inputs due to missing execution system.");
                throw new JobExecutionException(e);
            } catch (SystemUnavailableException e) {
                log.debug("System for job " + getJob().getUuid() + " is currently unavailable. " + e.getMessage());
                updateJobStatus(JobStatusType.PENDING,
                        "Input staging is currently paused waiting for the execution system " +
                                "to become available. If the system becomes available " +
                                "again within 7 days, this job " +
                                "will resume staging. After 7 days it will be killed.");
                throw new JobExecutionException(e);
            } catch (QuotaViolationException e) {
                log.debug("Input staging for job " + getJob().getUuid() + " is currently paused due to quota restrictions. " + e.getMessage());
                updateJobStatus(JobStatusType.PENDING,
                        "Input staging for job is currently paused due to quota restrictions. " +
                                e.getMessage() + ". This job will resume staging once one or more current jobs complete.");
                throw new JobExecutionException(e);
            } catch (Throwable e) {
                log.error("Failed to verify user quota for job " + getJob().getUuid() +
                        ". Job will be returned to queue and retried later.", e);
                getJob().setRetries(getJob().getRetries()+1);
                updateJobStatus(JobStatusType.STAGED,
                        "Failed to verify user quota for job " + getJob().getUuid() +
                                ". Job will be returned to queue and retried later.");
                throw new JobExecutionException(e);
            }

            // kill jobs past their max staging deadline
            Instant jobCreatedTime = getJob().getCreated().toInstant();

            // we will only retry for 7 days
            if (jobCreatedTime.plus(7, ChronoUnit.DAYS).isBefore(Instant.now())) {
                log.debug("Terminating job " + getJob().getUuid() + " after 7 days of attempting to stage inputs.");
                updateJobStatus(JobStatusType.KILLED,
                        "Terminating job from after 7 days attempting to stage inputs.");
                updateJobStatus(JobStatusType.FAILED,
                        "Unable to stage inputs for job after 7 days. Job cancelled.");
            }

            // if the execution system for this job has a local storage config,
            // all other transfer workers will pass on it.
            if (!StringUtils.equals(Settings.LOCAL_SYSTEM_ID, getJob().getSystem()) &&
                    executionSystem.getStorageConfig().getProtocol() == StorageProtocolType.LOCAL) {
                // this instance of the staging worker is not configured to handle staging of data for this job,
                // we pass on the job, allowing another worker to handle it.
            } else {
                int attempts = 0;
                boolean staged = false;

                // if the job doesn't need to be staged, just move on with things.
                if (JobManager.getJobInputMap(getJob()).isEmpty()) {
                    updateJobStatus(JobStatusType.STAGED,
                            "Skipping staging. No input data associated with this job.");
                }
                // otherwise, attempt to stage the job Settings.MAX_SUBMISSION_RETRIES times
                else {
                    while (!staged && !isStopped() && attempts <= Settings.MAX_SUBMISSION_RETRIES) {
                        attempts++;

                        getJob().setRetries(attempts - 1);

                        log.debug("Attempt " + attempts + " to stage job " + getJob().getUuid() + " inputs");

                        // mark the job as submitting so no other process claims it
                        updateJobStatus(JobStatusType.PROCESSING_INPUTS,
                                "Attempt " + attempts + " to stage job inputs");

                        try {
                            if (isStopped()) {
                                throw new ClosedByInterruptException();
                            }

                            setWorkerAction(new StagingAction(getJob()));

                            try {
                                // wrap this in a try/catch so we can update the local reference to the
                                // job before it hist
                                getWorkerAction().run();
                            } finally {
                                setJob(getWorkerAction().getJob());
                            }

                            // if the task was not stopped, or it was stopped, but after the inputs were all staged,
                            // then we can mark the job as staged and move on.
                            if (!isStopped() || getJob().getStatus() == JobStatusType.STAGED) {
                                staged = true;
                                getJob().setRetries(0);
                                JobDao.persist(getJob());
                            } else {
                                // otherwise, one or more inputs failed to stage. The job will go back into the queue
                                // to get picked up by some other task.
                            }
                        } catch (ClosedByInterruptException e) {
                            log.debug("Job input staging cancelled by outside process for job " + getJob().getUuid());
                            throw e;
                        } catch (SystemUnavailableException e) {
                            log.debug(e.getMessage());
                            updateJobStatus(JobStatusType.PENDING,
                                    "Input staging is currently paused waiting for a system containing " +
                                            "input data to become available. If the system becomes available " +
                                            "again within 7 days, this job " +
                                            "will resume staging. After 7 days it will be killed.");
                            throw new JobExecutionException(e);
                        } catch (JobDependencyException e) {
                            log.error("Failed to stage inputs for job " + getJob().getUuid() + ". " + e.getMessage(), e);
                            updateJobStatus(JobStatusType.FAILED, e.getMessage());
                            throw new JobExecutionException(e);
                        } catch (JobException e) {
                            if (attempts >= Settings.MAX_SUBMISSION_RETRIES) {
                                log.error("Failed to stage job " + getJob().getUuid() +
                                        " inputs after " + attempts + " attempts.", e);

                                updateJobStatus(JobStatusType.STAGING_INPUTS, "Attempt "
                                        + attempts + " failed to stage job inputs. " + e.getMessage());
                                try {
                                    setJob(getJobManager().deleteStagedData(getJob()));
                                } catch (Throwable t) {
                                    log.error("Failed to remove remote work directory for job " + getJob().getUuid(), t);
                                    updateJobStatus(JobStatusType.FAILED,
                                            "Failed to remove remote work directory.");
                                }

                                log.error("Unable to stage inputs for job " + getJob().getUuid() +
                                        " after " + attempts + " attempts. Job cancelled.");
                                updateJobStatus(JobStatusType.FAILED,
                                        "Unable to stage inputs for job" +
                                                " after " + attempts + " attempts. Job cancelled.");

                                throw new JobExecutionException(e);
                            } else {
                                updateJobStatus(JobStatusType.PENDING, "Attempt "
                                        + attempts + " failed to stage job inputs. " + e.getMessage());
                            }
                        } catch (Exception e) {
                            log.error("Failed to stage inputs for job " + getJob().getUuid(), e);
                            updateJobStatus(JobStatusType.FAILED,
                                    "Failed to stage file due to unexpected error.");
                            throw new JobExecutionException(e);
                        }
                    }
                }
            }
        } catch (JobExecutionException e) {
            throw e;
        } catch (ClosedByInterruptException e) {
            log.debug("Staging task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");

            try {
                updateJobStatus(JobStatusType.PENDING,
                        "Job staging reset due to worker shutdown. Staging will resume in another worker automatically.");
            } catch (Throwable t) {
                log.error("Failed to roll back job " + getJob().getUuid() + " status when staging task was interrupted.", t);
            }
            throw new JobExecutionException("Staging task for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");
        } catch (StaleObjectStateException | UnresolvableObjectException e) {
            log.debug("Job " + getJob().getUuid() + " already being processed by another staging thread. Ignoring.");
            throw new JobExecutionException("Job " + getJob().getUuid() + " already being processed by another staging thread. Ignoring.", e);
        } catch (JobException e) {
            try {
                if (e.getCause() instanceof UnknownSoftwareException) {
                    log.debug("Staging task for job " + getJob().getUuid() + " aborted due to unknown software id " + getJob().getSoftwareName());

                    updateJobStatus(JobStatusType.FAILED,
                            "Input staging failed due to the app associated with this job, "
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
        } catch (Throwable e) {
            if (e.getCause() instanceof StaleObjectStateException) {
                log.debug("Job " + getJob().getUuid() + " already being processed by another staging thread. Ignoring.");
                throw new JobExecutionException("Job " + getJob().getUuid() + " already being processed by another staging thread. Ignoring.");
            } else {
                String message = "Failed to stage input data for job " + getJob().getUuid();
                log.error(message, e);
                try {
                    updateJobStatus(JobStatusType.FAILED, message);
                } catch (Throwable ignored) {
                }
                throw new JobExecutionException(e);
            }
        } finally {
            taskComplete.set(true);
            try {
                HibernateUtil.flush();
            } catch (Exception ignored) {
            }
            try {
                HibernateUtil.commitTransaction();
            } catch (Exception ignored) {
            }
            try {
                HibernateUtil.disconnectSession();
            } catch (Exception ignored) {
            }
        }
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.AbstractJobWatch#rollbackStatus()
     */
    @Override
    protected void rollbackStatus() {
        try {
            HibernateUtil.closeSession();
        } catch (Exception ignored) {
        }
        ;
        try {
            if (getJob().getStatus() != JobStatusType.STAGED) {
                job = JobDao.getById(getJob().getId());
                getJob().setStatus(JobStatusType.PENDING,
                        "Job input staging reset due to worker shutdown. Staging will resume in another worker automatically.");
                JobDao.persist(job);
            }
        } catch (Throwable e) {
            log.error("Failed to roll back status of job " +
                    getJob().getUuid() + " to PENDING upon worker failure.", e);
        }
    }

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

}

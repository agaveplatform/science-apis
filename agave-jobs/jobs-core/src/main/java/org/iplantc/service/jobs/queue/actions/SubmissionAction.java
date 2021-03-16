/**
 * 
 */
package org.iplantc.service.jobs.queue.actions;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.apps.exceptions.UnknownSoftwareException;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobDependencyException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.SchedulerException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.launchers.JobLauncher;
import org.iplantc.service.jobs.managers.launchers.JobLauncherFactory;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.ClosedByInterruptException;

/**
 * Handles launching of job on an {@link org.iplantc.service.systems.model.ExecutionSystem}. The general process is:
 * <ol>
 *     <li></li>
 * </ol>
 *
 * @author dooley
 *
 */
public class SubmissionAction extends AbstractWorkerAction {
    
    private static Logger log = Logger.getLogger(SubmissionAction.class);
    
    private JobLauncher jobLauncher = null;

    protected JobManager jobManager = null;

    public SubmissionAction(Job job) {
        super(job);
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

    @Override
    public synchronized void setStopped(boolean stopped) {
        super.setStopped(stopped);
        
        if (getJobLauncher() != null) {
            getJobLauncher().setStopped(true);
        }
    }
    
    /**
     * This method attempts to stage application assets and launch a job on the remote {@link ExecutionSystem}.
     * If submission is not possible, the job is failed and the remote job work directory is removed.
     *
     * @throws SystemUnavailableException
     * @throws SystemUnknownException
     * @throws JobException
     * @throws JobDependencyException 
     */
    public void run() throws SystemUnavailableException, SystemUnknownException, JobException,
            ClosedByInterruptException, JobDependencyException
    {
        boolean submitted = false;
        
        int attempts = getJob().getRetries();
        
        try 
        {
            while (!submitted && attempts <= Settings.MAX_SUBMISSION_RETRIES)
            {
                checkStopped();
                
                getJob().setRetries(attempts);
                
                attempts++;
                
                log.debug("Attempt " + attempts + " to submit job " + getJob().getUuid() +
                          " (max retries = " + Settings.MAX_SUBMISSION_RETRIES + ").");
                
                updateJobStatus(JobStatusType.SUBMITTING, "Attempt " + attempts + " to submit job");
                
                try 
                {
                    setJobLauncher(new JobLauncherFactory().getInstance(getJob()));
                    
                    getJobLauncher().launch();
                    
                    submitted = true;
                    
                    log.info("Successfully submitted job " + getJob().getUuid() + " to " + getJob().getSystem());
                }
                catch (ClosedByInterruptException e) {
                    log.debug("Job submission cancelled by outside process for job " + getJob().getUuid());
                    throw e;
                } 
                catch (UnknownSoftwareException|SystemUnknownException e) {
                    log.debug("Failed to submit job " + getJob().getUuid() + " on " + getJob().getSystem() +
                                    ". " + e.getMessage());
                    updateJobStatus(JobStatusType.FAILED, "Unable to submit job " + getJob().getUuid() + ". " +
                            e.getMessage() + " No further attempts will be made.");
                    throw new JobDependencyException(e);
                }
                catch (SystemUnavailableException e) {
                    log.debug("One or more dependent systems for job " + getJob().getUuid() + " is currently unavailable. " + e.getMessage());
                    updateJobStatus(JobStatusType.STAGED,
                        "Remote execution of  job " + getJob().getUuid() + " is currently paused waiting for " + getJob().getSystem() +
                        " to become available. If the system becomes available again within 7 days, this job " +
                        "will resume staging. After 7 days it will be killed. Original error message: " + e.getMessage());
                    break;
                }
                catch (SoftwareUnavailableException e) {
                    log.debug("Software for job " + getJob().getUuid() + " is currently unavailable. " + e.getMessage());
                    updateJobStatus(JobStatusType.STAGED,
                            "Remote execution of  job " + getJob().getUuid() + " is currently paused waiting for app " + getJob().getSoftwareName() +
                                    " to become available. If the app becomes available again within 30 days of job creation, this job " +
                                    "will resume staging. After that time, it will be killed.");
                    break;
                }
                catch (SchedulerException e) {
                    log.error("Failed to submit job " + getJob().getUuid() + " on " + getJob().getSystem() +
                            " due to scheduler exception: " + e.getMessage());
                    updateJobStatus(getJob().getStatus(), "Attempt "
                        + attempts + " failed to submit job due to scheduler exception. " + e.getMessage());
                }
                catch (IOException e) {
                    log.debug("Failed to submit job " + getJob().getUuid() +
                            ". Unable to connect to remote system " + e.getMessage());
                    updateJobStatus(getJob().getStatus(), e.getMessage() +
                            " The service was unable to connect to the target execution system " +
                            "for this application, " + getJob().getSystem() + ". This job will " +
                            "remain in queue until the system becomes available. Original error message: " + e.getMessage());
                }
                catch (JobException e) {

                }
                catch (Exception e)
                {   
                    if (e.getCause() instanceof UnresolvableObjectException ||
                            e.getCause() instanceof ObjectNotFoundException) {
                        log.error("Race condition was just avoided for job " + getJob().getUuid(), e);

                    }
                    else if (attempts >= Settings.MAX_SUBMISSION_RETRIES ) 
                    {
                        log.error("Failed to submit job " + getJob().getUuid() +
                            " after " + attempts + " attempts.", e);
                        updateJobStatus(getJob().getStatus(), "Attempt "
                                + attempts + " failed to submit job. " + e.getCause().getMessage());

                        try {
                            if (getJob().isArchived()) {
                                setJob(getJobManager().deleteStagedData(job));
                            }
                        } catch (Exception e1) {
                            log.error("Failed to remove remote work directory for job " + getJob().getUuid(), e1);
                            updateJobStatus(getJob().getStatus(),
                                    "Failed to remove remote work directory.");
                        }
                        
                        log.error("Unable to submit job " + getJob().getUuid() +
                                " after " + attempts + " attempts. Job cancelled.");
                        updateJobStatus(JobStatusType.FAILED,
                                "Unable to submit job after " + attempts + " attempts. Job cancelled.");
                        break;
                    } 
                    else 
                    {
                        try 
                        {
                            String msg = "Attempt " + attempts + " failed to submit job. " + e.getCause().getMessage();
                            log.info(msg, e);
                            updateJobStatus(getJob().getStatus(), msg);
                        }
                        catch (Exception e1) {
                            log.error("Failed to update job " + getJob().getUuid() + " status to " + getJob().getStatus(), e1);
                        }
                    }
                }
            }
        }
        finally
        {
            // clean up the job directory now that we're either done or failed
            if (getJobLauncher() != null) {
                FileUtils.deleteQuietly(getJobLauncher().getTempAppDir());
            }
        }
    }

    public synchronized JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    public synchronized void setJobLauncher(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
    }
    
    /**
     * Returns the current job refernce. Since the launcher will update the
     * job status as it goes and invalidate the current referenced entity,
     * this method will use that value if present, otherwise it will use its
     * own.  (This, of course, is ridiculous.)
     * 
     * @return current valid job reference
     */
    @Override
    public synchronized Job getJob() {
        if (getJobLauncher() == null) {
            return this.job;
        } else {
            return getJobLauncher().getJob();
        }
    }
}

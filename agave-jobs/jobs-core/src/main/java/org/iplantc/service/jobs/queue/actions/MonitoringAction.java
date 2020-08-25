/**
 * 
 */
package org.iplantc.service.jobs.queue.actions;

import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.hibernate.UnresolvableObjectException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.monitors.AbstractJobMonitor;
import org.iplantc.service.jobs.managers.monitors.JobMonitor;
import org.iplantc.service.jobs.managers.monitors.JobMonitorFactory;
import org.iplantc.service.jobs.managers.monitors.parsers.JobStatusResponseParser;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.nio.channels.ClosedByInterruptException;

/**
 * Each {@link SchedulerType} has an associated {@link JobStatusResponseParser} implementation that handles
 * the parsing of the response from querying the remote system for information. The actual remote job status
 * check is made by an {@link JobMonitor} implementation. Most implementations will use or extend the
 * {@link AbstractJobMonitor} class, just implementing {@link JobMonitor#getJobStatusResponse(String)}.
 *
 *
 * @author dooley
 *
 */
public class MonitoringAction extends AbstractWorkerAction {
    
    private static Logger log = Logger.getLogger(MonitoringAction.class);
    
    private JobMonitor jobMonitor = null;

    public MonitoringAction(Job job) {
        super(job);
    }

    /**
     * This method attempts check the status of the remote job/process/service represented by an {@link Job}.
     *
     * @throws SystemUnavailableException if the remote system is not available
     * @throws JobException if the job monitor cannot be processed
     */
    public void run() throws SystemUnavailableException, JobException, ClosedByInterruptException
    {
        try 
        {
            setJobMonitor(new JobMonitorFactory().getInstance(getJob(), getExecutionSystem()));
            
            log.debug("Checking status of job " + job.getUuid());
            
            this.job = getJobMonitor().monitor();
        }
        catch (ClosedByInterruptException e) {
            throw e;
        }
        catch (StaleObjectStateException | UnresolvableObjectException e) {
            throw e;
        }
        catch (SystemUnavailableException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new JobException("Failed to check status of job " + job.getUuid(), e);
        }
    }

    public synchronized JobMonitor getJobMonitor() {
        return jobMonitor;
    }

    public synchronized void setJobMonitor(JobMonitor jobMonitor) {
        this.jobMonitor = jobMonitor;
    }

    /**
     * Looks up and returns the {@link ExecutionSystem} for this job
     * @return the job executionSystem
     */
    public ExecutionSystem getExecutionSystem() throws SystemUnavailableException {
        return new JobManager().getJobExecutionSystem(getJob());
    }
}

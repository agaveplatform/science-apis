package org.iplantc.service.jobs.queue.actions;

import io.grpc.netty.shaded.io.netty.util.internal.logging.InternalLogger;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.TransferTaskImpl;

import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractWorkerAction implements WorkerAction {

    protected final static Logger log = Logger.getLogger(AbstractWorkerAction.class);

//    private static Logger log = Logger.getLogger(AbstractWorkerAction.class);
    private AtomicBoolean stopped = new AtomicBoolean(false);
    
    protected Job job;
    protected URLCopy urlCopy;
    protected TransferTaskImpl rootTask;


    /**
     * Get or create a job manager for this action
     * @return an existing job manager, or new one if not yet initialized.
     */
    protected JobManager getJobManager() {
        if (jobManager == null) {
            this.jobManager = new JobManager();
        }

        return this.jobManager;
    }

    protected JobManager jobManager;

    public AbstractWorkerAction(Job job) {
        this.job = job;
    }

    /**
     * @return the stopped
     */
    @Override
    public boolean isStopped() {
        return stopped.get();
    }

    /**
     * @param stopped the stopped to set
     */
    @Override
    public synchronized void setStopped(boolean stopped) {
        this.stopped.set(stopped);
        
        if (getUrlCopy() != null) {
            getUrlCopy().setKilled(true);
        }
    }

    /**
     * @return the job
     */
    @Override
    public synchronized Job getJob() {
        return job;
    }

    /**
     * @param job the job to set
     */
    @Override
    public synchronized void setJob(Job job) {
        this.job = job;
    }

    /**
     * @return the urlCopy
     */
    @Override
    public synchronized URLCopy getUrlCopy() {
        return urlCopy;
    }

    /**
     * @param urlCopy the urlCopy to set
     */
    @Override
    public synchronized void setUrlCopy(URLCopy urlCopy) {
        this.urlCopy = urlCopy;
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.actions.WorkerAction#checkStopped()
     */
    @Override
    public void checkStopped() throws ClosedByInterruptException {
        if (isStopped()) {
            throw new ClosedByInterruptException();
        }
    }

    /**
     * Convenience method to update job status and update the class-local variable. Any {@link JobException} are
     * logged and swallowed to allow for cleaner code. Hibernate runtime exceptions are still thrown as they are
     * RunTimeExceptions.
     *
     * @param jobStatusType the new job status
     * @param message       the job event message for the update event
     * @throws org.hibernate.StaleStateException if the local job is out of sync with the db and cannot be updated. This
     *                                           can happen when two tasks compete for the same job. Only one can update the db to claim the task.
     */
    protected void updateJobStatus(JobStatusType jobStatusType, String message) {
        try {
            setJob(JobManager.updateStatus(getJob(), jobStatusType, message));
        } catch (JobException e) {
            log.error("Failed to update job " + getJob().getUuid() + " status to " + jobStatusType.name());
        } catch (Throwable t) {
            log.error("Failed to update job " + getJob().getUuid() + " status to " + jobStatusType.name());
            throw t;
        }
    }
}
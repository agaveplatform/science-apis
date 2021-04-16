package org.iplantc.service.jobs.queue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.dao.JobEventDao;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.queue.actions.WorkerAction;
import org.iplantc.service.jobs.queue.factory.AbstractJobProducerFactory;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractJobWatch implements WorkerWatch {

    protected static final Logger log = Logger.getLogger(AbstractJobWatch.class);
    
    protected AtomicBoolean stopped = new AtomicBoolean(false);
    protected AtomicBoolean taskComplete = new AtomicBoolean(false);
    protected Job job = null;
    private WorkerAction workerAction = null;
    protected boolean allowFailure = false;

    private String jobUuid;
    
    public AbstractJobWatch() {
        super();
    }
    
    public AbstractJobWatch(boolean allowFailure) {
        this();
        this.allowFailure = allowFailure;
    }

    /**
     * Null-safe getter for the {@link ExecutionSystem} associated with the job.
     * @return the job {@link ExecutionSystem}
     * @throws SystemUnavailableException if the system cannot be found, is not available, or has a stauts other than UP
     * @throws SystemUnknownException if the system is not in the db
     */
    protected ExecutionSystem getJobExecutionSystem() throws SystemUnavailableException, SystemUnknownException {
        ExecutionSystem jobExecutionSystem = (ExecutionSystem) new SystemDao().findBySystemId(getJob().getSystem());
        if (jobExecutionSystem == null) {
            throw new SystemUnknownException("No system found matching job execution system " + getJob().getSystem());
        } else if (!jobExecutionSystem.isAvailable() || !jobExecutionSystem.getStatus().equals(SystemStatusType.UP)) {
            throw new SystemUnavailableException("Job execution system " + getJob().getSystem() +
                    " is not currently available");
        }

        return jobExecutionSystem;
    }

    /* (non-Javadoc)
     * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
     */
    public void execute(JobExecutionContext context) 
    throws JobExecutionException 
    {
        try 
    	{
    		if (org.iplantc.service.common.Settings.isDrainingQueuesEnabled()) {
//    			log.debug("Queue draining has been enabled. Skipping archive task." );
    			return;
    		}
    		
    		if (context.getMergedJobDataMap().containsKey("uuid")) {
                setJobUuid(context.getMergedJobDataMap().getString("uuid"));
            }
    		
    		if (getJob() != null) 
            {
//    		    log.debug(getClass().getSimpleName() + " worker found job " + getJob().getUuid() + " for user " 
//                        + getJob().getOwner() + " to process");
    		    
    		    // this is a new thread and thus has no tenant info loaded. we set it up
                // here so things like app and system lookups will stay local to the 
                // tenant
                TenancyHelper.setCurrentTenantId(getJob().getTenantId());
                TenancyHelper.setCurrentEndUser(getJob().getOwner());
                
    			doExecute();
            } 

    	}
    	catch(JobExecutionException e) {
    	    if (allowFailure) throw e;
    	}
    	catch (Throwable e) 
    	{
    		log.error("Unexpected error during job worker execution", e);
    		if (allowFailure) 
    		    throw new JobExecutionException("Unexpected error during job worker execution",e);
    	}
    	finally {
    	    if (getJob() != null) {
    	        log.debug("Releasing job " + getJob().getUuid() + " after task completion");
    	        AbstractJobProducerFactory.releaseJob(getJob().getUuid());
    	    }
    	}
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
    	
        setStopped(true);
        
        if (getJob() != null) 
    	{
    		// this is a new thread and thus has no tenant info loaded. we set it up
    		// here so things like app and system lookups will stay local to the 
    		// tenant
    		TenancyHelper.setCurrentTenantId(getJob().getTenantId());
    		TenancyHelper.setCurrentEndUser(getJob().getOwner());
    		
    		try {
                for (JobEvent event: JobEventDao.getByJobId(getJob().getId())) 
                {
                	if (event.getTransferTask() != null) 
                	{
                		try { 
                			TransferTaskDao.cancelAllRelatedTransfers(event.getTransferTask().getId());
                		} catch (Throwable e) {
                			log.error("Failed to cancel transfer task " + 
                					event.getTransferTask().getUuid() + " associated with job " + this.jobUuid, e);
                		}
                	}
                }
            } catch (JobException e) {
                log.error("Failed to cancel transfers related to job " + getJob().getUuid() +
                        " during submission interrupt.", e);
            }

    		// set status back to previous state that will allow it to 
    		// be picked up by a worker again.
    		rollbackStatus();
    	}
    }
    
    /**
     * Performs the job status rollback to a previous state allowing it 
     * to be picked up by other workers.
     */
    protected abstract void rollbackStatus();

    @Override
    public synchronized void setJob(Job job) {
    	this.job = job;
    }

    /**
     * @return the workerAction
     */
    @Override
    public synchronized WorkerAction getWorkerAction() {
        return workerAction;
    }

    /**
     * @param workerAction the workerAction to set
     */
    @Override
    public synchronized void setWorkerAction(WorkerAction workerAction) {
        this.workerAction = workerAction;
    }

    @Override
    public boolean isStopped() {
        return stopped.get();
    }

    @Override
    public void setStopped(boolean killed) throws UnableToInterruptJobException {
        this.stopped.set(killed);
        if (getWorkerAction() != null) {
            getWorkerAction().setStopped(true);
            // Nothing else is acting on behalf of this task, so just kill it immediately. 
            // all transfers will be cleaned up in the setRollaback method.
            
//            int timeout = 0;
//            while(!isTaskComplete()) {
//                try { Thread.sleep(1000); } catch (InterruptedException e) {}
//                timeout++;
//                if (timeout >= 60) {
//                    throw new UnableToInterruptJobException("Unable to interrupt archiving task for job " 
//                            + this.jobUuid + " after 30 seconds.");
//                }
//                
//            }
        }
    }

    @Override
    public synchronized void setJobUuid(String uuid) throws JobException {
        this.jobUuid = uuid;
    }

    @Override
    public boolean isTaskComplete() {
        return taskComplete.get();
    }

    @Override
    public void setTaskComplete(boolean complete) {
        this.taskComplete.set(complete);
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.queue.WorkerWatch#getJob()
     */
    @Override
    public synchronized Job getJob() {
        if (this.job == null && StringUtils.isNotBlank(this.jobUuid)) {
            try {
                this.job = JobDao.getByUuid(this.jobUuid);
            } catch (JobException e) {
                log.error("Unable to resolve job " + this.jobUuid);
            }
        }
        
        return job;
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
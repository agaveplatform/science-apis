package org.iplantc.service.jobs.queue;

import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.SchedulerException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.queue.actions.WorkerAction;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public interface WorkerWatch extends InterruptableJob {

    void doExecute() throws JobExecutionException;
    
    /**
     * Selects the next available job for processing by a 
     * worker process. The selection process is left up to 
     * the implementing class.
     * 
     * @return uuid of the selected job
     */
    String selectNextAvailableJob() throws JobException, SchedulerException ;

    Job getJob() throws JobException;

    void setJob(Job job);
    
    void setJobUuid(String uuid) throws JobException;

    /**
     * @return the stopped
     */
    boolean isStopped();

    /**
     * @param killed the killed to set
     * @throws UnableToInterruptJobException 
     */
    void setStopped(boolean killed) throws UnableToInterruptJobException;

    /**
     * @return true of the task has completed, false otherwise
     */
    boolean isTaskComplete();

    /**
     * @param complete the complete to set
     */
    void setTaskComplete(boolean complete);

    /**
     * @return the workerAction
     */
    WorkerAction getWorkerAction();

    /**
     * @param workerAction the workerAction to set
     */
    void setWorkerAction(WorkerAction workerAction);
    
//    public void setJobProducerFactory(JobProducerFactory jobProducerFactory);

}
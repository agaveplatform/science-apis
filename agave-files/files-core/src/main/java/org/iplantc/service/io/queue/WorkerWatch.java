package org.iplantc.service.io.queue;

import org.iplantc.service.io.exceptions.TaskException;
import org.iplantc.service.io.model.QueueTask;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public interface WorkerWatch<T extends QueueTask> extends InterruptableJob {

    void doExecute() throws JobExecutionException;
    
    /**
     * Selects the next available job for processing by a 
     * worker process. The selection process is left up to 
     * the implementing class.
     * 
     * @return
     */
    Long selectNextAvailableQueueTask() throws TaskException ;

    T getQueueTask();

    void setQueueTask(T queueTask);

    /**
     * @return the stopped
     */
    boolean isStopped();

    /**
     * @param killed the stopped to set
     * @throws UnableToInterruptJobException 
     */
    void setStopped(boolean killed) throws UnableToInterruptJobException;

    /**
     * @return true of the task has completed, false otherwise
     */
    boolean isTaskComplete();

    /**
     * @param complete the taskComplete to set
     */
    void setTaskComplete(boolean complete);

    void setQueueTaskId(Long queueTaskId);

}
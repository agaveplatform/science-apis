/**
 *
 */
package org.iplantc.service.jobs.managers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;




/**
 * @author dooley
 *
 */
public class JobManager {
	private static final Logger	log	= Logger.getLogger(JobManager.class);

	/**
	 * Returns the {@link ExecutionSystem} for the given {@code job}.
	 * @param job
	 * @return a valid exection system or null of it no longer exists.
	 */
	public static ExecutionSystem getJobExecutionSystem(Job job) throws SystemUnavailableException {
	    RemoteSystem jobExecutionSystem = new SystemDao().findBySystemId(job.getSystem());
	    if (jobExecutionSystem == null) {
	        throw new SystemUnavailableException("Job execution system "
                    + job.getSystem() + " is not currently available");
	    } else {
	        return (ExecutionSystem)jobExecutionSystem;
	    }
	}

	/**
     * Returns the {@link Software} for the given {@code job}.
     * @param job
     * @return a valid {@link Software} object or null of it no longer exists.
     */
    public static Software getJobSoftwarem(Job job) {
        return SoftwareDao.getSoftwareByUniqueName(job.getAppId());
    }


	
    /**
     * Determines whether the job has completed archiving and can thus
     * refer to the archive location for requests for its output data.
     *
     * @param job
     * @return
     */
    public static boolean isJobDataFullyArchived(Job job)
    {
        if (job.isArchive())
        {
            if (job.getStatus() == JobStatusType.ARCHIVING_FINISHED) {
                return true;
            }
            else if (job.getStatus() == JobStatusType.FINISHED) {
                for (JobEvent event: job.getEvents()) {
                    if (StringUtils.equalsIgnoreCase(event.getStatus(), JobStatusType.ARCHIVING_FINISHED.name())) {
                        return true;
                    }
                }
            }
        }

        // anything else means the job failed, hasn't reached a point of
        // archiving, is in process, or something happened.
        return false;
    }

	
}

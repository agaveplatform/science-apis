/**
 * 
 */
package org.iplantc.service.jobs.managers.launchers;

import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.MissingSoftwareDependencyException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClient;

/**
 * @author dooley
 * 
 */
public class JobLauncherFactory 
{
	private static final Logger log = Logger.getLogger(JobLauncherFactory.class);
	
	/**
	 * Returns an intance of a {@link JobLauncher} based on the parameters of the job.
	 * Prior to creating the {@link JobLaunch}, this method validates the availability 
	 * of the {@link Software} and {@link ExecutionSystem}.
	 * 
	 * @param job
	 * @return
	 * @throws JobException
	 * @throws SystemUnavailableException
	 * @throws SoftwareUnavailableException
	 */
	public static JobLauncher getInstance(Job job) throws JobException, SystemUnavailableException, SoftwareUnavailableException
	{
	    Software software = SoftwareDao.getSoftwareByUniqueName(job.getSoftwareName());
		ExecutionSystem executionSystem = JobManager.getJobExecutionSystem(job);
		
		// if the system is unavailable or missing...
		if (!executionSystem.isAvailable()) {
			String msg = "Job execution system " + executionSystem.getSystemId() + " is not currently available.";
			log.warn(msg);
		    throw new SystemUnavailableException(msg);
        } 
		// if the system is in down time or otherwise unavailable...
        else if (executionSystem.getStatus() != SystemStatusType.UP)
        {
        	String msg = "Job execution system " + executionSystem.getSystemId() + 
        			     " is currently " + executionSystem.getStatus() + ".";
        	log.warn(msg);
            throw new SystemUnavailableException(msg);
        }
		// if the software app is missing...
        else if (software == null) 
		{ 
        	String msg = job.getSoftwareName() + " is not a recognized application.";
        	log.error(msg);
			throw new JobException(msg); 
		}
		// if the software app is unavailable...
		else if (!software.isAvailable())
		{
			String msg = "Application " + job.getSoftwareName() + " is not available for execution.";
			log.warn(msg);
			throw new SoftwareUnavailableException(msg); 
		}
		// if the software deployment system is unavailable...
        else if (software.getStorageSystem() == null || !software.getStorageSystem().isAvailable()) 
		{
        	String msg = "Software deployment system " + software.getStorageSystem().getSystemId() + 
        			     " is not currently available.";
        	log.warn(msg);
		    throw new SystemUnavailableException(msg);
		} 
		// if the software deployment system is unavailable...
		else if (software.getStorageSystem().getStatus() != SystemStatusType.UP)
		{
			String msg = "Software deployment system " + software.getStorageSystem().getSystemId() + 
					     " is currently " + software.getStorageSystem().getStatus() + ".";
			log.warn(msg);
		    throw new SystemUnavailableException(msg);
		}
		// if the software assets are missing...
        else 
		{
			RemoteDataClient remoteDataClient = null;
			try {
				
				remoteDataClient = software.getStorageSystem().getRemoteDataClient();
				remoteDataClient.authenticate();
				
				if (software.isPubliclyAvailable())
				{	
					if (!remoteDataClient.doesExist(software.getDeploymentPath()))
					{
//					    // TODO: no point doing this until the underlying systems get more reliable
////					    if (Settings.DISABLE_MISSING_SOFTWARE) {
////    						software.setAvailable(false);
////    						SoftwareDao.persist(software);
////    						EmailMessage.send("Rion Dooley", 
////    								"dooley@tacc.utexas.edu", 
////    								"Public app " + software.getUniqueName() + " is missing.", 
////    								"While submitting a job, the Job Service noticed that the app bundle " +
////    								"of the public app " + software.getUniqueName() + " was missing. This " +
////    								"will impact provenance and could impact experiment reproducability. " +
////    								"Please restore the application zip bundle from archive and re-enable " + 
////    								"the application via the admin console.\n\n" +
////    								"Name: " + software.getUniqueName() + "\n" + 
////    								"User: " + job.getOwner() + "\n" +
////    								"Job: " + job.getUuid() + "\n" +
////    								"Time: " + job.getCreated().toString() + "\n\n");
////					    }
//						throw new MissingSoftwareDependencyException("Application executable is missing. Software is not available.");
					    String msg = "Missing software on host " + remoteDataClient.getHost() + " at path " +
					    		     software.getDeploymentPath() + ".";
					    log.error(msg);
						throw new MissingSoftwareDependencyException(msg);
					} 
				}
			    else if (!remoteDataClient.doesExist(software.getDeploymentPath() + '/' + software.getExecutablePath())) 
				{
				    String msg = "Missing software on host " + remoteDataClient.getHost() + " at path " +
			    		         software.getDeploymentPath() + '/' + software.getExecutablePath() + ".";
				    log.error(msg);
					throw new MissingSoftwareDependencyException(msg);
				}
			} catch (MissingSoftwareDependencyException e) {
				String msg = "Unable to locate the application wrapper template at agave://" 
                        + software.getStorageSystem().getSystemId() + "/" 
                        + software.getDeploymentPath() + '/' + software.getExecutablePath() 
                        + ". Job cannot run until the template is restored.";
				log.error(msg, e);
			    throw new SoftwareUnavailableException(msg);
			} catch (Throwable e) {
				String msg = "Unable to verify the availability of the application executable. Software is not available.";
				log.error(msg, e);
				throw new JobException(msg, e);
			} 
			finally {
				try { remoteDataClient.disconnect(); } catch(Exception e) {}
			}
		}

		// now submit the job to the target system using the correct launcher.
		if (software.getExecutionSystem().getExecutionType().equals(ExecutionType.HPC))
		{
			return new HPCLauncher(job);
		}
		else if (software.getExecutionSystem().getExecutionType().equals(ExecutionType.CONDOR))
		{
			return new CondorLauncher(job);
		}
		else
		{
			return new CLILauncher(job);
		}
	}
}

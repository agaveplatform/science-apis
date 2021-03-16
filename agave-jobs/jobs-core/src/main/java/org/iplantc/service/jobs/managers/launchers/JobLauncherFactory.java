/**
 * 
 */
package org.iplantc.service.jobs.managers.launchers;

import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.exceptions.UnknownSoftwareException;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.MissingSoftwareDependencyException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
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

	protected JobManager jobManager = null;

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

	/**
	 * Returns an intance of a {@link JobLauncher} based on the parameters of the job.
	 * Prior to creating the {@link JobLauncher}, this method validates the availability
	 * of the {@link Software} and {@link ExecutionSystem}.
	 * 
	 * @param job the job for which to create a {@link JobLauncher}
	 * @throws SystemUnavailableException when the job {@link ExecutionSystem} or {@link Software#getStorageSystem()} is disabled or has a non-UP status.
	 * @throws SystemUnknownException when the job {@link ExecutionSystem} or {@link Software#getStorageSystem()}does not exist
	 * @throws SoftwareUnavailableException when the {@link Software} is unavailable due to explicit or implicit reasons
	 * @throws UnknownSoftwareException when the {@link Software} has been deleted
	 */
	public JobLauncher getInstance(Job job) throws SystemUnknownException, SystemUnavailableException, UnknownSoftwareException, SoftwareUnavailableException
	{
		Software software = getJobManager().getJobSoftware(job.getSoftwareName());

		// Fetch the execution system info and ensure it's available.
		ExecutionSystem executionSystem = (ExecutionSystem) getJobManager().getAvailableSystem(job.getSystem());

		assertSoftwareExecutionTypeMatchesSystem(executionSystem, software);

		assertSoftwareWrapperTemplateExists(software);

		// now submit the job to the target system using the correct launcher.
		if (software.getExecutionType().equals(ExecutionType.HPC)){
			return new HPCLauncher(job, software, executionSystem);
		}else if (software.getExecutionType().equals(ExecutionType.CONDOR)) {
			return new CondorLauncher(job, software, executionSystem);
		} else {
			return new CLILauncher(job, software, executionSystem);
		}
	}

	/**
	 * Verifies that the {@link Software#getExecutionType()} is supported by the system matching the
	 * {@code executionSystemId}. An exception is thrown if they do not line up.
	 *
	 * @param executionSystem the system whose {@link ExecutionSystem#getExecutionType()} will be checked
	 *                             for compatibility with the {@link Software#getExecutionType()}
	 * @param software the {@link Software} whose {@link ExecutionType} will be checked
	 * @throws SystemUnavailableException when the job {@link ExecutionSystem} is disabled or has a non-UP status.
	 * @throws SystemUnknownException when the job {@link ExecutionSystem} does not exist
	 * @throws SoftwareUnavailableException when the execution system cannot support the {@link ExecutionType}
	 *                             			required by the {@link Software}
	 */
	protected void assertSoftwareExecutionTypeMatchesSystem(ExecutionSystem executionSystem, Software software) throws SystemUnavailableException, SystemUnknownException, SoftwareUnavailableException {
		// ensure the execution system supports the execution type configured for the app. This may change over time,
		// so we ensure it's still possible to do so here.
		if (!software.getExecutionType().getCompatibleExecutionTypes().contains(executionSystem.getExecutionType())) {
			throw new SoftwareUnavailableException("The software requested by for this job requires an execution type of " +
					software.getExecutionType() + ", but the requested execution system, " + executionSystem.getSystemId() +
					", has an incompatible execution type, " + executionSystem.getExecutionType() +
					", which prevents this job from being launched.");
		}
	}

	/**
	 * Ensures the {@link Software#getStorageSystem()} is available and has the {@link Software#getExecutablePath()}
	 * present.
	 * @param software the application whose deployment assets will be validated
	 * @throws SystemUnknownException if the {@link Software#getStorageSystem()} does not exist.
	 * @throws SystemUnavailableException if the {@link Software#getStorageSystem()} is not available and up.
	 * @throws SoftwareUnavailableException if the {@link Software#getExecutablePath()} is not present on the remote system.
	 */
	protected void assertSoftwareWrapperTemplateExists(Software software) throws SystemUnknownException, SystemUnavailableException, SoftwareUnavailableException {
		// if the software assets are missing...
		RemoteDataClient remoteDataClient = null;
		try {
			StorageSystem storageSystem = (StorageSystem) getJobManager().getAvailableSystem(software.getStorageSystem().getSystemId());
			remoteDataClient = storageSystem.getRemoteDataClient();
			remoteDataClient.authenticate();

			if (software.isPubliclyAvailable()) {
				if (!remoteDataClient.doesExist(software.getDeploymentPath())) {
					throw new MissingSoftwareDependencyException("Public app assets were not present at  agave://" +
							remoteDataClient.getHost() + ":" +
							 software.getDeploymentPath() + ". Job cannot run until the assets are restored.");
				}
			} else if (!remoteDataClient.doesExist(software.getDeploymentPath() + '/' + software.getExecutablePath())) {
				throw new MissingSoftwareDependencyException("App wrapper template expected at agave://" +
						remoteDataClient.getHost() + ":" + software.getDeploymentPath() + '/' +
						software.getExecutablePath() + ". Job cannot run until the assets are restored.");
			}
		} catch (SystemUnknownException e) {
			throw new SystemUnknownException("No system found matching id of app deployment system, " +
					software.getStorageSystem().getSystemId() + ".");
		} catch (SystemUnavailableException e) {
			throw new SystemUnavailableException("App deployment system " + software.getStorageSystem().getSystemId() +
					" is currently unavailable.");
		} catch (MissingSoftwareDependencyException e) {
			throw new SoftwareUnavailableException(e);
		} catch (Throwable e) {
			throw new SoftwareUnavailableException("Unable to verify the availability of the wrapper " +
					"template at agave://" + software.getStorageSystem().getStorageConfig().getHost() + ":" +
					software.getDeploymentPath() + '/' + software.getExecutablePath() +
					".Job cannot run until the assets are restored.", e);
		}
		finally {
			try { if (remoteDataClient != null) remoteDataClient.disconnect(); } catch(Exception ignored) {}
		}
	}
}

package org.iplantc.service.monitor.managers;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.hibernate.StaleStateException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.monitor.dao.MonitorCheckDao;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.events.MonitorEventProcessor;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.MonitorCheck;
import org.iplantc.service.monitor.model.enumeration.MonitorCheckType;
import org.iplantc.service.monitor.model.enumeration.MonitorEventType;
import org.iplantc.service.monitor.model.enumeration.MonitorStatusType;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.manager.SystemRoleManager;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.jclouds.blobstore.domain.PageSet;
import org.joda.time.DateTime;

/**
 * Management class to handle processing of monitoring tasks
 * 
 * @author dooley
 *
 */
public class MonitorManager 
{
	private static final Logger log = Logger.getLogger(MonitorManager.class);
	
	private MonitorEventProcessor eventProcessor;
	
	public MonitorManager() {
		this.eventProcessor = new MonitorEventProcessor();
	}

	/**
	 * Returns the {@link RemoteSystem} for the given {@code monitor}.
	 * @param monitor the monitor for which to get the {@link RemoteSystem}
	 * @return the system being monitored
	 * @throws SystemUnavailableException if the system is no longer available
	 * @throws SystemUnknownException if the {@link RemoteSystem} is not found in the database, or no {@link RemoteSystem} is linked ot the {@link Monitor}
	 */
	public RemoteSystem getSystem(Monitor monitor) throws SystemUnavailableException, SystemUnknownException {

		RemoteSystem system = monitor.getSystem();

		if (system == null) {
			throw new SystemUnknownException("Unable to find system for monitor " + monitor.getUuid());
		} else if (!system.isAvailable()) {
			throw new SystemUnavailableException("System is currently unavailable for use. Monitoring will be disabled.");
		}

		return system;
	}

	/**
	 * Mockable getter returning a {@link SystemRoleManager} for the system.
	 * @param system the system on which the {@link SystemRoleManager} will operate
	 * @return a {@link SystemRoleManager} for the {@link RemoteSystem}
	 */
	protected SystemRoleManager getSystemRoleManager(RemoteSystem system) {
		return new SystemRoleManager(system);
	}

	/**
	 * Checks that the {@link Monitor} owner's role satisfies {@link RoleType#canUse()} on the {@code system}.
	 * @param system the {@link RemoteSystem} for which to check user login roles
	 * @param username the username of the user to check system login access
	 * @throws PermissionException if the user has {@link RoleType#NONE} permissions on the system
	 */
	public void verifyUserHasSystemLoginPermission(RemoteSystem system, String username) throws SystemRoleException, PermissionException {
		SystemRoleManager systemRoleManager = getSystemRoleManager(system);
		if (! systemRoleManager.getUserRole(username).getRole().canUse()) {
			throw new PermissionException("User does not have a sufficient roles to run commands on system " +
					system.getSystemId());
		}
	}

	/**
	 * Checks that the {@link Monitor} owner's role satisfies {@link RoleType#canRead()} on the {@code system}.
	 * @param system the {@link RemoteSystem} for which to check user data roles
	 * @param username the username of the user to check system data access
	 * @throws PermissionException if the user has {@link RoleType#NONE} permissions on the system
	 */
	public void verifyUserHasSystemDataPermissions(RemoteSystem system, String username) throws SystemRoleException, PermissionException {
		SystemRoleManager systemRoleManager = getSystemRoleManager(system);
		if ( ! system.getUserRole(username).getRole().canRead()) {
			throw new PermissionException("User does not have a sufficient roles to access data on system" +
					system.getSystemId());
		}
	}

	/**
	 * Sends off all the monitors for a particular event and UUID for processing.
	 * 
	 * @param monitor the monitor to check
	 * @param createdBy the user who initiated the check
	 * @return a monitor with the check results or null if the system is not available to check
	 */
	public MonitorCheck check(Monitor monitor, String createdBy)
	{
		MonitorDao dao = new MonitorDao();
		RemoteSystem system = null;
		MonitorCheck check = null;
		try {
			system = getSystem(monitor);

			check = doStorageCheck(monitor, createdBy);

			if (monitor.getSystem() instanceof ExecutionSystem) {
				MonitorCheck loginCheck = doLoginCheck(monitor, createdBy);
				if (check.getResult() != MonitorStatusType.FAILED) {
					check = loginCheck;
				}
			}
			if (check.getResult().equals(MonitorStatusType.PASSED)) {
				monitor.setLastSuccess(new Date(check.getCreated().getTime()));
				try {
					dao.persist(monitor);
				} catch (Throwable e) {
					log.error("Check for monitor " + monitor.getUuid() +
							" succeeded, but failed to updated the monitor's last success date.");
				}
			}
		} catch (SystemUnknownException | SystemUnavailableException e) {
			disableMonitor(monitor);
			log.debug("Disabled monitor " + monitor.getUuid() + " on system " +
					monitor.getSystem().getSystemId() + " due to system being inactive.");
		}

		return check;
	}

	/**
	 * Performs a check on connectivity to a storage system.
	 * 
	 * @param monitor the monitor to check
	 * @param createdBy user who kicked off the check
	 * @return a monitor check with the current check results
	 */
	@SuppressWarnings("unused")
    private MonitorCheck doStorageCheck(Monitor monitor, String createdBy)
	{
		MonitorCheckDao checkDao = getMonitorCheckDao();
		MonitorCheck currentCheck = new MonitorCheck(monitor, MonitorStatusType.UNKNOWN, null, MonitorCheckType.STORAGE);

		RemoteDataClient monitoredSystemRemoteDataClient = null;
		MonitorCheck lastCheck = null;
		RemoteSystem system = null;
		try 
		{
			// system was verified when the monitoring task was selected, so we use the interal getter here
			system = monitor.getSystem();

			// check user still has permission to run the data check on the target system. The permission exception
			// will be handled with failure logic, but will not disable the monitor. This allows for situations where
			// a check might run while a system is being published, updated, or a user is revoked access for a short
			// period of time.
			verifyUserHasSystemDataPermissions(system, monitor.getOwner());

			lastCheck = checkDao.getLastMonitorCheck(monitor.getId());


			monitoredSystemRemoteDataClient = system.getRemoteDataClient(monitor.getInternalUsername());

			// remote storage auth check
			if (log.isTraceEnabled())
			    log.trace("[" + Thread.currentThread().getName() + "] Checking authentication to " + system.getSystemId() +
					system.getStorageConfig().toString());

			monitoredSystemRemoteDataClient.authenticate();

			if (log.isTraceEnabled())
				log.trace("Data connectivity succeeded to " + system.getSystemId() +
						"(" +  system.getStorageConfig().getProtocol() + ":" + monitoredSystemRemoteDataClient.getHost() + ")");


			// file system check
			if (log.isTraceEnabled())
				log.trace("[" + Thread.currentThread().getName() + "] Checking data connectivity to " + system.getSystemId() +
						system.getStorageConfig().toString());

			// we perform an existence check on / because if that does not exist, data operations on the remote system
			// won't be useful anyway.
			monitoredSystemRemoteDataClient.doesExist("/");

			if (log.isTraceEnabled())
				log.trace("Data connectivity succeeded to " + system.getSystemId() +
						"(" +  system.getStorageConfig().getProtocol() + ":" + monitoredSystemRemoteDataClient.getHost() + ")");


			// if both succeeded, this check passes
			currentCheck.setResult(MonitorStatusType.PASSED);
			currentCheck.setCreated(new Date());


		} catch (SystemRoleException e) {
			String msg = String.format("Unable to verify owner permissions on monitor %s to access system %s. Skipping check.",
					monitor.getUuid(), system.getSystemId());
			log.error(msg);

			currentCheck.setMessage(e.getMessage());
			currentCheck.setResult(MonitorStatusType.UNKNOWN);
		}
		catch (PermissionException e) {
			if (log.isTraceEnabled())
				log.trace(e.getMessage());

			// fail the check
			currentCheck.setMessage(e.getMessage());
			currentCheck.setResult(MonitorStatusType.FAILED);
		} catch (RemoteDataException|IOException  e) {
			if (system.getStorageConfig() != null) {
				log.debug("Data connectivity failed to " + system.getSystemId() + 
					"(" +  system.getStorageConfig().getProtocol() + ":" + system.getStorageConfig().getHost() + ")" +
					". " + e.getMessage());
			} else {
				log.debug("Data connectivity failed to " + system.getSystemId() + 
						". " + e.getMessage());
			}

			// something went wrong. The system storage is not accessible.
			currentCheck.setMessage(e.getMessage());
			currentCheck.setResult(MonitorStatusType.FAILED);
		}
		catch (RemoteCredentialException e) 
		{
			log.debug("Failed to retrieve an authentication credential for " + system.getSystemId() +
				" when running storage check. " +  e.getMessage());

			// something went wrong. The system storage is not accessible.
			currentCheck.setMessage("Authentication failed for " + system.getSystemId() + 
					". " + e.getMessage());
			currentCheck.setResult(MonitorStatusType.FAILED);
		}
		catch (Exception e)
		{
			if (system != null && system.getStorageConfig() != null) {
				log.debug("Data connectivity failed to " + system.getSystemId() + " " +
								system.getStorageConfig().toString() + ". " + e.getMessage());
			} else if (system != null) {
				log.debug("Data connectivity failed to " + system.getSystemId() + 
						". " + e.getMessage());
			} else {
				log.debug("Data connectivity failed for monitor " + monitor.getId());
			}
			
			currentCheck.setMessage("Failed to perform storage monitoring check on " + 
					monitor.getSystem().getSystemId() + "\n" + e.getMessage());
			currentCheck.setResult(MonitorStatusType.FAILED);
		}
		finally {
			try { if (monitoredSystemRemoteDataClient != null) monitoredSystemRemoteDataClient.disconnect(); } catch (Exception ignored) {}
		}
		
		try {
			currentCheck.setMonitor(monitor);
			checkDao.persist(currentCheck);
			eventProcessor.processCheckEvent(monitor, lastCheck, currentCheck, createdBy);
		} 
		catch (Exception e)  {
			log.error("Failed to persist storage monitor check " + currentCheck.getUuid() + " for monitor " + monitor.getUuid(), e);
		} finally {
			try {
				monitor.setLastUpdated(new Date());
				new MonitorDao().persist(monitor);
			} catch (StaleStateException e) {
				log.error("Failed to update stale reference to monitor " + monitor.getUuid() +
						" after check " + currentCheck.getUuid());
			} catch (Throwable t){
				log.error("Failed to update monitor " + monitor.getUuid() + " after check " +
						currentCheck.getUuid(), t);
			}
		}
		
		return currentCheck;
	}


	/**
	 * Performs a check on connectivity to a login system.
	 * 
	 * @param monitor the monitor to check
	 * @param createdBy user who kicked off the check
	 * @return a monitor check with the current check results
	 */
	@SuppressWarnings("unused")
	private MonitorCheck doLoginCheck(Monitor monitor, String createdBy)
	{
		MonitorCheckDao checkDao = getMonitorCheckDao();
		MonitorCheck currentCheck = new MonitorCheck(monitor, MonitorStatusType.FAILED, null, MonitorCheckType.LOGIN);
		
		MonitorCheck lastCheck = null;
		ExecutionSystem system = null;
		try 
		{	
			lastCheck = checkDao.getLastMonitorCheck(monitor.getId());
			
			system = (ExecutionSystem)monitor.getSystem();


			if (log.isTraceEnabled())
				log.trace(String.format("Checking login connectivity to %s -> %s",
					system.getSystemId(), system.getLoginConfig().toString()));

			try (RemoteSubmissionClient submissionClient = ((ExecutionSystem)monitor.getSystem()).getRemoteSubmissionClient(monitor.getInternalUsername())) {
				if (submissionClient.canAuthentication()) {
					currentCheck.setResult(MonitorStatusType.PASSED);

					if (log.isTraceEnabled())
						log.trace("Login authentication succeeded to " + system.getSystemId() +
							" -> " + system.getLoginConfig().toString());
				} else {
					currentCheck.setMessage("Failed to authenticate to system " + system.getSystemId() + " at " +
							system.getLoginConfig().getProtocol() + ":" + system.getLoginConfig().getHost());

					log.debug("Login authentication failed to " + system.getSystemId() +
							" -> " + system.getLoginConfig().toString());
				}
			} catch (AuthenticationException e) {
				log.debug("Failed to retrieve an authentication credential to " + system.getSystemId() +
						" -> " + system.getLoginConfig().toString() + " when running login check. " +  e.getMessage());

				// something went wrong. The system storage is not accessible.
				currentCheck.setMessage("Authentication failed for " + system.getSystemId() +
						". " + e.getMessage());
			}
		} catch (SystemRoleException e) {
			String msg = String.format("Unable to verify owner permissions on monitor %s to access system %s. Skipping check.",
					monitor.getUuid(), system.getSystemId());
			log.error(msg);

			currentCheck.setMessage(e.getMessage());
			currentCheck.setResult(MonitorStatusType.UNKNOWN);
		}
		catch (PermissionException e) {
			if (log.isTraceEnabled())
				log.trace(e.getMessage());

			// fail the check
			currentCheck.setMessage(e.getMessage());
		} catch (MonitorException e) {
			log.error("Failed to fetch previous check for monitor " + monitor.getUuid() +
					" from the database when processing check " + currentCheck.getUuid() + ". " + e.getMessage());
		}
		catch (Exception e)
		{
			String msg = String.format("Failed to perform login check %s for monitor %s on system %s. %s",
					currentCheck.getUuid(), monitor.getUuid(), system.getSystemId(), e.getMessage());
			log.error(msg);

			// set the status to unknown as we do not know the actual remote status since we were unable to
			// run the check.
			currentCheck.setResult(MonitorStatusType.UNKNOWN);
			// pass on the message to the end user.
			currentCheck.setMessage(String.format("Failed to perform login check on %s. Caused by: %s",
					system.getSystemId(), e.getMessage()));
		}
		
		try {
			currentCheck.setMonitor(monitor);
			currentCheck.setCreated(new Date());
			checkDao.persist(currentCheck);

			eventProcessor.processCheckEvent(monitor, lastCheck, currentCheck, createdBy);
		} catch (Exception e) {
			log.error("Failed to persist login monitor check " + currentCheck.getUuid() + " for monitor " + monitor.getUuid(), e);
		} finally {
			try {
				monitor.setLastUpdated(new Date());
				new MonitorDao().persist(monitor);
			} catch (Exception ignored){}
			
		}
		
		return currentCheck;
	}


	/**
	 * Sets {@link Monitor#isActive()} to false for the given {@code monitor}. A {@link MonitorEventType#DISABLED}
	 * event is raised.
	 * @param monitor the monitor to disable
	 */
	protected void disableMonitor(Monitor monitor) {
		try {
			monitor.setActive(false);
			monitor.setLastUpdated(new Date());
			getMonitorDao().persist(monitor);
		} catch (Exception e) {
			log.error("Failed to disable monitor " + monitor.getUuid() + " after system was found unavailable", e);
		} finally {
			eventProcessor.processContentEvent(monitor, MonitorEventType.DISABLED, monitor.getOwner());
		}
	}

	/**
	 * Reschedules a {@link MonitorCheck} for the next avaialble time based on the
	 * frequency set at registration.
	 * 
	 * @param monitor the monitor whose timer will be reset
	 * @throws MonitorException when the monitor cannot be updated
	 */
	public void resetNextUpdateTime(Monitor monitor) throws MonitorException
	{
		MonitorDao dao = new MonitorDao();
		dao.refresh(monitor);
		monitor.setLastUpdated(new Date());
		monitor.setNextUpdateTime(new DateTime().plusMinutes(monitor.getFrequency()).toDate());
		dao.persist(monitor);
	}

	/**
	 * Mockable getter
	 * @return the {@link MonitorCheckDao} to get
	 */
	protected MonitorCheckDao getMonitorCheckDao() {
		return new MonitorCheckDao();
	}

	/**
	 * Mockable getter
	 * @return the {@link MonitorDao} to get
	 */
	protected MonitorDao getMonitorDao() {
		return new MonitorDao();
	}
}

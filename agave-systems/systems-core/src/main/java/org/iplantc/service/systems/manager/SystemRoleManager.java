package org.iplantc.service.systems.manager;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.clients.AgaveProfileServiceClient;
import org.iplantc.service.common.clients.beans.Profile;
import org.iplantc.service.common.exceptions.NotificationException;
import org.iplantc.service.common.util.HTMLizer;
import org.iplantc.service.notification.util.EmailMessage;
import org.iplantc.service.systems.Settings;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.events.RemoteSystemEventProcessor;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.systems.util.ServiceUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles adding and removing roles for a user on a system.
 * 
 * @author dooley
 *
 */
public class SystemRoleManager {
	private static final Logger	log	= Logger.getLogger(SystemRoleManager.class);
	
	private RemoteSystem system;
	private RemoteSystemEventProcessor eventProcessor;
	
	/**
	 * Default constructor to create an role manager on a system.
	 * 
	 * @param system
	 * @throws SystemException
	 */
	public SystemRoleManager(RemoteSystem system) throws SystemException
	{
		if (system == null) { 
			throw new SystemException("RemoteSystem cannot be null"); 
		}
		setSystem(system);
		this.eventProcessor = new RemoteSystemEventProcessor();
	}

	/**
	 * Assigns a specific role to a user on a system.
	 * 
	 * @param username user to whom the role will be granted
	 * @param roleType the role type to be granted
	 * @param createdBy the user granting the role the recipient
	 * @throws SystemException if the update fails
	 */
	public void setRole(String username, RoleType roleType, String createdBy)
	throws SystemException
	{
		if (StringUtils.isEmpty(username) || StringUtils.equals(username, "null")) { 
			throw new SystemException("Invalid username"); 
		}

		// owners and admins cannot alter their own permissions
		if (getSystem().getOwner().equals(username) || ServiceUtils.isAdmin(username))
			return;

		// Apps cannot be published to storage systems, so this role means nothing for storage
		// systems. We throw an exception if such assignment is attempted.
		if (roleType == RoleType.PUBLISHER && getSystem().getType() == RemoteSystemType.STORAGE) {
			throw new SystemException("Cannot set PUBLISHER role on storage systems."); 
		}

		SystemRole currentRole = getSystem().getUserRole(username);
		SystemDao dao = new SystemDao();
		
		if (currentRole == null)
		{ 
			if (roleType.equals(RoleType.NONE)) {
				return;
			} else {
			    SystemRole newRole = new SystemRole(username, roleType);
				getSystem().addRole(newRole);
				
				this.eventProcessor.processPermissionEvent(getSystem(), newRole, createdBy);

				dao.merge(getSystem());
			}
		} 
		else 
		{
			// remove the current role if it's set to NONE
			if (roleType.equals(RoleType.NONE)) {
				getSystem().removeRole(currentRole);
				this.eventProcessor.processPermissionEvent(getSystem(), new SystemRole(username, RoleType.NONE), createdBy);
			// otherwise, only update if the role is not the same
			} else if (!currentRole.getRole().equals(roleType)) {
				getSystem().removeRole(currentRole);
				SystemRole newRole = new SystemRole(username, roleType);
				getSystem().addRole(newRole);

				this.eventProcessor.processPermissionEvent(getSystem(), newRole, createdBy);
			}

			setSystem(dao.merge(getSystem()));
		}
	}

	/**
	 * Removes all but the system owner roles on a system.
	 * 
	 * @param createdBy the username of the user clearing the roles
	 * @throws SystemException if the system could not be updated
	 */
	public void clearRoles(String createdBy) throws SystemException
	{
		SystemDao dao = new SystemDao();

	    Set<SystemRole> currentRoles = getSystem().getRoles();
		SystemRole[] deletedRoles = currentRoles.toArray(new SystemRole[] {});
		
		for (SystemRole role: deletedRoles) {
			getSystem().removeRole(role);
		}

		dao.persist(getSystem());
//		setSystem(dao.merge(getSystem()));

		if (!getSystem().isPubliclyAvailable())
		{
			for (SystemRole deletedRole: deletedRoles) {
				this.eventProcessor.processPermissionEvent(getSystem(), new SystemRole(deletedRole.getUsername(), RoleType.NONE), createdBy);
			}
		}
	}
	
	/**
	 * Alerts an app publishers that their applications were disabled. 
	 * This occurs when a publisher role has been removed from them on a system.
	 * 
	 * @param appOwner the user whose publisher status was revoked and whose app is now disabled
	 * @param appIds the app that was disabled
	 * @param system the system for which the app was disabled
	 */
	public void sendApplicationDisabledMessage(String appOwner, List<String> appIds, RemoteSystem system) 
	{
		if (ServiceUtils.isValid(appIds)) return;
		
		AgaveProfileServiceClient profileClient = new AgaveProfileServiceClient(
				Settings.IPLANT_PROFILE_SERVICE + "profile/search/username/" + appOwner, 
				Settings.IRODS_USERNAME, 
				Settings.IRODS_PASSWORD);
		
		String fullname = "";
		Profile ownerProfile = null;
		try
		{
			for(Profile profile: profileClient.getUsers()) 
			{
				if (profile.getUsername().equals(appOwner)) {
					ownerProfile = profile;
					break;
				}
			}
			
			if (ownerProfile == null) {
				throw new NotificationException("User profile not found for " + appOwner);
			}
		}
		catch (Exception e)
		{
			log.error("Error looking up email address for " + appOwner + 
					" no notification email will be sent", e);
			return;
		}
		
		try 
		{	
			String subject = "Your access to \"" + getSystem().getName() + "\" has been revoked.";
			
			String body = fullname + ",\n\n" +
					"This email is being sent to you as a courtesy by the Agave Platform. " +
					"Your access to " + getSystem().getName() + " (" + getSystem().getSystemId() + ") " +
					"has been revoked by the owner. As a result, the applications you " +
					"registered on this system have been disabled. The affected applications " +
					"are listed below.\n\n";
			for(String appId: appIds) {
				body += "\t" + appId + "\n";
			}
			body += "\nYou will still have access to the application data itself as long as " +
					"you have access to the storage system on which the application deployment " +
					"directory exists. Any jobs you and others have run using the disabled applications " +
					"will remain accessible through your personal job histories. The generated job " +
					"data will be available through the job service. If you have further questions, " +
					"please contact help@agaveplatform.org.";
			
			try {
				if (StringUtils.isNotBlank(ownerProfile.getEmail())) {
					EmailMessage.send(fullname, ownerProfile.getEmail(), subject, body, HTMLizer.htmlize(body));
				} else {
					log.debug("Unable to send software deactivation notification for " +
									getSystem().getSystemId() + " to " + appOwner +
							" due to missing email address in their profile.");
				}
			} catch (Exception e) {
				log.error("Failed to send software deactivation notification to " +
						appOwner + " at " + ownerProfile.getEmail(), e);
			}
		}
		catch (Exception e)
		{
			log.error("Error notifying " + appOwner + " of application deactivation.", e);
		}
	}

	/**
	 * Returns effective {@link SystemRole} of user after adjusting for 
	 * resource scope, public, and world user roles.
	 * 
	 * @param username the username of the user to check
	 * @return the role the user has or {@link RoleType#NONE} if they have no role.
	 */
	public SystemRole getUserRole(String username) {
		if (StringUtils.isEmpty(username)) {
			return new SystemRole(username, RoleType.NONE, getSystem());
		}
		else if (username.equals(getSystem().getOwner()))
		{
			return new SystemRole(username, RoleType.OWNER, getSystem());
		}
		else if (ServiceUtils.isAdmin(username))
		{
			return new SystemRole(username, RoleType.ADMIN, getSystem());
		}
		else
		{
			SystemRole worldRole = new SystemRole(Settings.WORLD_USER_USERNAME, RoleType.NONE, getSystem());
			for(SystemRole role: getSystem().getRoles()) {
				if(role.getUsername().equals(username)) {
					if (role.getRole() == RoleType.PUBLISHER && getSystem().getType() == RemoteSystemType.STORAGE) {
						return new SystemRole(username, RoleType.USER, getSystem());
					} else {
						return role;
					}
				} else if (role.getUsername().equals(Settings.WORLD_USER_USERNAME)) {
					worldRole = role;
				}
			}

			if ( getSystem().isPubliclyAvailable())
			{
				if (getSystem().getType() != RemoteSystemType.EXECUTION && worldRole.canRead())
				{
					return new SystemRole(username, RoleType.GUEST, getSystem());
				}
				else
				{
					return new SystemRole(username, RoleType.USER, getSystem());
				}
			}
			else
			{
				return new SystemRole(username, RoleType.NONE, getSystem());
			}
		}
	}

	/**
	 * Fetches all system roles, applying the given pagination.
	 * @param limit the max results to return
	 * @param offset the number of roles to skip in the response
	 * @return a list of the {@link SystemRole} for the {@link #system}
	 */
	public List<SystemRole> getRoles(int limit, int offset) {
		if (limit < 0) {
			limit = 0;
		}
		else if (limit > org.iplantc.service.common.Settings.MAX_PAGE_SIZE) {
			limit = org.iplantc.service.common.Settings.MAX_PAGE_SIZE;
		}

		if (offset < 0) offset = 0;

		// define the owner role
		SystemRole ownerRole = new SystemRole(getSystem().getOwner(), RoleType.OWNER);

		List<SystemRole> effectiveRoles = new ArrayList<SystemRole>();
		effectiveRoles.add(ownerRole);
		effectiveRoles.addAll(getSystem().getRoles());

		return effectiveRoles.stream().skip(offset).limit(limit).collect(Collectors.toList());
	}

	/**
	 * @return the system
	 */
	public RemoteSystem getSystem() {
		return this.system;
	}

	/**
	 * @param system the system to set
	 */
	public void setSystem(RemoteSystem system) {
		this.system = system;
	}

	/**
	 * @return the eventProcessor
	 */
	public RemoteSystemEventProcessor getEventProcessor() {
		return eventProcessor;
	}

	/**
	 * @param eventProcessor the eventProcessor to set
	 */
	public void setEventProcessor(RemoteSystemEventProcessor eventProcessor) {
		this.eventProcessor = eventProcessor;
	}

}

package org.agaveplatform.service.transfers.util;

import org.hibernate.HibernateException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.manager.SystemRoleManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;

public class RemoteSystemAO {

    /**
     * Fetches the {@link RemoteSystem} with the given {@code systemId} in the {@code tenantId}.
     * Legacy DAO have a forced tenancy filter applied. We have to explicitly set that here to make the api call.
     * Sanity checks for availability and status are made prior to returning the system.
     *
     * <em>Note:</em> no access control checks are made for user access to the system. This is just a lookup. For a
     * combined lookup with access check, see the {@link #getUserSystemBySystemId(String, String, String)}.
     * @param tenantId the id of the tenant containing the system
     * @param systemId the system id of the {@link RemoteSystem}
     * @return the remote system
     * @throws SystemUnavailableException when the system status is down or has been disabled
     * @throws SystemUnknownException if no system with the given id in the given tenant exists
     */
    public RemoteSystem getSystemBySystemId(String tenantId, String systemId) throws SystemUnknownException, SystemUnavailableException {
        SystemDao systemDao = new SystemDao();
        TenancyHelper.setCurrentTenantId(tenantId);
        RemoteSystem system;
        try {
            system = systemDao.findBySystemId(systemId);
        } catch (HibernateException t) {
            throw new SystemUnknownException("Unable to lookup system to to persistence issue", t);
        }

        if (system == null) {
            throw new SystemUnknownException("No system found for tenant " + tenantId + " with id " + systemId);
        } else if (!system.isAvailable()) {
            throw new SystemUnavailableException("System " + systemId + " has been disabled and is not available for use");
        } else if (system.getStatus() == SystemStatusType.DOWN || system.getStatus() == SystemStatusType.MAINTENANCE) {
            throw new SystemUnavailableException("System " + systemId + " is currently unavailable due to maintenance. " +
                    "This transfer will resume once the system returns from maintenance mode.");
        }

        return system;
    }


    /**
     * Fetches the {@link RemoteSystem} with given {@code systemId} in the {@code tenantId} tenant where the given
     * {@code username} has at least a {@link RoleType#GUEST} role. Sanity checks for availability and status are made
     * prior to returning the system.
     * @param tenantId the id of the tenant containing the system
     * @param username the user to check for system access
     * @param systemId the system id of the {@link RemoteSystem}
     * @return the remote system
     * @throws SystemUnavailableException when the system status is down or has been disabled
     * @throws SystemUnknownException if no system with the given id in the given tenant exists
     */
    public RemoteSystem getUserSystemBySystemId(String tenantId, String username, String systemId) throws SystemUnknownException, SystemUnavailableException {
        SystemDao systemDao = new SystemDao();
        TenancyHelper.setCurrentTenantId(tenantId);
        RemoteSystem system;
        try {
            system = systemDao.findUserSystemBySystemId(username, systemId);
        } catch (HibernateException t) {
            throw new SystemUnknownException("Unable to lookup system to to persistence issue", t);
        }

        if (system == null) {
            throw new SystemUnknownException("No system found for tenant " + tenantId + " with id " + systemId);
        } else if (!system.isAvailable()) {
            throw new SystemUnavailableException("System " + systemId + " has been disabled and is not available for use");
        } else if (system.getStatus() == SystemStatusType.DOWN || system.getStatus() == SystemStatusType.MAINTENANCE) {
            throw new SystemUnavailableException("System " + systemId + " is currently unavailable due to maintenance. " +
                    "This transfer will resume once the system returns from maintenance mode.");
        }

        return system;
    }

    /**
     * Checks whether user has access to the {@link RemoteSystem} with the given {@code systemId} in the {@code tenantId}
     * and with at least the {@code minimumRole}.
     * @param tenantId the id of the tenant containing the system
     * @param username the user to check for system access
     * @param systemId the system id of the {@link RemoteSystem}
     * @param minimumRole the minimum {@link RoleType} to check the user role against.
     * @return true if the {@code minimumRole} is less than or equal to the user's role on the system.
     * @throws SystemUnavailableException when the system status is down or has been disabled
     * @throws SystemUnknownException if no system with the given id in the given tenant exists
     */
    public boolean userHasMinimumRoleOnSystem(String tenantId, String username, String systemId, RoleType minimumRole) throws SystemRoleException, SystemUnknownException, SystemUnavailableException {
        RemoteSystem system = getSystemBySystemId(tenantId, systemId);
        SystemRoleManager systemRoleManager = new SystemRoleManager(system);
        SystemRole role = systemRoleManager.getUserRole(username);

        // ensure the existing role grant is at least that of the minimum role required to access the data
        return role.getRole().compareTo(minimumRole) >= 0;
    }
}

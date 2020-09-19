package org.iplantc.service.metadata.managers;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.events.MetadataEventProcessor;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.notification.managers.NotificationManager;

import java.util.List;

/**
 * Management class for handling operations on {@link MetadataItem} objects.
 * The constructor assigns a calling user to whom all events resulting from
 * permission operations will be assigned. Each permission event results in a
 * unique event being thrown.
 *
 * @author dooley
 */
public class MetadataPermissionManager {

    private final String authenticatedUsername;
    private final MetadataEventProcessor eventProcessor = new MetadataEventProcessor();
    private final MetadataDao metadataDao = new MetadataDao();
    private MetadataItem metadataItem;

    /**
     * Base constructor binding a {@link MetadataItem} to a new
     * instance of this {@link MetadataPermissionManager}.
     *
     * @param metadataItem                  the {@link MetadataItem} to which permission checks apply
     * @param authenticatedUsername the username of the user responsible for invoking methods on the {@code metadataItem}
     * @throws MetadataException if the argumetns are invalid
     */
    public MetadataPermissionManager(MetadataItem metadataItem, String authenticatedUsername) throws MetadataException {
        if (metadataItem == null) {
            throw new MetadataException("Metadata item cannot be null");
        }
        this.metadataItem = metadataItem;
        if (authenticatedUsername == null) {
            throw new MetadataException("Authenticated username cannot be null");
        }
        this.authenticatedUsername = authenticatedUsername;
    }

    /**
     * Checks whether the given {@code username} has the given {@code jobPermissionType}
     * for the the {@link MetadataItem} associated with this permission manager.
     *
     * @param username          the user to whom the permission will be checked
     * @param desiredPermissionType the permission to check
     * @return
     * @throws MetadataException
     */
    public boolean hasPermission(String username,
                                 PermissionType desiredPermissionType) throws MetadataException {

        if (StringUtils.isBlank(username)) {
            return false;
        }

        if (getAuthenticatedUsername().equals(username) || AuthorizationHelper.isTenantAdmin(username))
            return true;

        for (MetadataPermission pem : getMetadataItem().getPermissions()) {
            if (List.of(username, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME).contains(pem.getUsername())
                    && pem.getPermission().equals(desiredPermissionType)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks whether the given {@code username} has {@link PermissionType#READ},
     * {@link PermissionType#READ_WRITE}, or {@link PermissionType#ALL}
     * for the the {@link MetadataItem} associated with this permission manager.
     *
     * @param username the user to whom the permission will be checked
     * @return true if they have read permission, false otherwise
     */
    public boolean canRead(String username) {

        if (StringUtils.isBlank(username)) {
            return false;
        }

        if (StringUtils.equals(getAuthenticatedUsername(), username) || AuthorizationHelper.isTenantAdmin(username)
                || StringUtils.equals(getMetadataItem().getOwner(), username))
            return true;

        for (MetadataPermission pem : getMetadataItem().getPermissions()) {
            if (List.of(username, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME).contains(pem.getUsername())
                    && pem.canRead()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks whether the given {@code username} has {@link PermissionType#WRITE},
     * {@link PermissionType#READ_WRITE}, or {@link PermissionType#ALL}
     * for the the {@link MetadataItem} associated with this permission manager.
     *
     * @param username the user to whom the permission will be checked
     * @return true if they have write permission, false otherwise
     */
    public boolean canWrite(String username) {

        if (StringUtils.isBlank(username)) {
            return false;
        }

        if (getAuthenticatedUsername().equals(username) || AuthorizationHelper.isTenantAdmin(username)
                || getMetadataItem().getOwner().equals(username))
            return true;

        for (MetadataPermission pem : getMetadataItem().getPermissions()) {
            if (List.of(username, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME).contains(pem.getUsername())
                    && pem.canWrite()) {
                return true;
            }
        }

        return false;
    }


    /**
     * Assigns the given {@code sPermission} to the given {@code username}
     * for the the {@link MetadataItem} associated with this permission manager.
     *
     * @param username    the user to whom the permission will be granted
     * @param sPermission the permission to set
     * @throws MetadataException
     * @throws PermissionException if the permission value is invalid
     */
    public void setPermission(String username, String sPermission)
            throws MetadataException, PermissionException, MetadataStoreException {
        if (StringUtils.isBlank(username)) {
            throw new MetadataException("Invalid username");
        }

        if (getAuthenticatedUsername().equals(username))
            return;

        if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
            StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
            boolean worldAdmin = JWTClient.isWorldAdmin();
            boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
            if (!tenantAdmin && !worldAdmin) {
                throw new PermissionException("User does not have permission to edit public metadata item permissions");
            }
        }

        MetadataPermission userPermission = getMetadataItem().getPermissionForUsername(username);

        // if the permission is empty or null, delete it
        if (StringUtils.isEmpty(sPermission) || sPermission.equalsIgnoreCase("none")) {
            // delete the permission if it exists
            if (userPermission != null) {
                getMetadataItem().removePermission(userPermission);
                getMetadataItem().setPermissions(getMetadataDao().updatePermission(getMetadataItem()));

                // getEventProcessor().processPermissionEvent(getMetadataItem().getUuid(), pem, MetadataEventType.PERMISSION_REVOKE, getAuthenticatedUsername(), new MetadataDao().getByUuid(getMetadataItem().getUuid()).toJSON());
                NotificationManager.process(getMetadataItem().getUuid(), MetadataEventType.PERMISSION_REVOKE.name(), username);
            } else {
                // otherwise do nothing, no permission existed before or after
            }
        }
        // they're updating/adding a permission, so resolve the permission and
        // and alert the appropriate subscriptions
        else {
            PermissionType permissionType = PermissionType.
//                    valueOf(sPermission.toUpperCase());
                    getIfPresent(sPermission.toUpperCase());    //handles invalid permission types

            if (permissionType == PermissionType.UNKNOWN) {
                throw new PermissionException("Unable to set unknown permission, please use " +
                        PermissionType.READ.toString() + ", " +
                        PermissionType.READ_WRITE.toString() + ", " +
                        PermissionType.NONE.toString());
            }

            // if not present, add it
            if (userPermission == null) {
                userPermission = new MetadataPermission(username, permissionType);

                getMetadataItem().getPermissions().add(userPermission);
                getMetadataItem().setPermissions(getMetadataDao().updatePermission(getMetadataItem()));

                // getEventProcessor().processPermissionEvent(getMetadataItem().getUuid(), pem, MetadataEventType.PERMISSION_GRANT, getAuthenticatedUsername(), new MetadataDao().getByUuid(getMetadataItem().getUuid()).toJSON());
                NotificationManager.process(getMetadataItem().getUuid(), MetadataEventType.PERMISSION_GRANT.name(), username);
            }
            // otherwise, update the existing permission
            else {
                userPermission.setPermission(permissionType);
                getMetadataItem().updatePermissions(userPermission);
                getMetadataItem().setPermissions(getMetadataDao().updatePermission(getMetadataItem()));

//                this.metadataItem = getMetadataDao().findSingleMetadataItem();
                // getEventProcessor().processPermissionEvent(getMetadataItem().getUuid(), pem, MetadataEventType.PERMISSION_UPDATE, getAuthenticatedUsername(), new MetadataDao().getByUuid(getMetadataItem().getUuid()).toJSON());
                NotificationManager.process(getMetadataItem().getUuid(), MetadataEventType.PERMISSION_UPDATE.name(), username);
            }
        }
    }

    /**
     * Removes all permissions, save ownership
     *
     * @throws MetadataException
     */
    public void clearPermissions() throws MetadataException, MetadataStoreException {
        getMetadataItem().getPermissions().clear();
        getMetadataItem().setPermissions(getMetadataDao().updatePermission(getMetadataItem()));

        NotificationManager.process(getMetadataItem().getUuid(), "PERMISSION_REVOKE", getAuthenticatedUsername());
    }

    /**
     * Fetches the user permission for the the {@link MetadataItem} associated with this
     * permission manager.
     *
     * @param username
     * @return the assigned permission for the user
     * @throws MetadataException
     */
    public MetadataPermission getPermission(String username) throws MetadataException {
        MetadataPermission pem = new MetadataPermission(username, PermissionType.NONE);

        if (StringUtils.isBlank(username)) {
            return pem;
        } else if (getAuthenticatedUsername().equals(username) || AuthorizationHelper.isTenantAdmin(username)) {
            pem.setPermission(PermissionType.ALL);
            return pem;
        } else {
            for (MetadataPermission dbPems : getMetadataItem().getPermissions()) {
                if (dbPems.getUsername().equals(username)) {
                    return dbPems;
                } else if (dbPems.getUsername().equals(Settings.WORLD_USER_USERNAME) ||
                    dbPems.getUsername().equals(Settings.PUBLIC_USER_USERNAME)) {
                    pem = dbPems;
                }
            }
            return pem;
        }
    }

    /**
     * The user responsible for making the permission manager requests
     *
     * @return the authenticatedUsername
     */
    public String getAuthenticatedUsername() {
        return authenticatedUsername;
    }

//    /**
//     * @param authenticatedUsername the authenticatedUsername to set
//     */
//    public void setAuthenticatedUsername(String authenticatedUsername) {
//        this.authenticatedUsername = authenticatedUsername;
//    }

    /**
     * @return the eventProcessor
     */
    public MetadataEventProcessor getEventProcessor() {
        return eventProcessor;
    }

//    /**
//     * @param eventProcessor the eventProcessor to set
//     */
//    public void setEventProcessor(MetadataEventProcessor eventProcessor) {
//        this.eventProcessor = eventProcessor;
//    }

    public void setMetadataItem(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
    }

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }

    public MetadataDao getMetadataDao() {
        return metadataDao;
    }
}

package org.iplantc.service.metadata.managers;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.util.ServiceUtils;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;

public class MetadataItemPermissionManager {

    private List<String> accessibleOwners;
    private String authenticatedUsername;
    private String metadataUUID;
    MetadataDao metadataDao;


    public MetadataItemPermissionManager(String user, String uuid) {
        this.metadataDao = new MetadataDao().getInstance();
        this.authenticatedUsername = user;
        this.metadataUUID = uuid;
    }


    public String getAuthenticatedUsername() {
        return authenticatedUsername;
    }

    public void setAuthenticatedUsername(String authenticatedUsername) {
        this.authenticatedUsername = authenticatedUsername;
    }

    public String getMetadataUUID() {
        return metadataUUID;
    }

    public void setMetadataUUID(String metadataUUID) {
        this.metadataUUID = metadataUUID;
    }

    public MetadataDao getMetadataDao() {
        return metadataDao;
    }

    public void setMetadataDao(MetadataDao metadataDao) {
        this.metadataDao = metadataDao;
    }

    /**
     * @return List of users who are tenant admins or the owner for the {@link MetadataItem}
     */
    public List<String> setAccessibleOwnersExplicit() {
        this.accessibleOwners = Arrays.asList(this.authenticatedUsername,
                Settings.PUBLIC_USER_USERNAME,
                Settings.WORLD_USER_USERNAME);

        return this.accessibleOwners;
    }

    /**
     * @return List of the authenticated user for the {@link MetadataItem} for implicit search queries
     */
    public List<String> setAccessibleOwnersImplicit() {
        if (this.accessibleOwners == null)
            this.accessibleOwners = new ArrayList<>();
        this.accessibleOwners.add(this.authenticatedUsername);
        return this.accessibleOwners;
    }

    /**
     * Check if authenticated user has read permissions for the given uuid
     *
     * @return True if authenticated user has read permissions for the given uuid
     */
    public boolean canRead() {
        if (!ServiceUtils.isValid(this.authenticatedUsername)) {
            return false;
        }

        if (ServiceUtils.isAdmin(this.authenticatedUsername))
            return true;

        return metadataDao.hasRead(this.authenticatedUsername, this.metadataUUID);
    }

    /**
     * Check if authenticated user has write permissions for the given uuid
     *
     * @return True if authenticated user has write permissions for the given uuid
     */
    public boolean canWrite() {
        if (!ServiceUtils.isValid(this.authenticatedUsername)) {
            return false;
        }

        if (ServiceUtils.isAdmin(this.authenticatedUsername))
            return true;

        return metadataDao.hasWrite(this.authenticatedUsername, this.metadataUUID);
    }

    /**
     * Retrieve the {@link MetadataItem} corresponding to the {@code metadataUUID}
     *
     * @return {@link MetadataItem} matching with {@code metadataUUID}, null if none matching
     */
    private MetadataItem getMetadataItem() {
        return metadataDao.findSingleMetadataItem(eq("uuid", this.metadataUUID));
    }

    /**
     * Create/Update permission for user
     *
     * @param permissionToUpdate
     * @throws MetadataException
     * @throws PermissionException
     */
    public List<MetadataPermission> updatePermissions(MetadataPermission permissionToUpdate) throws MetadataException, PermissionException, MetadataStoreException {
        if (permissionToUpdate == null)
            throw new MetadataException("Metadata permission cannot be null.");

        if (permissionToUpdate.getPermission().equals(PermissionType.UNKNOWN))
            throw new MetadataException("Unknown metadata permission.");

        metadataDao.setAccessibleOwners(this.accessibleOwners);

        //check if user has write permissions
        if (canWrite()) {
            MetadataItem itemToUpdate = getMetadataItem();
            if (itemToUpdate == null)
                throw new MetadataException("No Metadata item found with uuid " + this.metadataUUID);

            if (permissionToUpdate.getPermission().equals(PermissionType.NONE) || permissionToUpdate == null) {
                MetadataPermission pemDelete = itemToUpdate.getPermissions_User(permissionToUpdate.getUsername());
                if (pemDelete == null)
                    return null; //nothing to delete
                itemToUpdate.updatePermissions_delete(pemDelete);
            } else {
                itemToUpdate.updatePermissions(permissionToUpdate);
            }
            return metadataDao.updatePermission(itemToUpdate, this.authenticatedUsername);
        } else {
            throw new PermissionException("User does not have sufficient permissions to update metadata item.");
        }
    }


    /**
     * Return all permissions for the user
     *
     * @param user to find permission for
     * @return list of {@link MetadataItem} where the user was specified permissions; null if the user making the query
     * does not have permission to view the metadata item
     */
    public List<MetadataItem> findPermission_User(String user) throws PermissionException {
        if (canRead()) {
            //only the owner or tenantAdmin can access if no permissions are explicitly set
            metadataDao.setAccessibleOwners(this.accessibleOwners);
            return metadataDao.find(this.authenticatedUsername, and(eq("uuid", this.metadataUUID),
                    or(eq("owner", user), eq("permissions.username", user))));
        } else {
            throw new PermissionException("User does not have sufficient permissions to update metadata item.");
        }
    }

    /**
     * Return all permissions for given uuid
     *
     * @return list of {@link MetadataPermission} for the provided uuid
     */
    public List<MetadataPermission> findPermission_Uuid() throws PermissionException {
        if (canRead()) {
            metadataDao.setAccessibleOwners(this.accessibleOwners);
            List<MetadataItem> metadataItemList = metadataDao.find(this.authenticatedUsername, eq("uuid", this.metadataUUID));
            if (metadataItemList.size() == 0)
                return null;
            return metadataItemList.get(0).getPermissions();
        } else {
            throw new PermissionException("User does not have sufficient permissions to update metadata item.");
        }
    }
}

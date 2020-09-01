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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;

public class MetadataItemPermissionManager {
    private List<String> accessibleOwners;
    private String authenticatedUsername;
    MetadataDao metadataDao;


    public MetadataItemPermissionManager(String user) {
        this.metadataDao = new MetadataDao().getInstance();
        this.authenticatedUsername = user;
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
     * Check if user has correct permissions, throw Permission Exception if the user doesn't have the
     * correct permission
     *
     * @param pem      user's Permission type
     * @param username user to check
     * @param bolWrite true for read/write permission, false for read permission
     * @throws PermissionException if the user does not have permissions to read or write
     * @throws MetadataException   if the {@code username} is blank
     */
    public void checkPermission(PermissionType pem, String username, boolean bolWrite) throws PermissionException, MetadataException {
        if (StringUtils.isBlank(username)) {
            throw new MetadataException("Invalid username");
        }

        if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
                StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
            boolean worldAdmin = JWTClient.isWorldAdmin();
            boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
            if (!tenantAdmin && !worldAdmin) {
                throw new PermissionException("User does not have permission to edit public metadata item permissions");
            }
        }

        if (bolWrite) {
            if (!Arrays.asList(PermissionType.READ_WRITE, PermissionType.WRITE, PermissionType.ALL).contains(pem)) {
                throw new PermissionException("user does not have permission to edit public metadata item.");
            }
        } else {
            if (pem == PermissionType.NONE) {
                throw new PermissionException("user does not have permission to edit public metadata item.");
            }
        }
    }

    /**
     * Check if authenticated user has read permissions for the given uuid
     *
     * @param uuid to check permissions for
     * @return True if authenticated user has read permissions for the given uuid
     */
    public boolean canRead(String uuid) {
        return metadataDao.hasRead(this.authenticatedUsername, uuid);
    }

    /**
     * Check if authenticated user has write permissions for the given uuid
     *
     * @param uuid to check permissions for
     * @return True if authenticated user has write permissions for the given uuid
     */
    public boolean canWrite(String uuid) {
        return metadataDao.hasWrite(this.authenticatedUsername, uuid);
    }

//    /**
//     * Add/update the user's permission to {@code pem}
//     *
//     * @param userToUpdate String user to update
//     * @param group        group to be updated
//     * @param pem          {@link PermissionType} to be updated to
//     * @throws MetadataException      if unable to update the permission of {@code user}
//     * @throws MetadataStoreException if the connection cannot be found/created, or db connection is bad
//     */
//    public void updatePermissions(String userToUpdate, String group, PermissionType pem) throws MetadataException {
//        MetadataPermission metadataPermission;
//        //check if user has write permissions
//        if (pem.equals(PermissionType.NONE) || pem == null) {
//            //delete permission
//            MetadataPermission pemDelete = metadataItem.getPermissions_User(userToUpdate);
//            metadataItem.updatePermissions_delete(pemDelete);
//            List<MetadataPermission> metadataPermissionsList = metadataItem.getPermissions();
//
//        } else {
//            metadataPermission = new MetadataPermission(metadataItem.getUuid(), userToUpdate, pem);
//            metadataPermission.setGroup(group);
//
//            metadataItem.updatePermissions(metadataPermission);
//        }
//        metadataDao.updatePermission(metadataItem, this.authenticatedUsername);
//    }


    /**
     * Create/Update permission for user
     *
     * @param userToUpdate
     * @param permissionToUpdate
     * @param metadataItemToUpdate MetadataItem to update
     * @throws MetadataException
     * @throws PermissionException
     */
    public List<MetadataPermission> updatePermissions(String userToUpdate, MetadataPermission permissionToUpdate, MetadataItem metadataItemToUpdate) throws MetadataException, PermissionException, MetadataStoreException {
        if (permissionToUpdate == null )
            throw new MetadataException("Metadata permission cannot be null.");

        if (permissionToUpdate.getPermission().equals(PermissionType.UNKNOWN))
            throw new MetadataException("Unknown metadata permission.");


        //check if user has write permissions
        if (canWrite(metadataItemToUpdate.getUuid())) {
            metadataDao.setAccessibleOwners(this.accessibleOwners);

            if (permissionToUpdate.getPermission().equals(PermissionType.NONE) || permissionToUpdate == null) {
                //delete permission
                MetadataPermission pemDelete = metadataItemToUpdate.getPermissions_User(userToUpdate);
                if (pemDelete == null)
                    return null; //nothing to delete
                metadataItemToUpdate.updatePermissions_delete(pemDelete);
            } else {
                metadataItemToUpdate.updatePermissions(permissionToUpdate);
            }
            return metadataDao.updatePermission(metadataItemToUpdate, this.authenticatedUsername);
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
    public List<MetadataItem> findPermission_User(String user, String uuid) {
        //only the owner or tenantAdmin can access if no permissions are explicitly set
        metadataDao.setAccessibleOwners(this.accessibleOwners);
        return metadataDao.find(this.authenticatedUsername, and(eq("uuid", uuid),
                or(eq("owner", user), eq("permissions.username", user))));
    }

    /**
     * Return all permissions for given uuid
     *
     * @param uuid to search for
     * @return list of {@link MetadataPermission} for the provided uuid
     */
    public MetadataItem findPermission_Uuid(String uuid) {
        metadataDao.setAccessibleOwners(this.accessibleOwners);
        List<MetadataItem> metadataItemList = metadataDao.find(this.authenticatedUsername, eq("uuid", uuid));
        if (metadataItemList.size() == 0)
            return null;
        return metadataItemList.get(0);
    }
}

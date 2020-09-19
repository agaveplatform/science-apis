package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MetadataSearch {
    private String value;
    private String username;
    private MetadataAssociationList associationList;
    private String orderField;
    private int orderDirection;
    private int offset;
    private int limit;
    private MetadataItem metadataItem;
    private ArrayNode notifications;
    private ArrayNode permissions;
    private List<String> accessibleOwners;


    MetadataDao metadataDao;
//    private Bson dbQuery;

    public MetadataSearch(String username) {
//        this.bolImplicitPermissions = bolImplicitPermissions;
        this.metadataItem = new MetadataItem();
        metadataItem.setTenantId(TenancyHelper.getCurrentTenantId());
        this.username = username;
        this.metadataDao = new MetadataDao().getInstance();
        this.limit = org.iplantc.service.common.Settings.DEFAULT_PAGE_SIZE;
        this.offset = 0;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getOwner() {
        return this.metadataItem.getOwner();
    }

    public void setOwner(String owner) {
        this.metadataItem.setOwner(owner);
    }

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }

    public void setMetadataItem(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
    }

    public String getUuid() {
        return this.metadataItem.getUuid();
    }

    public void setUuid(String uuid) {
        this.metadataItem.setUuid(uuid);
    }

    public void setOrderField(String orderField) {
        this.orderField = orderField;
    }

    public void setOrderDirection(int orderDirection) {
        this.orderDirection = orderDirection;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public ArrayNode getNotifications() {
        return notifications;
    }

    public ArrayNode getPermissions() {
        return permissions;
    }

    /**
     * Remove all documents from the Metadata collection
     * @deprecated
     */
    public void clearCollection() {
        metadataDao.clearCollection();
    }

    /**
     * Find the {@link MetadataItem} matching the {@code userQuery} and the offset/limit specified
     *
     * @param userQuery String query to search the collection for
     * @return list of {@link MetadataItem} matching the {@code userQuery} in the sort order specified
     * @throws MetadataQueryException if {@code userQuery} is invalid format
     * @deprecated  use find (Bson userQuery)
     */
    public List<MetadataItem> find(String userQuery) throws MetadataQueryException {
        List<MetadataItem> result = new ArrayList<>();
        try {
            Document doc = new JsonHandler().parseUserQueryToDocument(userQuery);
            Bson permissionFilter = and(getTenantIdQuery(), doc);

            BasicDBObject order = (orderField == null) ? new BasicDBObject() : new BasicDBObject(orderField, orderDirection);
            metadataDao.setAccessibleOwners(this.accessibleOwners);

            result = metadataDao.find(this.username, permissionFilter, offset, limit, order);

        } catch (MetadataQueryException e) {
            throw new MetadataQueryException("Unable to parse query.");
        }
        return result;
    }

    public List<MetadataItem> find(Bson userQuery) throws MetadataStoreException {
        try {
            Bson permissionFilter = and(getTenantIdQuery(), userQuery);

            BasicDBObject order = (orderField == null) ? new BasicDBObject() : new BasicDBObject(orderField, orderDirection);
            metadataDao.setAccessibleOwners(this.accessibleOwners);

             return metadataDao.find(this.username, permissionFilter, offset, limit, order);

        } catch (Exception e) {
            throw new MetadataStoreException("Unable to find resource based on query.");
        }
    }

    /**
     * @deprecated
     * Find all documents in the collection
     *
     * @return list of {@link MetadataItem} found
     * @throws UnknownHostException if the connection cannot be found/created, or db connection is bad
     */
    public List<MetadataItem> findAll() {
        return metadataDao.findAll();
    }

    /**
     * Find all documents in the collection
     *
     * @return list of {@link MetadataItem} found
     * @throws UnknownHostException if the connection cannot be found/created, or db connection is bad
     */
    public List<MetadataItem> filterFindAll(String[] filters) {
        return metadataDao.findAll();
    }


    /**
     * Find single document in the collection
     * matching the {@link MetadataItem} uuid and tenantId
     *
     * @return {@link MetadataItem} matching the criteria
     */
    public MetadataItem findOne() {
        return metadataDao.findSingleMetadataItem(and(eq("uuid", this.getUuid()),
                eq("tenantId", this.metadataItem.getTenantId())));
    }

    /**
     * Find a single document in the collection
     *
     * @param query to search the collection with
     * @return {@link MetadataItem} matching the criteria
     */
    public MetadataItem findOne(Bson query) throws MetadataQueryException {
//        Document docQuery = getDocumentFromQuery(query);
        return metadataDao.findSingleMetadataItem(query);
    }


    /**
     * Find all documents matching {@code query} with fields specified by {@code filters}
     *
     * @param query   to search the collection with
     * @param filters {@link List} of fields to filter result with
     * @return {@link List} of {@link Document} matching the query
     */
    public List<Document> filterFind(String query, String[] filters) throws MetadataQueryException {
        Document docQuery = new JsonHandler().parseUserQueryToDocument(query);
        Document docFilter = parseDocumentfromList(filters);
        return metadataDao.filterFind(docQuery, docFilter);
    }

    /**
     * Find one document matching {@link Bson} query with fields specified by {@code filters}
     *
     * @param query   {@link Bson} query to search the collection with
     * @param filters {@link List} of fields to filter result with
     * @return {@link Document} matching the query, null if nothing is found
     */
    public Document filterFindOne(Bson query, String[] filters) {
        Document docFilter = parseDocumentfromList(filters);

        List<Document> foundDocuments = metadataDao.filterFind(query, docFilter);
        if (foundDocuments.size() > 0)
            return foundDocuments.get(0);
        return null;
    }

    /**
     * Parse String List to a Document with String key and value of 1
     *
     * @param filters List of String specifying fields to filter by
     * @return Document with the each String as the key with value of 1
     */
    public Document parseDocumentfromList(String[] filters) {
        Document docFilter = new Document();
        if (filters.length > 0) {
            for (String filter : filters) {
                docFilter.append(filter, 1);
            }
        }

        //don't include mongo id
        docFilter.append("_id", 0);
        return docFilter;
    }

    public MetadataItem insertCurrentMetadataItem() throws MetadataStoreException {
        setMetadataItem(metadataDao.insert(getMetadataItem()));
        return getMetadataItem();
    }

    /**
     * Update collection using {@link MetadataItem}, replaces the existing item
     *
     * @return {@link MetadataItem} that was updated successfully
     * @throws MetadataException   if unable to update the {@link MetadataItem} in the metadata collection
     * @throws PermissionException if user does not have permission to update the {@link MetadataItem}
     * @deprecated
     */
    public MetadataItem updateCurrentMetadataItem() throws MetadataException, PermissionException {
        metadataDao.setAccessibleOwners(this.accessibleOwners);
        MetadataItem currentMetadata = metadataDao.findSingleMetadataItem(and(eq("uuid", this.metadataItem.getUuid()),
                eq("tenantId", this.metadataItem.getTenantId())));

        this.metadataItem.setCreated(currentMetadata.getCreated());
        this.metadataItem.setTenantId(currentMetadata.getTenantId());
        this.metadataItem.setOwner(currentMetadata.getOwner());
//        this.setOwner(currentMetadata.getOwner());
        this.metadataItem.setPermissions(currentMetadata.getPermissions());

        setMetadataItem(metadataDao.updateMetadata(this.metadataItem, this.username));

        return getMetadataItem();
    }

    /** Update collection using {@link Document} doc, merges with the existing item
     *
     * @param doc {@link Document} to update Metadata Item with
     * @return updated {@link Document}
     * @throws MetadataException
     * @throws PermissionException
     */
    public Document updateMetadataItem(Document doc) throws MetadataException {
        metadataDao.setAccessibleOwners(this.accessibleOwners);
        doc = updateLastUpdatedDateForDocument(doc);
        doc.append("uuid", getUuid());
        doc.append("tenantId", TenancyHelper.getCurrentTenantId());
        return metadataDao.updateDocument(doc);
    }

    /**
     * Append to {@code doc} the lastUpdated time
     * @param doc {@link Document} to update
     * @return updated {@link Document}
     */
    public Document updateLastUpdatedDateForDocument(Document doc){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        doc.append("lastUpdated", formatter.format(new Date()));
        return doc;
    }
    /**
     * Delete the metadata item
     *
     * @return {@link MetadataItem} that was deleted successfully, null otherwise
     * @throws PermissionException if user does not have permission to update the {@link MetadataItem}
     */
    public MetadataItem deleteCurrentMetadataItem() throws PermissionException, MetadataException {
        if (getMetadataItem().getPermissionForUsername(this.username).canWrite()){
            metadataDao.setAccessibleOwners(this.accessibleOwners);
            setMetadataItem(metadataDao.deleteMetadata(this.metadataItem));
            return getMetadataItem();
        } else {
            throw new PermissionException("User does not have permission to modify this resource.");
        }
    }

    //---------------------------------------------

    //------  Helpers ---------------

    /**
     * @return {@link Bson} filter for the current tenantId
     */
    public Bson getTenantIdQuery() {
        return eq("tenantId", this.metadataItem.getTenantId());
    }

    //------  Permission handling ---------------

    /**
     * @return List of users who are tenant admins or the owner for the {@link MetadataItem}
     */
    public List<String> setAccessibleOwnersExplicit() {
        this.accessibleOwners = Arrays.asList(this.username,
                Settings.PUBLIC_USER_USERNAME,
                Settings.WORLD_USER_USERNAME);

        return this.accessibleOwners;
    }

    public List<String> setAccessibleOwnersImplicit() {
        if (this.accessibleOwners == null)
            this.accessibleOwners = new ArrayList<>();
        this.accessibleOwners.add(this.username);
        return this.accessibleOwners;
    }
//    /**
//     * Check if user has correct permissions, throw Permission Exception if the user doesn't have the
//     * correct permission
//     *
//     * @param pem      user's Permission type
//     * @param username user to check
//     * @param bolWrite true for read/write permission, false for read permission
//     * @throws PermissionException if the user does not have permissions to read or write
//     * @throws MetadataException   if the {@code username} is blank
//     */
//    public void checkPermission(PermissionType pem, String username, boolean bolWrite) throws PermissionException, MetadataException {
//        if (StringUtils.isBlank(username)) {
//            throw new MetadataException("Invalid username");
//        }
//
//        if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
//                StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
//            boolean worldAdmin = JWTClient.isWorldAdmin();
//            boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
//            if (!tenantAdmin && !worldAdmin) {
//                throw new PermissionException("User does not have permission to edit public metadata item permissions");
//            }
//        }
//
//        if (bolWrite) {
//            if (!Arrays.asList(PermissionType.READ_WRITE, PermissionType.WRITE, PermissionType.ALL).contains(pem)) {
//                throw new PermissionException("user does not have permission to edit public metadata item.");
//            }
//        } else {
//            if (pem == PermissionType.NONE) {
//                throw new PermissionException("user does not have permission to edit public metadata item.");
//            }
//        }
//    }
//
//    /**
//     * Add/update the user's permission to {@code pem}
//     *
//     * @param userToUpdate String user to update
//     * @param group        group to be updated
//     * @param pem          {@link PermissionType} to be updated to
//     * @throws MetadataException      if unable to update the permission of {@code user}
//     * @throws MetadataStoreException if the connection cannot be found/created, or db connection is bad
//     */
//    public void updatePermissions(String userToUpdate, String group, PermissionType pem) throws MetadataException, MetadataStoreException, PermissionException {
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
//        metadataDao.updatePermission(metadataItem, this.username);
//    }
//
//    /**
//     * Return all permissions for the user
//     *
//     * @param user to find permission for
//     * @return list of {@link MetadataItem} where the user was specified permissions; null if the user making the query
//     * does not have permission to view the metadata item
//     */
//    public List<MetadataItem> findPermission_User(String user, String uuid) {
//        //only the owner or tenantAdmin can access if no permissions are explicitly set
//        metadataDao.setAccessibleOwners(this.accessibleOwners);
//
//        if (metadataDao.hasRead(this.username, uuid)) {
//            return metadataDao.find(this.username, and(eq("uuid", uuid),
//                    or(eq("owner", user), eq("permissions.username", user))));
//        }
//        else
//            return null;
//    }
//
//    /**
//     * Return all permissions for given uuid
//     *
//     * @param uuid to search for
//     * @return list of {@link MetadataPermission} for the provided uuid
//     */
//    public MetadataItem findPermission_Uuid(String uuid) {
//        List<MetadataPermission> permissionList = new ArrayList<>();
//        List<MetadataItem> metadataItemList = metadataDao.find(this.username, eq("uuid", uuid));
//        return metadataItemList.get(0);
//    }
    //------------------------------



}

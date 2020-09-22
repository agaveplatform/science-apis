package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;

import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MetadataSearch {
    private String username;
    private String orderField;
    private int orderDirection;
    private int offset;
    private int limit;
    private MetadataItem metadataItem;
    private List<String> accessibleOwners;

    MetadataDao metadataDao;

    public MetadataSearch(String username) {
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

    /**
     * Find the {@link MetadataItem} matching the {@code userQuery} and the offset/limit specified
     *
     * @param userQuery {@link Bson} query to search collection for
     * @return list of {@link MetadataItem} matching the {@code userQuery} in the sort order specified
     * @throws MetadataStoreException if {@code userQuery} is unable to find matching {@link MetadataItem}
     *
     *
     */
    public List<MetadataItem> find(Bson userQuery) throws MetadataStoreException {
        try {
            Bson permissionFilter = and(eq("tenantId", TenancyHelper.getCurrentTenantId()), userQuery);

            Document order = (orderField == null) ? new Document() : new Document(orderField, orderDirection);
            getMetadataDao().setAccessibleOwners(this.accessibleOwners);

            return getMetadataDao().find(this.username, permissionFilter, offset, limit, order);

        } catch (Exception e) {
            throw new MetadataStoreException("Unable to find resource based on query.");
        }
    }

    /**
     * Find a single document in the collection
     *
     * @param uuid     the uuid to lookup
     * @param tenantId the tenant to lookup
     * @return {@link MetadataItem} matching the criteria
     */
    public MetadataItem findById(String uuid, String tenantId) throws MetadataException {
        if (StringUtils.isBlank(uuid))
            throw new MetadataException("Uuid cannot be null.");

        if (StringUtils.isBlank(tenantId))
            throw new MetadataException("TenantId cannot be null.");

        Bson query = getMetadataDao().getUuidAndTenantIdQuery(uuid, tenantId);
        return getMetadataDao().findSingleMetadataItem(query);
    }

    /**
     * Find all documents matching {@code query} with fields specified by {@code filters}
     *
     * @param query   to search the collection with
     * @param filters {@link List} of fields to filter result with
     * @return {@link List} of {@link Document} matching the query
     *
     */
    public List<Document> filterFind(Bson query, String[] filters) {
        Document docFilter = parseDocumentFromList(filters);
        Bson withPermissionQuery = and(query, getMetadataDao().getHasReadQuery(username, this.accessibleOwners));

        Document order = (orderField == null) ? new Document() : new Document(orderField, orderDirection);
        getMetadataDao().setAccessibleOwners(this.accessibleOwners);

        return getMetadataDao().filterFind(withPermissionQuery, docFilter, offset, limit, order);
    }

    /**
     * Find one document matching {@link Bson} query with fields specified by {@code filters}
     *
     * @param uuid     the uuid to lookup
     * @param tenantId the tenant to lookup
     * @param filters  String list of fields to show in result
     * @return {@link Document} matching the query, null if nothing is found
     */
    public Document filterFindById(String uuid, String tenantId, String[] filters) throws MetadataException {
        if (StringUtils.isBlank(uuid))
            throw new MetadataException("Uuid cannot be null.");

        if (StringUtils.isBlank(tenantId))
            throw new MetadataException("TenantId cannot be null.");

        Bson query = getMetadataDao().getUuidAndTenantIdQuery(uuid, tenantId);
        List<Document> foundDocuments = filterFind(query, filters);
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
    public Document parseDocumentFromList(String[] filters) {
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

    /**
     * Insert {@link MetadataItem} to the collection
     *
     * @param metadataItem to add
     * @return successfully added {@link MetadataItem}
     * @throws MetadataException      if {@code metadataItem} is null
     * @throws MetadataStoreException when the insertion failed
     */
    public MetadataItem insertMetadataItem(MetadataItem metadataItem) throws MetadataException, MetadataStoreException {
        return getMetadataDao().insert(metadataItem);
    }

    /**
     * Update collection using {@link Document} doc, merges with the existing item
     *
     * @param doc {@link Document} to update Metadata Item with
     * @return updated {@link Document}
     * @throws MetadataException if no {@link Document} found matching the document
     *                           based on the uuid and tenantId
     */
    public Document updateMetadataItem(Document doc, String uuid) throws MetadataException {
        if (doc == null)
            doc = new Document();

        getMetadataDao().setAccessibleOwners(this.accessibleOwners);

//        if (getMetadataDao().hasWrite(this.username, getMetadataItem().getUuid())) {
            doc.append("uuid", uuid);
            doc.append("tenantId", TenancyHelper.getCurrentTenantId());
            return getMetadataDao().updateDocument(doc);
//        } else {
//            throw new PermissionException("User does not have permission to modify this resource.");
//        }

    }

    /**
     * Delete the {@link MetadataItem} matching the given uuid and tenantId
     * @param uuid id of MetadataItem to delete
     * @param tenantId tenant of MetadataItem to delete
     * @return successfully deleted MetadataItem, null if delete unsuccessful
     * @throws MetadataException if
     *
     *
     */
    public MetadataItem deleteMetadataItem(String uuid, String tenantId) throws MetadataException {
        if (StringUtils.isBlank(uuid))
            throw new MetadataException("Uuid cannot be null.");

        if (StringUtils.isBlank(tenantId))
            throw new MetadataException("TenantId cannot be null.");

        getMetadataDao().setAccessibleOwners(this.accessibleOwners);
        //find item to delete
        MetadataItem metadataItem = getMetadataDao().findSingleMetadataItem(
                and(eq("uuid", uuid), eq("tenantId", tenantId)));

        return getMetadataDao().deleteMetadata(metadataItem);
    }

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

    public MetadataDao getMetadataDao() {
        return metadataDao;
    }

    public void setMetadataDao(MetadataDao metadataDao) {
        this.metadataDao = metadataDao;
    }

}

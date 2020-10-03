package org.iplantc.service.metadata.dao;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.iplantc.service.metadata.model.enumerations.PermissionType.ALL;

public class MetadataDao {

    private static final Logger log = Logger.getLogger(MetadataDao.class);

    private MongoDatabase db = null;
    private MongoClients mongoClients = null;
    private com.mongodb.client.MongoClient mongov4Client = null;

    private static MetadataDao dao = null;

    private MetadataItem metadataItem = null;
    private List<String> accessibleOwners = null;

    public MetadataDao(MongoClients paramMongoClients) {
        this.mongoClients = paramMongoClients;
    }

    public MetadataDao() {
    }

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }

    public void setMetadataItem(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
    }

    public List<String> getAccessibleOwners() {
        return accessibleOwners;
    }

    public void setAccessibleOwners(List<String> accessibleOwners) {
        this.accessibleOwners = accessibleOwners;
    }

    public static MetadataDao getInstance() {
        if (dao == null) {
            dao = new MetadataDao();
        }
        return dao;
    }

    public MongoClient getMongoClients() {
        if (mongov4Client == null) {

            ClassModel<JsonNode> valueModel = ClassModel.builder(JsonNode.class).build();
            ClassModel<MetadataPermission> metadataPermissionModel = ClassModel.builder(MetadataPermission.class).build();
            PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(valueModel, metadataPermissionModel).build();
            CodecRegistry registry = CodecRegistries.fromCodecs(new MetadataItemCodec());
            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(pojoCodecProvider),
                    registry);

            mongov4Client = mongoClients.create(MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(
                            new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT))))
                    .credential(getMongoCredential())
                    .codecRegistry(pojoCodecRegistry)
                    .build());
        }
        return mongov4Client;
    }

    /**
     * Creates a new MongoDB credential for the database collections
     *
     * @return valid mongo credential for this instance
     */
    private MongoCredential getMongoCredential() {
        return MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());
    }

    /**
     * Gets the default metadata collection from the default mongodb metatadata db.
     *
     * @return collection from the db
     */
    public MongoCollection getDefaultCollection() {
        return getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
    }

    /**
     * Gets the named collection from the named db.
     *
     * @param dbName         database name
     * @param collectionName collection name
     * @return collection from the db
     */
    public MongoCollection getCollection(String dbName, String collectionName) {

        db = getMongoClients().getDatabase(dbName); //update to 4.0

        // Gets a collection, if it does not exist creates it
        return db.getCollection(collectionName);

    }

    /**
     * Gets the default {@link MetadataItem} configured metadata collection from the default mongodb metadata db
     *
     * @return collection from the db
     */
    public MongoCollection<MetadataItem> getDefaultMetadataItemCollection() {
        return getMetadataItemCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
    }

    /**
     * Get the named collection configured with the {@link MetadataItem} class from the named db
     *
     * @param dbName         database name
     * @param collectionName colletion name
     * @return collection from the db
     */
    public MongoCollection<MetadataItem> getMetadataItemCollection(String dbName, String collectionName) {
        db = getMongoClients().getDatabase(dbName);
        return db.getCollection(collectionName, MetadataItem.class);
    }

    /**
     * Stores the provided {@link MetadataItem} in the mongo collection
     *
     * @param metadataItem the {@link MetadataItem} to be inserted
     * @return the inserted {@link MetadataItem}
     * @throws MetadataStoreException when the insertion failed
     */
    public MetadataItem insert(MetadataItem metadataItem) throws MetadataStoreException, MetadataException {
        if (metadataItem == null)
            throw new MetadataException("Cannot insert a null MetadataItem");

        MongoCollection<MetadataItem> metadataItemCollection;
        try {
            //using POJO Codec
            metadataItemCollection = getDefaultMetadataItemCollection();
            metadataItemCollection.insertOne(metadataItem);

            return metadataItem;
        } catch (MongoException e) {
            throw new MetadataStoreException("Failed to insert metadata item", e);
        }
    }

    /**
     * Find the {@link MetadataItem} from the mongo collection based on the {@link Bson query} filtered
     * by {@code filter}
     *
     * @param query  {@link Bson} query to search with
     * @param filter list of String to specify which fields to return
     * @return List of {@link Document} matching the query criteria
     */
    public List<Document> filterFind(Bson query, Bson filter) {
        return filterFind(query, filter, 0, Settings.DEFAULT_PAGE_SIZE, new Document());
    }

    /**
     * Find the {@link MetadataItem} from the mongo collection based on the {@link Bson query} filtered
     * by {@code filter}
     *
     * @param query  {@link Bson} query to search with
     * @param filter list of String to specify which fields to return
     * @return List of {@link Document} matching the query criteria
     */
    public List<Document> filterFind(Bson query, Bson filter, int offset, int limit, Document order) {
        MongoCursor cursor;
        List<Document> resultList = new ArrayList<>();

        //Don't use custom codecs for faster processing with filters/projections
        MongoCollection mongoCollection;
        mongoCollection = getDefaultCollection();

        if (query == null) {
            query = new Document();
        }

        try {
            cursor = mongoCollection.find(query)
                    .sort(order)
                    .skip(offset)
                    .limit(limit)
                    .projection(filter).cursor();

            while (cursor.hasNext()) {
                Document foundDocuments = (Document) cursor.next();
                resultList.add(foundDocuments);
            }

        } catch (Exception e) {
        }
        return resultList;
    }

    /**
     * Find the {@link MetadataItem} from the mongo collection based on the {@link Bson} query as the {@code user}
     * with the default offset, limit, and sort settings
     *
     * @param user  making the query
     * @param query {@link Bson} query to search with
     * @return metadataItem
     */
    public List<MetadataItem> find(String user, Bson query) {
        return find(user, query, 0, org.iplantc.service.common.Settings.DEFAULT_PAGE_SIZE, new Document());
    }

    /**
     * Find all {@link MetadataItem} from the mongo collection based on the {@link Bson} query as the {@code user}
     *
     * @param user   making the query
     * @param query  {@link Bson} query to search with
     * @param offset int offset
     * @return metadataItem
     */
    public List<MetadataItem> find(String user, Bson query, int offset, int limit, Document order) {
        MongoCursor cursor;
        List<MetadataItem> resultList = new ArrayList<>();

        //POGO
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        if (query == null) {
            query = new Document();
        }

        if (accessibleOwners == null) {
            accessibleOwners = new ArrayList<>();
        }

        Bson withPermissionQuery = and(query, getHasReadQuery(user, accessibleOwners));

        try {
            cursor = metadataItemMongoCollection.find(withPermissionQuery)
                    .sort(order)
                    .skip(offset)
                    .limit(limit).cursor();

            while (cursor.hasNext()) {
                MetadataItem metadataItem = (MetadataItem) cursor.next();
                resultList.add(metadataItem);
            }

        } catch (Exception e) {
        }
        return resultList;
    }

    /**
     * Find the {@link MetadataItem} with the provided {@link Bson} filter
     *
     * @param query {@link Bson} filter to search the collection with
     * @return {@link MetadataItem} matching the {@link Bson} filter, null if nothing found
     */
    public MetadataItem findSingleMetadataItem(Bson query) {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        try {
            return metadataItemMongoCollection.find(query).first();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Update the permission for the specified user to the specified permission
     *
     * @param metadataItem to be updated
     * @return all the permissions for the metadata item including the updated one.
     */
    public List<MetadataPermission> updatePermission(MetadataItem metadataItem) throws MetadataStoreException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        UpdateResult update;

        try {
            //get collection
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Bson uuidAndTenantQuery = getUuidAndTenantIdQuery(metadataItem.getUuid(), metadataItem.getTenantId());
            update = metadataItemMongoCollection.updateOne(uuidAndTenantQuery, set("permissions", metadataItem.getPermissions()));
            if (update.getMatchedCount() == 1) {
                //update success. fetch the doc
                MetadataItem updatedItem = findSingleMetadataItem(uuidAndTenantQuery);
                if (updatedItem != null) {
                    return updatedItem.getPermissions();
                } else {
                    throw new MetadataStoreException("Metadata item " + metadataItem.getUuid() + " is no longer present.");
                }
            } else {
                throw new MetadataStoreException("Failed to update permissions of metadata item " + metadataItem.getUuid());
            }
        } catch (MetadataStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to update permission", e);
        }
    }

    /**
     * Updates a single document representing a {@link MetadataItem}.
     *
     * @param doc the metadata item fields to update, marshalled to a {@link Document}.
     * @return freshly updated Document representing the metadata item
     * @throws MetadataException if unable to find Metadata item
     */
    public Document updateDocument(Document doc) throws MetadataException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        try {
            metadataItemMongoCollection = getDefaultCollection();

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'-05:00");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            doc.append("lastUpdated", formatter.format(new Date()));

            //push instead of replace
            UpdateResult update = metadataItemMongoCollection.updateOne(and(eq("uuid", doc.get("uuid")),
                    eq("tenantId", doc.get("tenantId"))), new Document("$set", doc));

            if (update.getModifiedCount() == 0) {
                throw new MetadataException("No document found with uuid " + doc.get("uuid") + ".");
            } else {
                Document uuidLookupDoc = new Document().append("uuid", doc.get("uuid")).append("tenantId", doc.get("tenantId"));
                List<Document> foundDocuments = filterFind(uuidLookupDoc, new Document());
                if (foundDocuments.size() > 0)
                    return foundDocuments.get(0);

                throw new MetadataException("No metadata item found for user with id " + doc.get("uuid"));
            }
        } catch (MongoException e) {
            throw new MetadataException("Failed to add/update metadata item.", e);
        }
    }

//    /**
//     * Update the {@link MetadataItem} as the provided user
//     *
//     * @param metadataItem the {@link MetadataItem} to be updated
//     * @param user         the user performing the update
//     * @return the inserted {@link MetadataItem}
//     * @throws MetadataException when update failed
//     * @deprecated
//     */
//    public MetadataItem updateMetadata(MetadataItem metadataItem, String user) throws
//            MetadataException {
//        MongoCollection<MetadataItem> metadataItemMongoCollection;
//
//        try {
//            metadataItemMongoCollection = getDefaultMetadataItemCollection();
//
//            // TODO: This should be handled before we get here. DAO is just for interacting with db
////            if (hasWrite(user, metadataItem.getUuid())) {
//            //push instead of replace
//            UpdateResult update = metadataItemMongoCollection.updateOne(
//                    getUuidAndTenantIdQuery(metadataItem.getUuid(), metadataItem.getTenantId()),
//                    new Document("$set", metadataItem));
//
//            if (update.getModifiedCount() == 0) {
//                throw new MetadataException("No document found with uuid " + metadataItem.getUuid() + ".");
//            } else {
//                Document uuidLookupDoc = new Document().append("uuid", metadataItem.getUuid()).append("tenantId", metadataItem.getTenantId());
//                MetadataItem foundDocument = findSingleMetadataItem(uuidLookupDoc);
//                if (foundDocument != null)
//                    return foundDocument;
//
//                throw new MetadataException("No metadata item found for user with id " + metadataItem.getUuid());
//            }
////            } else {
////                throw new PermissionException("User does not have sufficient access to edit public metadata item.");
////            }
//        } catch (MongoException e) {
//            throw new MetadataException("Failed to add/update metadata item.", e);
//        }
//    }


    /**
     * Delete {@code metadataItem} from the collection as the specified user
     *
     * @param metadataItem {@link MetadataItem} to delete
     * @return the deleted {@link MetadataItem}
     */
    public MetadataItem deleteMetadata(MetadataItem metadataItem) throws MetadataException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        if (metadataItem == null)
            throw new MetadataException("Unable to delete null MetadataItem.");


        DeleteResult deleteResult = metadataItemMongoCollection.deleteOne(
                getUuidAndTenantIdQuery(metadataItem.getUuid(), metadataItem.getTenantId()));
        if (deleteResult.getDeletedCount() == 0) {
            //delete unsuccessful
            return null;
        }

        return metadataItem;
    }


    /**
     * Check if {@code user} has read permissions for the {@code uuid}
     *
     * @param user to lookup permissions for
     * @param uuid the uuid of {@link MetadataItem} to lookup
     * @return true if user has read permission, false otherwise
     */
    public boolean hasRead(String user, String uuid) {
        if (AuthorizationHelper.isTenantAdmin(user))
            return true;

        if (this.accessibleOwners == null)
            setAccessibleOwners(
                    List.of(user, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME));

        return checkPermission(uuid, this.getHasReadQuery(user, this.accessibleOwners));
    }

    /**
     * Check if {@code user} has writer permissions for the {@code uuid}
     *
     * @param user to lookup permissions for
     * @param uuid the uuid of {@link MetadataItem}  to lookup
     * @return true if user has write permissions, false otherwise
     */
    public boolean hasWrite(String user, String uuid) {
        if (AuthorizationHelper.isTenantAdmin(user))
            return true;

        if (this.accessibleOwners == null)
            setAccessibleOwners(
                    List.of(user, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME));

        return checkPermission(uuid, getHasWriteQuery(user, this.accessibleOwners));
    }


    /**
     * Find MetadataPermission for user for Metadata item with matching uuid and tenantId
     *
     * @param uuid     the uuid of {@link MetadataItem}  to lookup
     * @param username user to lookup permission of
     * @param tenantId the tenant to lookup
     * @return {@link MetadataPermission} of {@code username}, null if no permissions found
     * @throws MetadataException if unable to retrieve MetadataPermission
     */
    public MetadataPermission getPermission(String uuid, String username, String tenantId) throws MetadataException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        MetadataItem result;
        try {
            result = metadataItemMongoCollection.find(getUuidAndTenantIdQuery(uuid, tenantId)).first();
            return result.getPermissionForUsername(username);
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Check the mongodb if the {@code user} has the correct permissions
     * specified in {@code userFilter} for the metadata item for the
     * specified {@code uuid}
     *
     * @param uuid       the uuid of {@link MetadataItem}  to lookup
     * @param userFilter {@link Bson} query to check if user has the correct permissions
     * @return true if user has permissions specified by userFilter
     */
    public boolean checkPermission(String uuid, Bson userFilter) {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        MetadataItem result;
        try {
            //check if user has permission
            result = metadataItemMongoCollection.find(and(eq("uuid", uuid),
                    userFilter)).first();

            if (result == null) {
                //check if uuid exists
                result = metadataItemMongoCollection.find(eq("uuid", uuid)).first();
                if (result == null) {
                    //metadata item doesn't exist, user can read or write
                    return true;
                }
            } else {
                return true;
            }
        } catch (NullPointerException npe) {
        }
        return false;
    }

    /**
     * Get Bson query that checks if user is an accessible owner or if user's permission is
     * {@link PermissionType#ALL}, {@link PermissionType#READ}, {@link PermissionType#READ_WRITE},
     * {@link PermissionType#READ_EXECUTE}, {@link PermissionType#READ_PERMISSION}, or
     * {@link PermissionType#READ_WRITE_PERMISSION},
     *
     * @param user             to find permission for
     * @param accessibleOwners list of valid owners for the resource
     * @return {@link Bson} query
     */
    public Bson getHasReadQuery(String user, List<String> accessibleOwners) {
        return or(in("owner", accessibleOwners),
                elemMatch("permissions", and(
                        eq("username", user),
                        in("permission", PermissionType.ALL.toString(), PermissionType.READ.toString(),
                                PermissionType.READ_WRITE.toString(), PermissionType.READ_EXECUTE.toString(),
                                PermissionType.READ_PERMISSION.toString(), PermissionType.READ_WRITE_PERMISSION.toString()))));
    }

    /**
     * Get Bson query that checks if user is an accessible owner or if user's permission is
     * {@link PermissionType#ALL}, {@link PermissionType#READ_WRITE}, {@link PermissionType#WRITE},
     * {@link PermissionType#WRITE_EXECUTE}, {@link PermissionType#WRITE_PERMISSION}, or
     * {@link PermissionType#READ_WRITE_PERMISSION}
     *
     * @param user             to find permission for
     * @param accessibleOwners list of valid owners for the resource
     * @return {@link Bson} query
     */
    public Bson getHasWriteQuery(String user, List<String> accessibleOwners) {
        return or(in("owner", accessibleOwners),
                elemMatch("permissions", and(
                        eq("username", user),
                        in("permission", PermissionType.READ_WRITE.toString(), PermissionType.ALL.toString(),
                                PermissionType.WRITE.toString(), PermissionType.WRITE_EXECUTE.toString(),
                                PermissionType.WRITE_PERMISSION.toString(), PermissionType.READ_WRITE_PERMISSION.toString()))));
    }

    /**
     * @return List of users who are tenant admins or the owner for the {@link MetadataItem}
     */
    public List<String> getAccessibleOwners(String user, boolean bolImplicitPermissions) {
        List<String> accessibleOwners = new ArrayList<>();
        if (!bolImplicitPermissions) {
            accessibleOwners = Arrays.asList(user,
                    org.iplantc.service.metadata.Settings.PUBLIC_USER_USERNAME,
                    org.iplantc.service.metadata.Settings.WORLD_USER_USERNAME);
        } else {
            accessibleOwners.add(user);
        }
        return accessibleOwners;
    }

    /**
     * @param tenantId the tenant to lookup
     * @return {@link Bson} query that checks if tenantId is matching the given id
     */
    public Bson getTenantIdQuery(String tenantId) {
        return eq("tenantId", tenantId);
    }

    /**
     * @param uuid     the uuid of {@link MetadataItem} to lookup
     * @param tenantId the tenant to lookup
     * @return {@link Bson} query that checks if tenantId and uuid is matching the given ids
     */
    public Bson getUuidAndTenantIdQuery(String uuid, String tenantId) {
        return and(eq("uuid", uuid), eq("tenantId", tenantId));
    }

//    public MetadataItem persist(MetadataItem item) throws MetadataStoreException {
//        ObjectMapper mapper = new ObjectMapper();
//        MongoCollection collection;
//        try {
//            collection = getDefaultCollection();
//            collection.insertOne(BasicDBObject.parse(mapper.writeValueAsString(item)));
////            BsonDocument query = new BsonDocument()
////                    .append("uuid", new BsonString(item.getUuid()))
////                    .append("tenant_id", new BsonString(TenancyHelper.getCurrentTenantId()));
////            Object obj = collection.find(query).limit(1).first();
//            return item;
//        } catch (UnknownHostException|JsonProcessingException e) {
//            throw new MetadataStoreException("Failed to insert metadata item", e);
//        }
//    }

//    public JacksonDBCollection<MetadataItem, String> getDefaultCollection() throws UnknownHostException {
//        return getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
//    }
//    
//    public JacksonDBCollection<MetadataItem, String> getCollection(String dbName, String collectionName) throws UnknownHostException {
//        
//        db = getClient().getDB(dbName);
//        // Gets a collection, if it does not exist creates it
//        return JacksonDBCollection.wrap(db.getCollection(collectionName), MetadataItem.class,
//                String.class);
//    }
//    
//    @SuppressWarnings("deprecation")
//    private MongoClient getClient() throws UnknownHostException {
//        if (mongoClient == null) {
//            
//            mongoClient = new MongoClient(new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT), 
//                    Arrays.asList(MongoCredential.createMongoCRCredential(
//                            Settings.METADATA_DB_USER, "api", Settings.METADATA_DB_PWD.toCharArray())));
//        } 
//        else if (!mongoClient.getConnector().isOpen()) 
//        {
//            try { mongoClient.close(); } catch (Exception e) { log.error("Failed to close mongo client.", e); }
//            mongoClient = null;
//            mongoClient = new MongoClient(new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT), 
//                    Arrays.asList(MongoCredential.createMongoCRCredential(
//                            Settings.METADATA_DB_USER, "api", Settings.METADATA_DB_PWD.toCharArray())));
//        }
//            
//        return mongoClient;
//    }
//    
//    /**
//     * Generates a {@link Query} from the given {@code uuid} and {@link TenancyHelper#getCurrentTenantId()}
//     * @param uuid
//     * @return
//     */
//    private Query _getDBQueryForUuidAndTenant(String uuid) {
//        return _getDBQueryForUuidAndTenant(uuid, TenancyHelper.getCurrentTenantId());
//    }
//    
//    /**
//     * Generates a {@link Query} from the given {@code uuid} and {code tenantId}
//     * @param uuid
//     * @param tenantId
//     * @return
//     */
//    private Query _getDBQueryForUuidAndTenant(String uuid, String tenantId) {
//        return DBQuery.and(
//                DBQuery.is("uuid", uuid), 
//                DBQuery.is("tenantId", tenantId));
//    }
//
//    /**
//     * Returns a {@link MetadataItem} with the matching {@code uuid} and {code tenantId}
//     * @param uuid
//     * @param tenantId
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataItem findByUuidAndTenant(String uuid, String tenantId) 
//    throws MetadataQueryException 
//    {   
//        try {
//            return (MetadataItem)getDefaultCollection().findOne(_getDBQueryForUuidAndTenant(uuid, tenantId));
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to find metadata by UUID", e);
//        }        
//    }
//    
//    
//    /**
//     * Implements search for metadata items.
//     * 
//     * @param username
//     * @param tenantId
//     * @param userSearchTermMap
//     * @param offset
//     * @param limit
//     * @return
//     * @throws MetadataQueryException
//     */
//    public DBCursor<MetadataItem> findMatching(String username, String tenantId, Map<SearchTerm, Object> userSearchTermMap, int offset, int limit)
//    throws MetadataQueryException 
//    {   
//        try {
//            
//            DBObject userSearchCriteria = parseUserSearchCriteria(userSearchTermMap);
//            
//            DBObject tenantCriteria = QueryBuilder.start("tenantId").is(tenantId).get();
//            
//            DBCursor<MetadataItem> cursor = null; 
//                    
//            // skip permission queries if user is admin
//            if (AuthorizationHelper.isTenantAdmin(username)) {
//                
//                cursor = getDefaultCollection().find(QueryBuilder.start().and(
//                                    userSearchCriteria,
//                                    tenantCriteria).get())
//                                .skip(offset)
//                                .limit(limit);
//            } 
//            // non admins must check permissions for ownership or read grants
//            else {
//                DBObject authCriteria = createAuthCriteria(username, READ);
//                
//                cursor = getDefaultCollection().find(QueryBuilder.start().and(
//                                    authCriteria,
//                                    userSearchCriteria,
//                                    tenantCriteria).get())
//                                .skip(offset)
//                                .limit(limit);
//            }                
//        
//            return cursor;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Failed to fetch metadata from db", e);
//        }        
//    }
//    
//    public DBCursor<MetadataItem> legacyFindMatching(String username, String tenantId, Map<SearchTerm, Object> userSearchTermMap, int offset, int limit)
//    throws MetadataQueryException 
//    {
//        try {
//            DBObject userSearchCriteria = parseUserSearchCriteria(userSearchTermMap);
//            
//            DBObject tenantCriteria = QueryBuilder.start("tenantId").is(tenantId).get();
//            
//            DBCursor<MetadataItem> cursor = null; 
//                    
//            // skip permission queries if user is admin
//            if (AuthorizationHelper.isTenantAdmin(username)) {
//                
//                cursor = getDefaultCollection().find(QueryBuilder.start().and(
//                                    userSearchCriteria,
//                                    tenantCriteria).get())
//                                .skip(offset)
//                                .limit(limit);
//            } 
//            // non admins must check permissions for ownership or read grants
//            else {
//                
//                DBObject ownerCriteria = QueryBuilder.start("owner").in(
//                        Arrays.asList(Settings.PUBLIC_USER_USERNAME,
//                                      Settings.WORLD_USER_USERNAME,
//                                      username)).get();
//                
//                List<String> relationalSharedMetadataUuid = 
//                        MetadataPermissionDao.getUuidOfAllSharedMetataItemReadableByUser(username);
//                
//                DBObject sharedMetadataCriteria = QueryBuilder.start("uuid").in(
//                        relationalSharedMetadataUuid).get();
//                
//                DBObject authCriteria = QueryBuilder.start()
//                            .or(
//                                ownerCriteria, 
//                                sharedMetadataCriteria
//                                )
//                        .get();
//                
//                cursor = getDefaultCollection().find(QueryBuilder.start().and(
//                        authCriteria, 
//                        userSearchCriteria,
//                        tenantCriteria).get())
//                    .skip(offset)
//                    .limit(limit);
//            }
//            
//            return cursor;
//        } 
//        catch (Exception e) {
//            throw new MetadataQueryException("Failed to fetch metadata from db", e);
//        } 
//    }
//    
//    /**
//     * Delete one or more metadata value fields atomically 
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @throws MetadataQueryException
//     */
//    public void delete(String uuid, String tenantId, List<String> uuids) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (uuids == null || uuids.isEmpty()) {
//                return;
//             }
//             else 
//             {
//                 WriteResult<MetadataItem,String> result = getDefaultCollection().remove(
//                         DBQuery.and(DBQuery.in("uuid", uuids), DBQuery.is("tenantId", TenancyHelper.getCurrentTenantId())));
//                 // do we need to check for errors?
//                 if (!result.getWriteResult().getLastError().isEmpty()) {
//                     throw new MetadataQueryException("Failed to delete one or more items", 
//                             result.getWriteResult().getLastError().getException());
//                 }
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    
//    /**
//     * Unsets one or more metadata value fields atomically 
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataItem> unset(String uuid, String tenantId, List<String> fields) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (fields == null || fields.isEmpty()) {
//                return new ArrayList<MetadataItem>();
//             }
//             else 
//             {
//                 Builder builder = null;
//                 for (String key: fields) {
//                     if (builder == null) {
//                         builder = DBUpdate.unset(key);
//                     } else {
//                         builder = builder.unset(key);
//                     }
//                 }
//                 
//                 WriteResult<MetadataItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
//                 // do we need to check for errors?
//                 if (!result.getWriteResult().getLastError().isEmpty()) {
//                     throw new MetadataQueryException("Failed to unset one or more item fields", 
//                             result.getWriteResult().getLastError().getException());
//                 }
//                 return result.getSavedObjects();
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//
//    /**
//     * Insert a new metadata item
//     * @param item
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataItem insert(MetadataItem item) throws MetadataQueryException {
//        try {
//            WriteResult<MetadataItem, String> result = getDefaultCollection().insert(item);
//            return result.getSavedObject();
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Insert a new metadata item if another {@link MetadataItem} with the same {@code name}, 
//     * {@code tenantId}, {@code owner}, {@code value}, {@code associatedIds}, and {@code internalUser} 
//     * does not already exist.
//     *  
//     * @param item
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataItem insertIfNotPresent(MetadataItem item) 
//    throws MetadataQueryException 
//    {
//        try {
//            MetadataItem existingItem = getDefaultCollection().findOne(DBQuery.and(
//                                                                    DBQuery.is("name", item.getUuid()), 
//                                                                    DBQuery.is("tenantId", item.getTenantId()),
//                                                                    DBQuery.is("owner",  item.getOwner()),
//                                                                    DBQuery.is("value",  item.getValue()),
//                                                                    DBQuery.all("associatedIds", item.getAssociations().getRawUuid()),
//                                                                    DBQuery.is("internalUser",  item.getInternalUsername())));
//            
//            if (existingItem == null) {
//                return getDefaultCollection().insert(item).getSavedObject();
//            } else {
//                return existingItem;
//            }
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Update one or more metadata names or values in part or whole atomically 
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataItem> update(String uuid, String tenantId, Map<String, JsonNode> updates) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (updates == null || updates.isEmpty()) {
//                return new ArrayList<MetadataItem>();
//             }
//             else 
//             {
//                 Builder builder = null;
//                 for (String key: updates.keySet()) {
//                     if (builder == null) {
//                         builder = DBUpdate.set(key, updates.get(key));
//                     } else {
//                         builder = builder.set(key, updates.get(key));
//                     }
//                 }
//                 
//                 WriteResult<MetadataItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
//                 // do we need to check for errors?
//                 if (!result.getWriteResult().getLastError().isEmpty()) {
//                     throw new MetadataQueryException("Failed to update one or more items", 
//                             result.getWriteResult().getLastError().getException());
//                 }
//                 return result.getSavedObjects();
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Add the given value to the array value if it doesn't already exist in the specified field atomically
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataItem> add(String uuid, String tenantId, Map<String, JsonNode> updates) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (updates == null || updates.isEmpty()) {
//                return new ArrayList<MetadataItem>();
//             }
//             else 
//             {
//                 Builder builder = null;
//                 for (String key: updates.keySet()) {
//                     if (builder == null) {
//                         builder = DBUpdate.addToSet(key, updates.get(key));
//                     } else {
//                         builder = builder.addToSet(key, updates.get(key));
//                     }
//                 }
//                 
//                 WriteResult<MetadataItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
//                 // do we need to check for errors?
//                 if (!result.getWriteResult().getLastError().isEmpty()) {
//                     throw new MetadataQueryException("Failed to add to one or more items", 
//                             result.getWriteResult().getLastError().getException());
//                 }
//                 return result.getSavedObjects();
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Add one or ore values to the array value at each of the specified fields atomically
//     * @param uuid
//     * @param tenantId
//     * @param additions
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataItem> append(String uuid, String tenantId, Map<String, JsonNode> additions) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (additions == null || additions.isEmpty()) {
//                return new ArrayList<MetadataItem>();
//             }
//             else 
//             {
//                 Builder builder = null;
//                 for (String key: additions.keySet()) {
//                     if (builder == null) {
//                         if (additions.get(key) instanceof List) {
//                             builder = DBUpdate.pushAll(key, additions.get(key));
//                         } else {
//                             builder = DBUpdate.push(key, additions.get(key));
//                         }
//                     } else {
//                         if (additions.get(key) instanceof List) {
//                             builder = builder.pushAll(key, additions.get(key));
//                         } else {
//                             builder = builder.push(key, additions.get(key));
//                         }
//                     }
//                 }
//                 WriteResult<MetadataItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
//                 // do we need to check for errors?
//                 if (!result.getWriteResult().getLastError().isEmpty()) {
//                     throw new MetadataQueryException("Failed to append to one or more items", 
//                             result.getWriteResult().getLastError().getException());
//                 }
//                 return result.getSavedObjects();
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Perform an atomic increment action of a user-defined amount on the given metadata values(s).
//     * @param uuid
//     * @param tenantId
//     * @param increments
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataItem> increment(String uuid, String tenantId, Map<String, Integer> increments) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (increments == null || increments.isEmpty()) {
//               return new ArrayList<MetadataItem>();
//            }
//            else 
//            {
//                Builder builder = null;
//                for (String key: increments.keySet()) {
//                    if (builder == null) {
//                        builder = DBUpdate.inc(key, increments.get(key).intValue());
//                    } else {
//                        builder = builder.inc(key, increments.get(key).intValue());
//                    }
//                }
//                WriteResult<MetadataItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
//                // do we need to check for errors?
//                if (!result.getWriteResult().getLastError().isEmpty()) {
//                    throw new MetadataQueryException("Failed to increment one or more items", 
//                            result.getWriteResult().getLastError().getException());
//                }
//                return result.getSavedObjects();
//            }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }       
//    }

//    /**
//     * Creates a {@link DBObject} representing the appropriate permission check
//     * for {@code username} to establish they have @{link permission) for a
//     * {@link MetadataItem}.
//     *
//     * @param username
//     * @param permission
//     * @return
//     */
//    protected DBObject createAuthCriteria(String username, PermissionType permission) {
//        BasicDBList ownerList = new BasicDBList();
//        ownerList.addAll(Arrays.asList(Settings.PUBLIC_USER_USERNAME, Settings.WORLD_USER_USERNAME, username));
//
//        BasicDBList aclCriteria = new BasicDBList();
//        aclCriteria.add(QueryBuilder.start("username").in(ownerList).get());
//        if (permission == ALL) {
//            aclCriteria.add(QueryBuilder.start("read").is(true).and("write").is(true).get());
//        } else if (permission.canRead() && permission.canWrite()) {
//            aclCriteria.add(QueryBuilder.start("read").is(true).and("write").is(true).get());
//        } else if (permission.canRead()) {
//            aclCriteria.add(QueryBuilder.start("read").is(true).get());
//        } else if (permission.canWrite()) {
//            aclCriteria.add(QueryBuilder.start("write").is(true).get());
//        }
//
//        BasicDBList authConditions = new BasicDBList();
//        authConditions.add(QueryBuilder.start("owner").in(ownerList).get());
//        authConditions.add(QueryBuilder.start("acl").all(aclCriteria).get());
//
//        DBObject authCriteria = QueryBuilder.start().all(
//                authConditions).get();
//
//        return authCriteria;
//    }

//    /**
//     * Turns the search criteria supplied by the user in the URL query into a
//     * {@link DBObject} we can pass to the MongoDB driver.
//     *
//     * @param searchCriteria
//     * @return
//     * @throws MetadataQueryException
//     */
//    @SuppressWarnings("unchecked")
//    protected DBObject parseUserSearchCriteria(Map<SearchTerm, Object> searchCriteria) throws
//            MetadataQueryException {
//        DBObject userCriteria = null;
//        QueryBuilder queryBuilder = null;
//
//        if (searchCriteria == null || searchCriteria.isEmpty()) {
//            return new BasicDBObject();
//        } else {
//            for (SearchTerm searchTerm : searchCriteria.keySet()) {
//
//                // this is a freeform search query. Support regex then move on. if this exists, it is the only
//                // search criteria
//                if (searchCriteria.get(searchTerm) instanceof DBObject) {
//
//                    userCriteria = (DBObject) searchCriteria.get(searchTerm);
//
//                    // support regex in the freeform queries
//                    for (String key : userCriteria.keySet()) {
//
//                        // TODO: throw exception on unsafe mongo keywords in freeform search
//
//                        // we're just going one layer deep on the regex support. anything else won't work anyway due to
//                        // the lack of freeform query support in the java driver
//                        if (userCriteria.get(key) instanceof String) {
//                            if (((String) userCriteria.get(key)).contains("*")) {
//                                try {
//                                    Pattern regexPattern = Pattern.compile((String) userCriteria.get(key), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
//                                    userCriteria.put(key, regexPattern);
//                                } catch (Exception e) {
//                                    throw new MetadataQueryException("Invalid regular expression for " + key + " query", e);
//                                }
//                            }
//                        }
//                    }
//                }
//                // they are using the json.sql notation to search their metadata value
//                else { // if (searchTerm.getSearchField().equalsIgnoreCase("value")) {
//                    if (queryBuilder == null) {
//                        queryBuilder = QueryBuilder.start(searchTerm.getMappedField());
//                    } else {
//                        queryBuilder.and(searchTerm.getMappedField());
//                    }
//
//                    if (searchTerm.getOperator() == SearchTerm.Operator.EQ) {
//                        queryBuilder.is(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.NEQ) {
//                        queryBuilder.notEquals(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.IN) {
//                        queryBuilder.in(Arrays.asList(searchCriteria.get(searchTerm)));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.NIN) {
//                        queryBuilder.notIn(Arrays.asList(searchCriteria.get(searchTerm)));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.GT) {
//                        queryBuilder.greaterThan(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.GTE) {
//                        queryBuilder.greaterThanEquals(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.LT) {
//                        queryBuilder.lessThan(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.LTE) {
//                        queryBuilder.lessThanEquals(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.LIKE) {
//                        try {
//                            Pattern regexPattern = Pattern.compile((String) searchCriteria.get(searchTerm), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
//                            queryBuilder.regex(regexPattern);
//                        } catch (Exception e) {
//                            throw new MetadataQueryException("Invalid regular expression for " + searchTerm.getMappedField() + " query", e);
//                        }
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.NLIKE) {
//                        try {
//                            Pattern regexPattern = Pattern.compile((String) searchCriteria.get(searchTerm), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
//                            queryBuilder.not().regex(regexPattern);
//                        } catch (Exception e) {
//                            throw new MetadataQueryException("Invalid regular expression for " + searchTerm.getMappedField() + " query", e);
//                        }
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.ON) {
//                        queryBuilder.is(searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.BEFORE) {
//
//                        queryBuilder.lessThan((Date) searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.AFTER) {
//                        queryBuilder.greaterThan((Date) searchCriteria.get(searchTerm));
//                    } else if (searchTerm.getOperator() == SearchTerm.Operator.BETWEEN) {
//                        List<Date> dateRange = (List<Date>) searchCriteria.get(searchTerm);
//                        queryBuilder.greaterThan(dateRange.get(0))
//                                .and(searchTerm.getMappedField())
//                                .lessThan(dateRange.get(1));
//                    }
//                }
//            }
//
//            // generate the query if we used the query builder
//            if (queryBuilder != null) {
//                userCriteria = queryBuilder.get();
//            }
//            // if there wasn't a freeform search query, we need to init
//            else if (userCriteria == null) {
//                userCriteria = new BasicDBObject();
//            }
//
//            return userCriteria;
//        }
//    }
}

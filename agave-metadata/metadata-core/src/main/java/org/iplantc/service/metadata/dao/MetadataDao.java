package org.iplantc.service.metadata.dao;

import static org.bson.codecs.configuration.CodecRegistries.*;
import static org.iplantc.service.metadata.model.enumerations.PermissionType.ALL;

import java.net.UnknownHostException;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.mongodb.*;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoClients;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.common.Settings;
//import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class MetadataDao {
    
    private static final Logger log = Logger.getLogger(MetadataDao.class);
    
    private MongoDatabase db = null;
    private MongoClient mongoClient = null;
    private MongoClients mongoClients = null;

    private static MetadataDao dao = null;

    private boolean bolRead;
    private boolean bolWrite;

    public static MetadataDao getInstance() {
        if (dao == null) {
            dao = new MetadataDao();
        }
        
        return dao;
    }

    /**
     * Establishes a connection to the mongo server
     *
     * @return valid mongo client connection
     */
    public MongoClient getMongoClient()
    {
        if (mongoClient == null )
        {
            mongoClient = new MongoClient(
                    new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT),
                    getMongoCredential(),
                    MongoClientOptions.builder().build());
        }

        return mongoClient;
    }

    public com.mongodb.client.MongoClient getMongoClients() {
        if (mongoClients == null) {

            //testing for custom codec
            ClassModel<JsonNode> valueModel = ClassModel.builder(JsonNode.class).build();
            ClassModel<MetadataPermission> metadataPermissionModel = ClassModel.builder(MetadataPermission.class).build();
            PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(valueModel, metadataPermissionModel).build();

            CodecRegistry registry = CodecRegistries.fromCodecs(new MetadataItemCodec());

            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(pojoCodecProvider),
                    registry);

            return mongoClients.create(MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(
                            new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT))))
                    .credential(getMongoCredential())
                    .codecRegistry(pojoCodecRegistry)
                    .build());
        }
        return null;
    }

    /**
     * Creates a new MongoDB credential for the database collections
     * @return valid mongo credential for this instance
     */
    private MongoCredential getMongoCredential() {
        return MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());
    }


    /**
     * Gets the default metadata collection from the default mongodb metatadata db.
     * @return collection from the db
     * @throws UnknownHostException if the connection cannot be found/created, or db connection is bad
     */
    public MongoCollection getDefaultCollection() throws UnknownHostException {
        return getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
    }

    /**
     * Gets the named collection from the named db.
     * @param dbName database name
     * @param collectionName collection name
     * @return collection from the db
     * @throws UnknownHostException if the connection cannot be found/created, or db connection is bad
     */
    public MongoCollection getCollection(String dbName, String collectionName) throws UnknownHostException {

        db = getMongoClient().getDatabase(dbName);
        db = getMongoClients().getDatabase(dbName); //update to 4.0

        MongoCollection<MetadataItem> newDb = db.getCollection("api", MetadataItem.class);

        // Gets a collection, if it does not exist creates it
        return db.getCollection(collectionName);

    }

    public MongoCollection<MetadataItem> getDefaultMetadataItemCollection() throws UnknownHostException {
        return getMetadataItemCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
    }

    /**
     * Get the named collection configured with the MetadataItem class from the named db
     * @param dbName database name
     * @param collectionName colletion name
     * @return collection from the db
     */
    public MongoCollection<MetadataItem> getMetadataItemCollection (String dbName, String collectionName){
        db = getMongoClients().getDatabase(dbName);

        return db.getCollection(collectionName, MetadataItem.class);
    }

    /**
     * Stores the provided {@link MetadataItem} in the mongo collection
     * @param metadataItem the {@link MetadataItem} to be inserted
     * @return the inserted {@link MetadataItem}
     * @throws MetadataStoreException when the insertion failed
     */
    public MetadataItem insert(MetadataItem metadataItem) throws MetadataStoreException {
        MongoCollection<MetadataItem> metadataItemCollection;
        try {
            //using POJO Codec
            metadataItemCollection = getDefaultMetadataItemCollection();
            metadataItemCollection.insertOne(metadataItem);

            return metadataItem;
        } catch (UnknownHostException e) {
            throw new MetadataStoreException("Failed to insert metadata item", e);
        }
    }

    public MetadataItem insertMetadataItem(MetadataItem metadataItem) throws MetadataStoreException {
        ObjectMapper mapper = new ObjectMapper();

        MongoCollection<MetadataItem> metadataItemCollection;
        try {
            metadataItemCollection = getDefaultMetadataItemCollection();
            metadataItemCollection.insertOne(metadataItem);

            return metadataItem;
        } catch (UnknownHostException e) {
            throw new MetadataStoreException("Failed to insert metadata item", e);
        }
    }


    /**
     * Return the metadataItem from the collection
     * @return metadataItem
     */
    public List<MetadataItem> find (String user, Bson query) throws MetadataStoreException {
        return find(user, query, 0, org.iplantc.service.common.Settings.DEFAULT_PAGE_SIZE, new BasicDBObject());
    }

    public List<MetadataItem> find(String user, Bson query, int offset, int limit, BasicDBObject order) throws MetadataStoreException {
        MongoCursor cursor;
        List<MetadataItem> resultList = new ArrayList<>();

        //POGO
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        try{
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

//            Document docPermissions = new Document("permissions", new Document("$elemMatch", new Document("username", user)));
//
//            Bson docFilter = elemMatch("permissions", and (eq("username", user),
//                    nin("permission", Arrays.asList(PermissionType.NONE.toString()))));
//
//            Bson permissionFilter = or (eq("owner", user), docFilter);

            if (query == null) {
                query = new Document();
            }

            try {
//                cursor = metadataItemMongoCollection.find(and(query, or(eq("owner", user), docFilter)))
                cursor = metadataItemMongoCollection.find(query)
                        .sort(order)
                        .skip(offset)
                        .limit(limit).cursor();

//                cursor = metadataItemMongoCollection.find(query).cursor();

                while (cursor.hasNext()) {
                    resultList.add((MetadataItem) cursor.next());
                }

            } catch (Exception e) {
            }

            return resultList;

        } catch (UnknownHostException e) {
            throw new MetadataStoreException("Failed to find metadata item", e);
        }
    }

    public MetadataItem find_uuid(Bson filter) throws MetadataStoreException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        try{
            metadataItemMongoCollection = getDefaultMetadataItemCollection();
            return metadataItemMongoCollection.find(filter).first();

        } catch (UnknownHostException e) {
            throw new MetadataStoreException("Failed to find metadata item", e);
        }
    }

    public List<MetadataItem> aggFind(String user, Bson query) {
        List<MetadataItem> resultList = new ArrayList<>();
        MongoCollection collection;
        MetadataItem returnEntity;
        MongoCursor cursor;

        //POGO
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        try {
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Document docPermissions = new Document("permissions", new Document("$elemMatch", new Document("username", user)));

            Bson docFilter = or(eq("username", user), elemMatch("permissions", eq("username", user)));

            List<Bson> aggregateList = new ArrayList<>();
            aggregateList.add(docFilter);
            aggregateList.add(query);

            cursor = metadataItemMongoCollection.aggregate(aggregateList).cursor();
            while (cursor.hasNext()) {
                resultList.add((MetadataItem) cursor.next());
            }


        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return resultList;
    }

        /**
     * Removes the permission for the specified user
     * @param metadataItem to be updated
     * @param user to be removed
     */
    public MetadataItem deleteUserPermission(MetadataItem metadataItem, String user) throws MetadataStoreException{
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        String uuid = metadataItem.getUuid();
        try{
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Document docQuery = new Document("uuid", uuid)
                    .append("permissions", new Document("$elemMatch", new Document("username", user)));

            MetadataPermission pemDelete = metadataItem.getPermissions_User(user);
            metadataItem.updatePermissions_delete(pemDelete);
            List<MetadataPermission> metadataPermissionsList = metadataItem.getPermissions();

            UpdateResult update = metadataItemMongoCollection.updateOne(docQuery, set("permissions", metadataPermissionsList));

        } catch (Exception e){
            throw new MetadataStoreException("Failed to delete user's permission", e);
        }
        return metadataItem;
    }

    /**
     * Removes all permissions for the metadataItem
     * @param metadataItem to be updated
     */
    public void deleteAllPermissions(MetadataItem metadataItem) throws MetadataStoreException{
        MongoCollection collection;
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        try{
            collection = getDefaultCollection();
            metadataItemMongoCollection = getDefaultMetadataItemCollection();


            metadataItem.setPermissions(new ArrayList<>());
//            BasicDBList pemList = setPermissionsForDB(metadataItem.getPermissions());

            BasicDBObject query = new BasicDBObject("uuid", metadataItem.getUuid());
            collection.updateOne(query, new BasicDBObject("$set", new BasicDBObject("permissions", new ArrayList<MetadataPermission>())));
            metadataItemMongoCollection.updateOne(query, set("permissions", new ArrayList<MetadataItem>()));
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to delete all permissions", e);
        }
    }

    /**
     * Update the permision for the specified user to the specified permission
     * @param metadataItem to be updated
     * @param user to be updated
     * @param pem PermissionType to be updated
     */
    public MetadataPermission updatePermission(MetadataItem metadataItem, String user, PermissionType pem) throws MetadataStoreException{
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        long matchCount, modified;
        UpdateResult update;
        String uuid;
        MongoCursor cursor;
        List<MetadataPermission> pemList;

        try{
            //get collection
            metadataItemMongoCollection = getDefaultMetadataItemCollection();
            uuid = metadataItem.getUuid();

            Document docQuery = new Document("uuid", uuid);
//                    .append("permissions", new Document("$elemMatch", new Document("username", user)));

//            Bson docFilter = and(eq("uuid", uuid), elemMatch("permissions", eq("username", user)));

            MetadataPermission pemUpdate = metadataItem.getPermissions_User(user);

            if (pemUpdate == null) {
                //user permission doesn't exist, create it
                pemUpdate = new MetadataPermission(uuid, user, pem);
            } else {
                //update permission
                pemUpdate.setPermission(pem);
            }

            metadataItem.updatePermissions(pemUpdate);
            List<MetadataPermission> metadataPermissionsList = metadataItem.getPermissions();
            update = metadataItemMongoCollection.updateOne(docQuery, set("permissions", metadataPermissionsList));

            return pemUpdate;
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to update permission", e);
        }
    }

    public MetadataItem updateMetadata (MetadataItem metadataItem, String user) throws MetadataException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        try {
            metadataItemMongoCollection = getDefaultMetadataItemCollection();
            metadataItemMongoCollection.replaceOne(eq("uuid", metadataItem.getUuid()), metadataItem);

        } catch (UnknownHostException e) {
            throw new MetadataException("Failed to add/update metadata item.", e);
        }

        return metadataItem;
    }

    public MetadataItem deleteMetadata (MetadataItem metadataItem, String user) throws MetadataStoreException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        try {
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Document docPermissions = new Document("permissions", new Document("$elemMatch", new Document("username", user)));

            Bson deleteFilter = and(eq("uuid", metadataItem.getUuid()), eq("tenantId", metadataItem.getTenantId()));
            Bson queryFilter = and(deleteFilter, or(eq("owner", user), docPermissions));

            DeleteResult deleteResult = metadataItemMongoCollection.deleteOne(queryFilter);
            if (deleteResult.getDeletedCount() == 0){
                //delete unsuccessful
                return null;
            }

        } catch (UnknownHostException e) {
            throw new MetadataStoreException("No item was deleted", e);
        }
        return metadataItem;

    }

    public Bson createDocQuery(MetadataItem metadataItem){
        return and(eq("tenantId", metadataItem.getTenantId()), eq("uuid", metadataItem.getUuid()));
    }

    /**
     * Remove all documents in the collection
     */
    public void clearCollection() throws UnknownHostException {
        if (this.getCollectionSize() > 0){
            MongoCollection<MetadataItem> metadataPermissionMongoCollection;
            metadataPermissionMongoCollection = getDefaultMetadataItemCollection();
            metadataPermissionMongoCollection.deleteMany(new Document());
        }
    }

    /**
     * Return number of documents in collection
     * @return number of documents in collection
     */
    long  getCollectionSize() throws UnknownHostException {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        metadataItemMongoCollection = getDefaultMetadataItemCollection();
        return metadataItemMongoCollection.countDocuments();
    }

    /**
     * Return all documents in the collection
     * @return list of all documents in collection
     */
    public List<MetadataItem> findAll() throws UnknownHostException {
        List<MetadataItem> resultList = new ArrayList<>();
        MongoCollection<MetadataItem> metadataItemMongoCollection;
        MongoCursor cursor = null;

        metadataItemMongoCollection = getDefaultMetadataItemCollection();

        cursor = metadataItemMongoCollection.find(new Document()).cursor();

        while (cursor.hasNext()) {
            resultList.add((MetadataItem)cursor.next());
        }
        return resultList;
    }


    public boolean hasRead(String user, String uuid) {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        try {
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Bson userFilter = elemMatch("permissions", and (eq("username", user), nin("permission", Arrays.asList(PermissionType.NONE.toString()))));

            MetadataItem result;
            try {
                //check if uuid exists
                result = metadataItemMongoCollection.find(eq("uuid", uuid)).first();
                if (result != null) {
                    result = metadataItemMongoCollection.find(and(eq("uuid", uuid), or(eq("owner", user), userFilter))).first();
                    if (result != null) {
                        bolRead = true;
                    } else {
                        //check if uuid exists
                        bolRead = false;
                    }
                } else {
                    bolRead = true;
                }
            } catch (NullPointerException npe) {
                bolRead = false;
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return bolRead;
    }

    public boolean hasWrite(String user, String uuid) {
        MongoCollection<MetadataItem> metadataItemMongoCollection;

        try {
            metadataItemMongoCollection = getDefaultMetadataItemCollection();

            Bson userFilter = elemMatch("permissions", and (eq("username", user), in("permission", Arrays.asList(PermissionType.READ_WRITE.toString()))));

            MetadataItem result;
            try {
                result = metadataItemMongoCollection.find(and(eq("uuid", uuid), or(eq("owner", user), userFilter))).first();
                if (result != null) {
                    bolWrite = true;
                } else {
                    bolWrite = false;
                }
            } catch (NullPointerException npe) {
                bolWrite =  false;
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return bolWrite;
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
    
    /**
     * Creates a {@link DBObject} representing the appropriate permission check 
     * for {@code username} to establish they have @{link permission) for a 
     * {@link MetadataItem}.
     * 
     * @param username
     * @param permission
     * @return
     */
    protected DBObject createAuthCriteria(String username, PermissionType permission) {
        BasicDBList ownerList = new BasicDBList();
        ownerList.addAll(Arrays.asList(Settings.PUBLIC_USER_USERNAME, Settings.WORLD_USER_USERNAME, username));
        
        BasicDBList aclCriteria = new BasicDBList();
        aclCriteria.add(QueryBuilder.start("username").in(ownerList).get());
        if (permission == ALL) {
            aclCriteria.add(QueryBuilder.start("read").is(true).and("write").is(true).get());
        } 
        else if (permission.canRead() && permission.canWrite()) {
            aclCriteria.add(QueryBuilder.start("read").is(true).and("write").is(true).get());
        } 
        else if (permission.canRead()) {
            aclCriteria.add(QueryBuilder.start("read").is(true).get());
        } 
        else if (permission.canWrite()) {
            aclCriteria.add(QueryBuilder.start("write").is(true).get());
        }
        
        BasicDBList authConditions = new BasicDBList();
        authConditions.add(QueryBuilder.start("owner").in(ownerList).get());
        authConditions.add(QueryBuilder.start("acl").all(aclCriteria).get());
        
        DBObject authCriteria = QueryBuilder.start().all(
                authConditions).get();
        
        return authCriteria;
    }
    
    /**
     * Turns the search criteria supplied by the user in the URL query into a 
     * {@link DBObject} we can pass to the MongoDB driver.
     * 
     * @param searchCriteria
     * @return
     * @throws MetadataQueryException 
     */
    @SuppressWarnings("unchecked")
    protected DBObject parseUserSearchCriteria(Map<SearchTerm, Object> searchCriteria) throws MetadataQueryException {
        DBObject userCriteria = null;
        QueryBuilder queryBuilder = null;
        
        if (searchCriteria == null || searchCriteria.isEmpty()) {
            return new BasicDBObject();
        } 
        else {
            for (SearchTerm searchTerm: searchCriteria.keySet()) {
                
                // this is a freeform search query. Support regex then move on. if this exists, it is the only
                // search criteria
                if (searchCriteria.get(searchTerm) instanceof DBObject) {
                    
                    userCriteria = (DBObject)searchCriteria.get(searchTerm);
                    
                    // support regex in the freeform queries
                    for (String key: userCriteria.keySet()) {
                        
                        // TODO: throw exception on unsafe mongo keywords in freeform search
                        
                        // we're just going one layer deep on the regex support. anything else won't work anyway due to 
                        // the lack of freeform query support in the java driver
                        if (userCriteria.get(key) instanceof String) {
                            if (((String) userCriteria.get(key)).contains("*")) {
                                try {
                                    Pattern regexPattern = Pattern.compile((String)userCriteria.get(key), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                                    userCriteria.put(key, regexPattern);
                                } catch (Exception e) {
                                    throw new MetadataQueryException("Invalid regular expression for " + key + " query", e);
                                }
                            }
                        }
                    }
                }
                // they are using the json.sql notation to search their metadata value
                else { // if (searchTerm.getSearchField().equalsIgnoreCase("value")) {
                    if (queryBuilder == null) {
                        queryBuilder = QueryBuilder.start(searchTerm.getMappedField());
                    } else {
                        queryBuilder.and(searchTerm.getMappedField());
                    }
                    
                    if (searchTerm.getOperator() == SearchTerm.Operator.EQ) {
                        queryBuilder.is(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.NEQ) {
                        queryBuilder.notEquals(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.IN) {
                        queryBuilder.in(Arrays.asList(searchCriteria.get(searchTerm)));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.NIN) {
                        queryBuilder.notIn(Arrays.asList(searchCriteria.get(searchTerm)));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.GT) {
                        queryBuilder.greaterThan(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.GTE) {
                        queryBuilder.greaterThanEquals(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.LT) {
                        queryBuilder.lessThan(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.LTE) {
                        queryBuilder.lessThanEquals(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.LIKE) {
                        try {
                            Pattern regexPattern = Pattern.compile((String)searchCriteria.get(searchTerm), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                            queryBuilder.regex(regexPattern);
                        } catch (Exception e) {
                            throw new MetadataQueryException("Invalid regular expression for " + searchTerm.getMappedField() + " query", e);
                        }
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.NLIKE) {
                        try {
                            Pattern regexPattern = Pattern.compile((String)searchCriteria.get(searchTerm), Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                            queryBuilder.not().regex(regexPattern);
                        } catch (Exception e) {
                            throw new MetadataQueryException("Invalid regular expression for " + searchTerm.getMappedField() + " query", e);
                        }
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.ON) {
                        queryBuilder.is(searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.BEFORE) {
                        
                        queryBuilder.lessThan((Date)searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.AFTER) {
                        queryBuilder.greaterThan((Date)searchCriteria.get(searchTerm));
                    }
                    else if (searchTerm.getOperator() == SearchTerm.Operator.BETWEEN) {
                        List<Date> dateRange = (List<Date>)searchCriteria.get(searchTerm);
                        queryBuilder.greaterThan(dateRange.get(0))
                                    .and(searchTerm.getMappedField())
                                        .lessThan(dateRange.get(1));
                    }
                }
            }
        
            // generate the query if we used the query builder
            if (queryBuilder != null) {
                userCriteria = queryBuilder.get();
            }
            // if there wasn't a freeform search query, we need to init
            else if (userCriteria == null) {
                userCriteria = new BasicDBObject();
            } 
            
            return userCriteria;
        }
    }
}

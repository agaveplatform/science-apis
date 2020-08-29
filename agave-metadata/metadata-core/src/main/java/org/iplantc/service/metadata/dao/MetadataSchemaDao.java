package org.iplantc.service.metadata.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataSchemaItem;

import java.net.UnknownHostException;

public class MetadataSchemaDao {
    
    private static final Logger log = Logger.getLogger(MetadataSchemaDao.class);

    private MongoClient mongoClient;
    private MongoDatabase db;

    private static MetadataSchemaDao dao = null;
    
    public static MetadataSchemaDao getInstance() {
        if (dao == null) {
            dao = new MetadataSchemaDao();
        }
        
        return dao;
    }

    /**
     * Establishes a connection to the mongo server
     *
     * @return valid mongo client connection
     * @throws UnknownHostException when the host cannot be found
     */
    public MongoClient getMongoClient() throws UnknownHostException
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
        return getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_SCHEMATA_COLLECTION);
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

        // Gets a collection, if it does not exist creates it
        return db.getCollection(collectionName);
    }

    /**
     * Stores the provided {@link MetadataSchemaItem} in the mongo collection
     * @param metadataSchema the {@link MetadataSchemaItem} to be inserted
     * @return the inserted {@link MetadataSchemaItem}
     * @throws MetadataStoreException when the insertion failed
     */
    public MetadataSchemaItem insert(MetadataSchemaItem metadataSchema) throws MetadataStoreException {
        ObjectMapper mapper = new ObjectMapper();
        MongoCollection collection;
        try {
            collection = getDefaultCollection();
            collection.insertOne(Document.parse(mapper.writeValueAsString(metadataSchema)));
            return metadataSchema;
        } catch (UnknownHostException| JsonProcessingException e) {
            throw new MetadataStoreException("Failed to insert metadata item", e);
        }
    }

    /**
     * Find the Metadata Schema based on the provided query in the mongo collection
     * @param query the {@link Bson} query to search the collection with
     * @return Document matching the {@link MetadataSchemaItem} found
     * @throws MetadataStoreException if unable to find the {@link MetadataSchemaItem}
     */
    public Document findOne(Bson query) throws MetadataStoreException {
        MongoCollection collection;
        try {
            collection = getDefaultCollection();
            return (Document) collection.find(query).first();
        } catch (UnknownHostException | NullPointerException e ) {
            throw new MetadataStoreException("Failed to find metadata schema item", e);
        }
    }


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
//     * Returns a {@link MetadataSchemaItem} with the matching {@code uuid} and {code tenantId}
//     * @param uuid
//     * @param tenantId
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataSchemaItem findByUuidAndTenant(String uuid, String tenantId) 
//    throws MetadataQueryException 
//    {   
//        try {
//            return (MetadataSchemaItem)getDefaultCollection().findOne(_getDBQueryForUuidAndTenant(uuid, tenantId));
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to find metadata schema by UUID", e);
//        }        
//    }
//    
//    /**
//     * Insert a new metadata item
//     * @param item
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataSchemaItem insert(MetadataSchemaItem item) throws MetadataQueryException {
//        try {
//            WriteResult<MetadataSchemaItem, String> result = getDefaultCollection().insert(item);
//            return result.getSavedObject();
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Insert a new metadata schema item if another {@link MetadataSchemaItem} with the same {@code schema}, 
//     * {@code tenantId}, {@code owner}, {@code associatedIds}, and {@code internalUser} 
//     * does not already exist.
//     *  
//     * @param item
//     * @return
//     * @throws MetadataQueryException
//     */
//    public MetadataSchemaItem insertIfNotPresent(MetadataSchemaItem item) 
//    throws MetadataQueryException 
//    {
//        try {
//            MetadataSchemaItem existingItem = getDefaultCollection().findOne(DBQuery.and(
//                                                                    DBQuery.is("name", item.getUuid()), 
//                                                                    DBQuery.is("tenantId", item.getTenantId()),
//                                                                    DBQuery.is("owner",  item.getOwner()),
//                                                                    DBQuery.is("schema",  item.getSchema()),
//                                                                    DBQuery.is("internalUsername",  item.getInternalUsername())));
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
//     * Update one or more metadata schema items in part or whole atomically 
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public List<MetadataSchemaItem> update(String uuid, String tenantId, Map<String, JsonNode> updates) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (updates == null || updates.isEmpty()) {
//                return new ArrayList<MetadataSchemaItem>();
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
//                 WriteResult<MetadataSchemaItem,String> result = getDefaultCollection().update(_getDBQueryForUuidAndTenant(uuid, tenantId), builder);
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
//     * Update one or more metadata schema items in part or whole atomically 
//     * @param uuid
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public boolean delete(String uuid, String tenantId) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (StringUtils.isEmpty(uuid)) {
//                return false;
//            }
//            else 
//            {
//                WriteResult<MetadataSchemaItem,String> result = getDefaultCollection()
//                        .remove(DBQuery.and(
//                                DBQuery.is("uuid", uuid), 
//                                DBQuery.is("tenantId", tenantId)));
//                
//                // do we need to check for errors?
//                if (!result.getWriteResult().getLastError().isEmpty()) {
//                    throw new MetadataQueryException("Failed to delete the metadata schema item", 
//                            result.getWriteResult().getLastError().getException());
//                }
//                
//                return true;
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
//    
//    /**
//     * Update one or more metadata schema items in part or whole atomically 
//     * @param uuids
//     * @param tenantId
//     * @param updates
//     * @return
//     * @throws MetadataQueryException
//     */
//    public int delete(List<String> uuids, String tenantId) 
//    throws MetadataQueryException 
//    {
//        try {
//            if (uuids == null || uuids.isEmpty()) {
//                return 0;
//            }
//            else 
//            {
//                WriteResult<MetadataSchemaItem,String> result = getDefaultCollection()
//                        .remove(DBQuery.and(
//                                DBQuery.in("uuid", uuids), 
//                                DBQuery.is("tenantId", tenantId)));
//                
//                // do we need to check for errors?
//                if (!result.getWriteResult().getLastError().isEmpty()) {
//                    throw new MetadataQueryException("Failed to update one or more metadata schema items", 
//                            result.getWriteResult().getLastError().getException());
//                }
//                return result.getWriteResult().getN();
//             }
//        } catch (MetadataQueryException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new MetadataQueryException("Unable to insert new metadata item", e);
//        }
//    }
}

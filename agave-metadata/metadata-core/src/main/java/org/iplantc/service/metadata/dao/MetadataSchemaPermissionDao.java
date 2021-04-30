/**
 *
 */
package org.iplantc.service.metadata.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.iplantc.service.metadata.model.MetadataSchemaPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.util.ServiceUtils;

import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * DAO class for metadata schemata. This needs moved into the mongodb with the schema records.
 *
 * @author dooley
 */
public class MetadataSchemaPermissionDao {

    private static final Logger log = Logger.getLogger(MetadataSchemaPermissionDao.class);

    private static MongoDatabase db = null;
    private static MongoClients mongoClients = null;
    private static com.mongodb.client.MongoClient mongov4Client = null;
    private static MetadataSchemaPermissionDao dao = null;

    public MetadataSchemaPermissionDao(MongoClients paramMongoClients) {
        this.mongoClients = paramMongoClients;
    }

    public MetadataSchemaPermissionDao() {
    }

    public static MetadataSchemaPermissionDao getInstance() {
        if (dao == null) {
            dao = new MetadataSchemaPermissionDao();
        }
        return dao;
    }

    public static MongoClient getMongoClients() {
        if (mongov4Client == null) {
            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));

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
    private static MongoCredential getMongoCredential() {
        return MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());
    }

    /**
     * Gets the default metadata collection from the default mongodb metatadata db.
     *
     * @return collection from the db
     */
    public static MongoCollection getDefaultCollection() {
        return getCollection(Settings.METADATA_DB_SCHEME, "metadata_schema_permissions");
    }

    /**
     * Gets the named collection from the named db.
     *
     * @param dbName         database name
     * @param collectionName collection name
     * @return collection from the db
     */
    public static MongoCollection getCollection(String dbName, String collectionName) {

        db = getMongoClients().getDatabase(dbName); //update to 4.0
        // Gets a collection, if it does not exist creates it
        return db.getCollection(collectionName);

    }

    /**
     * Gets the default {@link MetadataItem} configured metadata collection from the default mongodb metadata db
     *
     * @return collection from the db
     */
    public static MongoCollection<MetadataSchemaPermission> getDefaultMetadataSchemaPermissionCollection() {
        return getMetadataSchemaPermissionCollection(Settings.METADATA_DB_SCHEME, "metadata_schema_permissions");
    }

    /**
     * Get the named collection configured with the {@link MetadataItem} class from the named db
     *
     * @param dbName         database name
     * @param collectionName colletion name
     * @return collection from the db
     */
    public static MongoCollection<MetadataSchemaPermission> getMetadataSchemaPermissionCollection(String dbName, String collectionName) {
        db = getMongoClients().getDatabase(dbName);
        return db.getCollection(collectionName, MetadataSchemaPermission.class);
    }


    /************************************************************************************************************************/


    /**
     * Returns all metadata schema permissions for the given schemaId.
     *
     * @param schemaId the schema id for which we are fetching permissions
     * @return a list of permissions for the schema id
     * @throws org.iplantc.service.metadata.exceptions.MetadataException
     */
    @SuppressWarnings("unchecked")
    public static List<MetadataSchemaPermission> getBySchemaId(String schemaId)
            throws MetadataException {

        if (!ServiceUtils.isValid(schemaId))
            throw new MetadataException("Schema id cannot be null");

        List<MetadataSchemaPermission> pems = new ArrayList<>();

        try {
            MongoCollection<MetadataSchemaPermission> metadataItemMongoCollection;
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();

            Bson getQuery = eq("schemaId", schemaId);
            MongoCursor cursor = metadataItemMongoCollection.find(getQuery)
                    .sort(Sorts.ascending("username"))
                    .skip(0)
                    .batchSize(-Settings.MAX_PAGE_SIZE)
                    .limit(Settings.DEFAULT_PAGE_SIZE).cursor();

            while (cursor.hasNext()) {
                MetadataSchemaPermission metadataSchemaPermission = (MetadataSchemaPermission) cursor.next();
                pems.add(metadataSchemaPermission);
            }

            return pems;

        } catch (MongoException ex) {
            throw new MetadataException(ex);
        }
    }

    /**
     * Returns the {@link MetadataSchemaPermission#getSchemaId()} of {@link MetadataSchemaItem} to which
     * the user has read permission. Delegates to {@link #getUuidOfAllSharedMetataSchemaItemReadableByUser(String, int, int)}
     *
     * @param username the user for whom to look up permission grants
     * @return list of uuid to which the user has been granted read access.
     * @throws MetadataException
     */
    public static List<String> getUuidOfAllSharedMetataSchemaItemReadableByUser(String username)
            throws MetadataException {
        return getUuidOfAllSharedMetataSchemaItemReadableByUser(username, 0, Settings.DEFAULT_PAGE_SIZE);
    }

    /**
     * Returns the {@link MetadataSchemaPermission#getSchemaId()} of {@link MetadataSchemaItem} to which
     * the user has read permission.
     *
     * @param username the user for whom to look up permission grants
     * @param offset the number of results to skip before returning the response set
     * @param limit the maximum results to return
     * @return list of uuid to which the user has been granted read access.
     * @throws MetadataException
     */
    @SuppressWarnings("unchecked")
    public static List<String> getUuidOfAllSharedMetataSchemaItemReadableByUser(String username, int offset, int limit)
            throws MetadataException {

        if (StringUtils.isEmpty(username)) {
            throw new MetadataException("Username cannot be null in permission lookup");
        }

        List<String> pems = new ArrayList<>();

        try {
            MongoCollection<MetadataSchemaPermission> metadataItemMongoCollection;
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();

            Bson getQuery = and(eq("tenantId", TenancyHelper.getCurrentTenantId()),
                    and(
                            in("username", Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME, username)));

            Pattern regex = Pattern.compile("READ");
            Bson filter = or(eq("permission", regex), eq("permission", PermissionType.ALL.name()));

            MongoCursor cursor = metadataItemMongoCollection.find(getQuery)
                    .sort(Sorts.descending("lastUpdated"))
                    .skip(offset)
                    .batchSize(-Settings.MAX_PAGE_SIZE)
                    .limit(limit)
                    .filter(filter).cursor();

            while (cursor.hasNext()) {
                MetadataSchemaPermission metadataSchemaPermission = (MetadataSchemaPermission) cursor.next();
                pems.add(metadataSchemaPermission.getSchemaId());
            }
            return pems;
        } catch (MongoException ex) {
            throw new MetadataException(ex);
        }
    }

    /**
     * Gets the metadata permissions for the specified username and iod
     *
     * @param username the users to whom the permission is assigned
     * @param schemaId the id to delete
     * @return a matching permission if present, false otherwise
     * @throws org.iplantc.service.metadata.exceptions.MetadataException
     */
    @SuppressWarnings("unchecked")
    public static MetadataSchemaPermission getByUsernameAndSchemaId(String username, String schemaId) throws MetadataException {
        if (!ServiceUtils.isValid(username))
            throw new MetadataException("Username cannot be null");

        List<MetadataSchemaPermission> pems = new ArrayList<>();

        try {
            MongoCollection metadataItemMongoCollection;
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();

            Bson getQuery = and(eq("schemaId", schemaId), eq("username", username));
            MongoCursor cursor = metadataItemMongoCollection.find(getQuery)
                    .skip(0)
                    .batchSize(-Settings.MAX_PAGE_SIZE)
                    .limit(Settings.DEFAULT_PAGE_SIZE).cursor();

            while (cursor.hasNext()) {
                MetadataSchemaPermission metadataSchemaPermission = (MetadataSchemaPermission) cursor.next();
                pems.add(metadataSchemaPermission);
            }

            return pems.isEmpty() ? null : pems.get(0);
        } catch (MongoException ex) {
            throw new MetadataException(ex);
        }
    }

    public static MetadataSchemaPermission insert(MetadataSchemaPermission pem) throws MetadataException {
        if (pem == null)
            throw new MetadataException("Permission cannot be null");

        MongoCollection<MetadataSchemaPermission> metadataItemCollection;
        try {
            //using POJO Codec
            metadataItemCollection = getDefaultCollection();
            Document doc = new Document();
            doc.putAll(Map.of(
                    "schemaId", pem.getSchemaId(),
                    "username", pem.getUsername(),
                    "permission", pem.getPermission().name(),
                    "tenantId", pem.getTenantId(),
                    "lastUpdated", new Date()));

            metadataItemCollection.insertOne(pem);

            MongoCursor cursor = metadataItemCollection.find(eq("schemaId", pem.getSchemaId())).cursor();

            while (cursor.hasNext()) {
                return (MetadataSchemaPermission) cursor.next();
            }

        } catch (MongoException e) {
            throw new MetadataException("Failed to insert metadata item", e);
        }
        return null;
    }


    /**
     * Saves a new metadata permission. Upates existing ones.
     * @param pem the permission to delete
     * @throws org.iplantc.service.metadata.exceptions.MetadataException
     */
    public static MetadataSchemaPermission persist(MetadataSchemaPermission pem) throws MetadataException, MetadataStoreException {
        if (pem == null)
            throw new MetadataException("Permission cannot be null");

        MongoCollection<MetadataSchemaPermission> metadataItemMongoCollection;
        UpdateResult update;

        try {
            //get collection
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();

            Bson uuidAndTenantQuery = and(eq("username", pem.getUsername()), eq("schemaId", pem.getSchemaId()));

            Document doc = new Document();
            doc.putAll(Map.of(
                    "permission", pem.getPermission().name(),
                    "lastUpdated", new Date()));

            update = metadataItemMongoCollection.updateOne(uuidAndTenantQuery, new Document("$set", doc));

            if (update.getModifiedCount() == 0) {
                metadataItemMongoCollection.insertOne(pem);
            }
            //update success. fetch the doc
            MetadataSchemaPermission updatedItem = getByUsernameAndSchemaId(pem.getUsername(), pem.getSchemaId());
            if (updatedItem != null) {
                return updatedItem;
            } else {
                throw new MetadataStoreException("Metadata item " + pem.getSchemaId() + " is no longer present.");
            }
        } catch (MetadataStoreException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Failed to update permission: " + e.getMessage();
            throw new MetadataStoreException(msg, e);
        }
    }

    /**
     * Deletes the given iod permission.
     *
     * @param pem the permission to delete
     * @throws org.iplantc.service.metadata.exceptions.MetadataException
     */
    public static void delete(MetadataSchemaPermission pem) throws MetadataException {
        if (pem == null)
            throw new MetadataException("Permission cannot be null");

        try {
            MongoCollection<MetadataSchemaPermission> metadataItemMongoCollection;
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();


            Bson deleteQuery = and(eq("schemaId", pem.getSchemaId()), eq("tenantId", pem.getTenantId()));

            DeleteResult deleteResult = metadataItemMongoCollection.deleteOne(deleteQuery);
            if (deleteResult.getDeletedCount() == 0) {
                //delete unsuccessful
//				return null;
            }


        } catch (MongoException ex) {
            throw new MetadataException("Failed to delete metadata schema permission.", ex);
        }
    }

    /**
     * Deletes all permissions for the metadata schema with given schemaId
     *
     * @param schemaId the id to delete
     * @throws org.iplantc.service.metadata.exceptions.MetadataException
     */
    public static void deleteBySchemaId(String schemaId) throws MetadataException {
        if (schemaId == null) {
            return;
        }

        try {
            MongoCollection<MetadataSchemaPermission> metadataItemMongoCollection;
            metadataItemMongoCollection = getDefaultMetadataSchemaPermissionCollection();


            Bson deleteQuery = eq("schemaId", schemaId);

            DeleteResult deleteResult = metadataItemMongoCollection.deleteOne(deleteQuery);
            if (deleteResult.getDeletedCount() == 0) {
                //delete unsuccessful
//				return null;
            }

        } catch (MongoException ex) {
            throw new MetadataException("Failed to delete metadata schema permission", ex);
        }
    }

}

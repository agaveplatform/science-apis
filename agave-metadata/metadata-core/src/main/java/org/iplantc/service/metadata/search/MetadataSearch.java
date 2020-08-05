package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.google.gson.JsonObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import io.grpc.Metadata;
import io.grpc.internal.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.bson.conversions.Bson;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.SortSyntaxException;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.util.SimpleTimer;
import org.iplantc.service.common.util.StringToTime;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.*;
import org.iplantc.service.metadata.managers.MetadataRequestNotificationProcessor;
import org.iplantc.service.metadata.managers.MetadataRequestPermissionProcessor;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.json.JSONException;
import org.json.JSONObject;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.*;
import java.security.Permission;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class MetadataSearch {
//    private boolean bolImplicitPermissions;
    private String owner;
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
    private Bson dbQuery;

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

    public BasicDBObject parseUserQuery(String userQuery) throws MetadataQueryException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode mappedValue = mapper.createObjectNode();

        BasicDBObject query;
        query = BasicDBObject.parse(userQuery);
        for (String key : query.keySet()) {
            if (!StringUtils.equals(key, "name")) {
                if (query.get(key) instanceof String) {
                    if (((String) query.get(key)).contains("*")) {
                        try {
                            Pattern regexPattern = Pattern.compile(query.getString(key),
                                    Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                            query.put(key, regexPattern);

                            mappedValue.put(key, String.valueOf(regexPattern));
                        } catch (Exception e) {
                            throw new MetadataQueryException(
                                    "Invalid regular expression for " + key + " query");
                        }
                    }
                }
            }
        }
        return query;
    }

    /**
     * Parse {@link JsonNode} to {@link MetadataItem} with verified associatedIds
     *
     * @param jsonMetadata {@link JsonNode} parse from the query string
     * @throws MetadataQueryException if query values are missing or invalid
     */
    public void parseJsonMetadata(JsonNode jsonMetadata) throws MetadataQueryException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode items = mapper.createArrayNode();
            this.permissions = mapper.createArrayNode();
            this.notifications = mapper.createArrayNode();

            if (jsonMetadata.has("name") && jsonMetadata.get("name").isTextual()
                    && !jsonMetadata.get("name").isNull()) {
//                this.name = jsonMetadata.get("name").asText();
                metadataItem.setName(jsonMetadata.get("name").asText());
            } else {
                throw new MetadataQueryException(
                        "No name attribute specified. Please associate a value with the metadata name.");
            }

            if (jsonMetadata.has("value") && !jsonMetadata.get("value").isNull()) {
                if (jsonMetadata.get("value").isObject() || jsonMetadata.get("value").isArray())
                    this.value = jsonMetadata.get("value").toString();
                else
                    this.value = jsonMetadata.get("value").asText();
                this.metadataItem.setValue(this.parseValue(this.value));
            } else {
                throw new MetadataQueryException(
                        "No value attribute specified. Please associate a value with the metadata value.");
            }

            if (jsonMetadata.has("associationIds")) {
                if (jsonMetadata.get("associationIds").isArray()) {
                    items = (ArrayNode) jsonMetadata.get("associationIds");
                } else {
                    if (jsonMetadata.get("associationIds").isTextual())
                        items.add(jsonMetadata.get("associationIds").asText());
                }

//                MetadataAssociationList associationList = checkAssociationIds(items);
                MetadataAssociationList metadataAssociationList = checkAssociationIds_uuidApi(items);


//                this.metadataItem.setAssociations(associationList);
                this.metadataItem.setAssociations(metadataAssociationList);
            }

            if (jsonMetadata.hasNonNull("notifications")) {
                if (jsonMetadata.get("notifications").isArray()) {
                    this.notifications = (ArrayNode) jsonMetadata.get("notifications");
                } else {
                    throw new MetadataQueryException(
                            "Invalid notifications value. notifications should be an "
                                    + "JSON array of notification objects.");
                }
            }

            if (jsonMetadata.hasNonNull("permissions")) {
                if (jsonMetadata.get("permissions").isArray()) {
                    this.permissions = (ArrayNode) jsonMetadata.get("permissions");
                } else {
                    throw new MetadataQueryException(
                            "Invalid permissions value. permissions should be an "
                                    + "JSON array of permission objects.");
                }
            }

            if (jsonMetadata.has("schemaId") && jsonMetadata.get("schemaId").isTextual()) {
//                schemaId = jsonMetadata.get("schemaId").asText();'
                this.metadataItem.setSchemaId(jsonMetadata.get("schemaId").asText());
                checkSchemaId(this.metadataItem.getSchemaId());
            }
        } catch (MetadataQueryException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataQueryException(
                    "Unable to parse form. " + e.getMessage());
        }
    }

    /**
     * Parse {@link Document} to {@link MetadataItem}
     *
     * @param doc {@link Document} to parse
     * @throws MetadataQueryException if query is missing or invalid
     */
    public void parseDocument(Document doc) throws MetadataQueryException {
        try {
            for (String key : doc.keySet()) {
                if (StringUtils.equals(key, "name") && doc.get("name") != null) {
                    metadataItem.setName((String) doc.get("name"));
                } else {
                    throw new MetadataQueryException(
                            "No name attribute specified. Please associate a value with the metadata name.");
                }

                if (StringUtils.equals(key, "value") && doc.get("value") != null) {
                    this.value = (String) doc.get("value");
                    this.metadataItem.setValue(this.parseValue(this.value));
                } else {
                    throw new MetadataQueryException(
                            "No value attribute specified. Please associate a value with the metadata value.");
                }

                if (StringUtils.equals(key, "associationIds")) {
                    ArrayNode items = doc.get("associationIds", ArrayNode.class);
                    MetadataAssociationList associationList = checkAssociationIds(items);
                    this.metadataItem.setAssociations(associationList);
                    continue;
                }

                if (StringUtils.equals(key, "notifications")) {
                    this.notifications = doc.get("notifications", ArrayNode.class);
                }

                if (StringUtils.equals(key, "permissions")) {
                    ArrayNode permissions = doc.get("permissions", ArrayNode.class);
                    MetadataRequestPermissionProcessor permissionProcessor = new MetadataRequestPermissionProcessor(username,
                            this.metadataItem.getUuid());
                    permissionProcessor.process(permissions);
                    List<MetadataPermission> metaPemList = permissionProcessor.getPermissions();
                    this.metadataItem.setPermissions(metaPemList);
                    this.permissions = permissions;
                }

                if (StringUtils.equals(key, "schemaId")) {
                    this.metadataItem.setSchemaId((String) doc.get("schemaId"));
                    checkSchemaId(this.metadataItem.getSchemaId());
                }
            }

        } catch (MetadataQueryException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataQueryException(
                    "Unable to parse form. " + e.getMessage());
        }
    }

    /**
     * Parse string to {@link JsonNode} equivalent
     *
     * @param value to be parsed
     * @return {@link JsonNode} of {@code value}
     * @throws MetadataQueryException if invalid json format
     */
    public JsonNode parseValue(String value) throws MetadataQueryException {
        try {
            JsonFactory factory = new ObjectMapper().getFactory();
            return factory.createParser(value).readValueAsTree();
        } catch (IOException e) {
            throw new MetadataQueryException("Unable to parse value.", e);
        }
    }

    /**
     * Verify the associationIds using the agave-uuid api
     *
     * @param items {@link ArrayNode} of String associated uuids
     * @return {@link MetadataAssociationList} of valid associated uuids from {@code items}
     * @throws MetadataQueryException if no resource was found with any uuid in {@code items} was not found
     * @throws UUIDException          if unable to run query
     * @throws MetadataException      if unable to validate uuid using the agave-uuid api
     */
    public MetadataAssociationList checkAssociationIds_uuidApi(ArrayNode items) throws MetadataQueryException, UUIDException, MetadataException {
        if (this.associationList == null) {
            this.associationList = new MetadataAssociationList();
        }

        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                String associationId = items.get(i).asText();
                if (StringUtils.isNotEmpty(associationId)) {
                    AgaveUUID associationUuid = new AgaveUUID(associationId);
                    String apiOutput = getValidationResponse(associationId);
                    if (!apiOutput.isEmpty()) {
                        AssociatedReference associatedReference = parseValidationResponse(apiOutput);
                        associationList.add(associatedReference);
                    } else {
                        UUIDType type = associationUuid.getResourceType();

                        if (UUIDType.METADATA == type)
                            throw new MetadataQueryException("No metadata resource found with uuid " + associationId);
                        else if (UUIDType.SCHEMA == type)
                            throw new MetadataQueryException("No metadata schema found with uuid " + associationId);
                        else
                            throw new MetadataQueryException("No associated object found with uuid " + associationId);
                    }
                }
            }
        }
        return associationList;
    }

    /**
     * Verify and return list of verified associated Uuids from {@link ArrayNode} items
     *
     * @param items {@link ArrayNode} of String uuids
     * @return {@link MetadataAssociationList} of valid associated Uuids from {@code items}
     * @throws MetadataQueryException       if query is missing or invalid
     * @throws UUIDException                if unable to run query
     * @throws PermissionException          if user does not have read permissions
     * @throws MetadataAssociationException if the uuid is invalid
     */
    public MetadataAssociationList checkAssociationIds(ArrayNode items) throws MetadataQueryException, UUIDException, PermissionException, MetadataAssociationException {
//        MongoCollection<MetadataItem> collection = metadataDao.getDefaultMetadataItemCollection();

        if (this.associationList == null) {
            this.associationList = new MetadataAssociationList();
        }

        BasicDBList associations = new BasicDBList();
        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                String associationId = items.get(i).asText();

                if (StringUtils.isNotEmpty(associationId)) {
                    AgaveUUID associationUuid = new AgaveUUID(associationId);

                    if (UUIDType.METADATA == associationUuid.getResourceType()) {
                        BasicDBObject associationQuery = new BasicDBObject("uuid", associationId);
                        associationQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
//                            BasicDBObject associationDBObj = (BasicDBObject) collection.find(associationQuery).limit(1);
                        List<MetadataItem> associationDocument = metadataDao.find(this.username, associationQuery);

                        if (associationDocument.size() == 0) {
                            throw new MetadataQueryException(
                                    "No metadata resource found with uuid " + associationId);
                        }
                    } else if (UUIDType.SCHEMA == associationUuid.getResourceType()) {

                        MongoCollection schemataCollection = getSchemaCollection();

                        BasicDBObject associationQuery = new BasicDBObject("uuid", associationId);
                        associationQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
//                            BasicDBObject associationDBObj = (BasicDBObject) schemataCollection.find(
//                                    associationQuery).limit(1);
                        Document associationDocument = (Document) schemataCollection.find(
                                associationQuery).first();

                        if (associationDocument == null) {
                            throw new MetadataQueryException(
                                    "No metadata schema resource found with uuid " + associationId);
                        }
                    } else {
                        try {
                            associationUuid.getObjectReference();
                        } catch (Exception e) {
                            throw new MetadataQueryException(
                                    "No associated object found with uuid " + associationId);
                        }
                    }
                    associations.add(items.get(i).asText());
                    associationList.add(items.get(i).asText());
//                        metadataItem.setAssociations(associationList);

                }
            }
        }

        return associationList;

    }

    /**
     * Verify the {@code schemaId} is a valid schemaId and verify that the value conforms to it
     *
     * @param schemaId uuid of Schema
     * @throws MetadataSchemaValidationException if the value does not conform to the {@code schemaId}
     */
    public void checkSchemaId(String schemaId) throws MetadataSchemaValidationException {
        try {
            MongoCollection schemaCollection = getSchemaCollection();

            // if a schema is given, validate the metadata against that registered schema
            if (schemaId != null) {
                BasicDBObject schemaQuery = new BasicDBObject("uuid", schemaId);
                schemaQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
                BasicDBObject schemaDBObj = (BasicDBObject) schemaCollection.find(schemaQuery).limit(1);

                // lookup the schema
                if (schemaDBObj == null) {
                    throw new MetadataQueryException(
                            "Specified schema does not exist.");
                }

                // check user permsisions to view the schema
                try {
                    MetadataSchemaPermissionManager schemaPM = new MetadataSchemaPermissionManager(schemaId,
                            (String) schemaDBObj.get("owner"));
                    if (!schemaPM.canRead(this.username)) {
                        throw new MetadataException("User does not have permission to read metadata schema");
                    }
                } catch (MetadataException e) {
                    throw new MetadataQueryException(e.getMessage());
                }

                // now validate the json against the schema
                String schema = schemaDBObj.getString("schema");
                try {
                    JsonFactory factory = new ObjectMapper().getFactory();
                    JsonNode jsonSchemaNode = factory.createParser(schema).readValueAsTree();
                    JsonNode jsonMetadataNode = factory.createParser(value).readValueAsTree();
                    AgaveJsonValidator validator = AgaveJsonSchemaFactory.byDefault().getValidator();

                    ProcessingReport report = validator.validate(jsonSchemaNode, jsonMetadataNode);
                    if (!report.isSuccess()) {
                        StringBuilder sb = new StringBuilder();
                        for (Iterator<ProcessingMessage> reportMessageIterator = report.iterator(); reportMessageIterator.hasNext(); ) {
                            sb.append(reportMessageIterator.next().toString() + "\n");

                        }
                        throw new MetadataSchemaValidationException(
                                "Metadata value does not conform to schema. \n" + sb.toString());
                    }

                    metadataItem.setSchemaId(schemaId);

                } catch (MetadataSchemaValidationException e) {
                    throw e;
                } catch (Exception e) {
                    throw new MetadataSchemaValidationException(
                            "Metadata does not conform to schema.");
                }
            }
        } catch (MetadataQueryException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieve the Schema Collection
     *
     * @return {@link MongoCollection} for schemas
     */
    public MongoCollection getSchemaCollection() {
        MongoCollection schemaCollection = null;
        metadataDao = new MetadataDao().getInstance();
        schemaCollection = metadataDao.getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_SCHEMATA_COLLECTION);

        return schemaCollection;
    }

    /**
     * Retrieve the Metadata collection
     *
     * @return {@link MongoCollection} for metadata
     */
    public MongoCollection getCollection() {
        MongoCollection collection = null;
        collection = metadataDao.getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
        return collection;
    }

    /**
     * Remove all documents from the Metadata collection
     *
     * @throws UnknownHostException
     */
    public void clearCollection() throws UnknownHostException {
        metadataDao.clearCollection();
    }

    /**
     * @return {@link Bson} filter for the current tenantId
     */
    public Bson getTenantIdQuery() {
        return eq("tenantId", this.metadataItem.getTenantId());
    }

    /**
     * @return {@link Bson} filter for the current {@link MetadataItem} uuid
     */
    public Bson getUuidQuery() {
        return eq("uuid", this.metadataItem.getUuid());
    }

    /**
     * @return {@link Bson} filter for read permissions for the {@link MetadataItem}
     */
    public Bson getReadQuery() {
        return or(in("owner", this.accessibleOwners),
                elemMatch("permissions", and(
                        eq("username", this.username),
                        in("permission", PermissionType.ALL.toString(), PermissionType.READ.toString(),
                                PermissionType.READ_WRITE.toString(), PermissionType.READ_EXECUTE.toString()))));
    }

    /**
     * @return {@link Bson} filter for write permissions for the {@link MetadataItem}
     */
    public Bson getWriteQuery() {
        return or(in("owner", this.accessibleOwners),
                elemMatch("permissions", and(
                        eq("username", this.username),
                        in("permission", PermissionType.READ_WRITE.toString(), PermissionType.ALL.toString(),
                                PermissionType.WRITE.toString(), PermissionType.WRITE_EXECUTE.toString(),
                                PermissionType.WRITE_PERMISSION.toString(), PermissionType.READ_WRITE_PERMISSION.toString()))));

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

    public List<String> setAccessibleOwnersImplicit(){
        if (this.accessibleOwners == null)
            this.accessibleOwners = new ArrayList<>();
        this.accessibleOwners.add(this.username);
        return this.accessibleOwners;
    }




    /**
     * Parse JsonString {@code userQuery} to {@link Document}
     *
     * @param userQuery JsonString to parse
     * @return {@link Document} of the JsonString {@code userQuery}
     * @throws MetadataQueryException if invalid Json format
     */
    public Document getDocumentFromQuery(String userQuery) throws MetadataQueryException {
        Document doc = new Document();
        if (StringUtils.isNotEmpty(userQuery))
            try {
                doc = Document.parse(userQuery);
            } catch (Exception e) {
                throw new MetadataQueryException("Unable to parse query ", e);
            }
        return doc;
    }

    /**
     * Find all {@link MetadataItem} matching the {@code userQuery} and the offset/limit specified
     *
     * @param userQuery String query to search the collection for
     * @return list of {@link MetadataItem} matching the {@code userQuery} in the sort order specified
     * @throws MetadataQueryException if {@code userQuery} is invalid format
     */
    public List<MetadataItem> find(String userQuery) throws MetadataQueryException {
        List<MetadataItem> result = new ArrayList<>();
        try {
            Document doc;
            doc = getDocumentFromQuery(userQuery);
            Bson permissionFilter = and(getTenantIdQuery(), doc, getReadQuery());

            BasicDBObject order = (orderField == null) ? new BasicDBObject() : new BasicDBObject(orderField, orderDirection);
            metadataDao.setAccessibleOwners(this.accessibleOwners);
            result = metadataDao.find(this.username, permissionFilter, offset, limit, order);

        } catch (MetadataQueryException e) {
            throw new MetadataQueryException("Unable to parse query.");
        }
        return result;
    }

    /**
     * Find all documents in the collection
     *
     * @return list of {@link MetadataItem} found
     * @throws UnknownHostException if the connection cannot be found/created, or db connection is bad
     */
    public List<MetadataItem> findAll() throws UnknownHostException {
        return metadataDao.findAll();
    }

    /**
     * Update the metadata item
     *
     * @return {@link MetadataItem} that was updated successfully
     * @throws MetadataException      if unable to update the {@link MetadataItem} in the metadata collection
     * @throws MetadataStoreException if unable to connect to the metadata collection
     * @throws PermissionException    if user does not have permission to update the {@link MetadataItem}
     */
    public MetadataItem updateMetadataItem() throws MetadataException, PermissionException {
        metadataDao.setAccessibleOwners(this.accessibleOwners);
        return metadataDao.updateMetadata(this.metadataItem, this.username);
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

//    public void setMetadataPermission(String username, PermissionType pem, String group) throws MetadataStoreException, MetadataException, PermissionException, UnknownHostException {
//        if (StringUtils.isBlank(username)) {
//            throw new MetadataException("Invalid username");
//        }
//
////        if (getAuthenticatedUsername().equals(username))
////            return;
//
//        if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
//                StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
//            boolean worldAdmin = JWTClient.isWorldAdmin();
//            boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
//            if (!tenantAdmin && !worldAdmin) {
//                throw new PermissionException("User does not have permission to edit public metadata item permissions");
//            }
//        }
//    }

    /**
     * Add/update the user's permission to {@code pem}
     *
     * @param userToUpdate String user to update
     * @param group    group to be updated
     * @param pem      {@link PermissionType} to be updated to
     * @throws MetadataException      if unable to update the permission of {@code user}
     * @throws MetadataStoreException
     */
    public void updatePermissions(String userToUpdate, String group, PermissionType pem) throws MetadataException, MetadataStoreException, UnknownHostException {
        MetadataPermission metadataPermission;
        //check if user has write permissions
            if (pem.equals(PermissionType.NONE) || pem == null) {
                //delete permission
                MetadataPermission pemDelete = metadataItem.getPermissions_User(userToUpdate);
                metadataItem.updatePermissions_delete(pemDelete);
                List<MetadataPermission> metadataPermissionsList = metadataItem.getPermissions();

            } else {
                metadataPermission = new MetadataPermission(metadataItem.getUuid(), userToUpdate, pem);
                metadataPermission.setGroup(group);

                metadataItem.updatePermissions(metadataPermission);
            }
            metadataDao.updatePermission(metadataItem, this.username);
    }

    /**
     * Return all permissions for the user
     *
     * @param user to find permission for
     * @return list of {@link MetadataItem} where the user was specified permissions
     */
    public List<MetadataItem> findPermission_User(String user, String uuid) {
        //only the owner or tenantAdmin can access if no permissions are explicitly set
        return metadataDao.find(user, and(eq("uuid", uuid),
                or(eq("owner", user), eq("permissions.username", user))));
    }

    /**
     * Return all permissions for given uuid
     *
     * @param uuid to search for
     * @return list of {@link MetadataPermission} for the provided uuid
     */
    public MetadataItem findPermission_Uuid(String uuid) {
        List<MetadataPermission> permissionList = new ArrayList<>();
        List<MetadataItem> metadataItemList = metadataDao.find(this.username, eq("uuid", uuid));
        return metadataItemList.get(0);
    }

    /**
     * Verify if all the uuids of the MetadataItems in the given list are valid
     *
     * @param metadataItemList list of {@link MetadataItem} to validate the uuid for
     * @return true if all uuids in {@code metadataItemList} are valid
     * @throws MetadataException if unable to validate all the uuid in {@code metadataItemList}
     */
    public boolean validateUuids(List<MetadataItem> metadataItemList) throws MetadataException {
        for (MetadataItem metadataItem : metadataItemList) {
            if (!validateUuid(metadataItem.getUuid()))
                return false;
        }
        return true;
    }

    /**
     * Verify if given uuid is a valid uuid
     *
     * @param uuid to check
     * @return true if uuid is valid
     * @throws MetadataException if unable to validate {@code uuid}
     */
    public boolean validateUuid(String uuid) throws MetadataException {
        String apiOutput = getValidationResponse(uuid);

        if (StringUtils.isNotEmpty(apiOutput))
            return true;
        return false;
    }

    /**
     * Build the URL to query the agave-uuid api with the {@code uuid}
     *
     * @param uuid to check
     * @return String URL for the GET query to agave-uuid api
     * @throws TenantException    if unable to find the Tenant by TenantId
     * @throws URISyntaxException if unable to build a valid URL from the uuid
     */
    public String buildUuidValidationURL(String uuid) throws TenantException, URISyntaxException {
        Tenant tenant = null;
        String strUrl = "";
        URI uri = null;
        tenant = new TenantDao().findByTenantId(TenancyHelper.getCurrentTenantId());
        URI tenantBaseUrl = URI.create(tenant.getBaseUrl());
        URIBuilder builder = new URIBuilder();
        builder.setScheme(tenantBaseUrl.getHost());
        builder.setPath("uuid/v2");
        builder.setParameter("uuid", uuid);
        uri = builder.build();
        strUrl = uri.toString();
        return strUrl;
    }

    /**
     * Retrieve the GET response from agave-uuid api to validate the given uuid
     *
     * @param uuid to check
     * @return String of the response entity
     * @throws MetadataException if the uuid cannot be verified using the agave-uuid api
     */
    public String getValidationResponse(String uuid) throws MetadataException {
        //        String baseUrl = "http://localhost/uuid/v2/" + uuid;
        try {
            String strUrl = buildUuidValidationURL(uuid);
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(strUrl);
            HttpResponse httpResponse = httpClient.execute(getRequest);

            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                HttpEntity httpEntity = httpResponse.getEntity();
                return EntityUtils.toString(httpEntity);
            }
        } catch (URISyntaxException e) {
            throw new MetadataException("Invalid URI syntax ", e);
        } catch (TenantException e) {
            throw new MetadataException("Unable to retrieve valid tenant information ", e);
        } catch (Exception e) {
            throw new MetadataException("Unable to validate uuid ", e);
        }
        return null;
    }

    /**
     * Parse response from uuid validation to {@link AssociatedReference}
     *
     * @param validationResponse JSON String response
     * @return {@link AssociatedReference} with valid uuid and links
     * @throws MetadataException if unable to create valid {@link AssociatedReference}
     */
    public AssociatedReference parseValidationResponse(String validationResponse) throws MetadataException {
        try {
            JSONObject jsonObject = new JSONObject(validationResponse);

            AgaveUUID uuid = new AgaveUUID(jsonObject.getString("uuid"));
            String title = jsonObject.getString("type");
            String links = jsonObject.getJSONObject("_links")
                    .getJSONObject("self")
                    .getString("href");

            return new AssociatedReference(uuid, links);
        } catch (JSONException e) {
            throw new MetadataException("Invalid Json response ", e);
        } catch (UUIDException e) {
            throw new MetadataException("Invalid uuid value ", e);
        }
    }

    /**
     * Add the {@code associatedReference} to the associationList of the {@link MetadataItem}
     *
     * @param associatedReference to add to the associated id list
     * @return the {@code associatedReference} that was added successfully
     */
    public AssociatedReference setAssociatedReference(AssociatedReference associatedReference) {
        this.associationList = this.metadataItem.getAssociations();
        this.associationList.add(associatedReference);
        this.metadataItem.setAssociations(this.associationList);
        return associatedReference;
    }
}

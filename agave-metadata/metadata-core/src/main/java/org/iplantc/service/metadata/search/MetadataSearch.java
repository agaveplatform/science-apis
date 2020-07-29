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
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import io.grpc.Metadata;
import io.grpc.internal.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataSchemaValidationException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
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
    private boolean bolImplicitPermissions;
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


    MetadataDao metadataDao;

    private Bson dbQuery;
    public MetadataSearch(boolean bolImplicitPermissions,String username) {
        this.bolImplicitPermissions = bolImplicitPermissions;
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
        ObjectMapper mapper  = new ObjectMapper();
        ObjectNode mappedValue = mapper.createObjectNode();

        BasicDBObject query;
        query = BasicDBObject.parse(userQuery);
        for (String key : query.keySet()) {
            if (!StringUtils.equals(key, "name")){
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

                MetadataAssociationList associationList = checkAssociationIds(items);


                this.metadataItem.setAssociations(associationList);
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
                    permissions = (ArrayNode) jsonMetadata.get("permissions");
                    MetadataRequestPermissionProcessor permissionProcessor = new MetadataRequestPermissionProcessor(username,
                            this.metadataItem.getUuid());
                    permissionProcessor.process(permissions);
                    List<MetadataPermission> metaPemList = permissionProcessor.getPermissions();
                    this.metadataItem.setPermissions(metaPemList);

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

    public JsonNode parseValue(String value) throws MetadataQueryException {
        try {
            JsonFactory factory = new ObjectMapper().getFactory();
            return factory.createParser(value).readValueAsTree();
        } catch (IOException e) {
            throw new MetadataQueryException("Unable to parse value.", e);
        }
    }

    public MetadataAssociationList checkAssociationIds(ArrayNode items) throws MetadataQueryException, UnknownHostException {
        MongoCollection<MetadataItem> collection = metadataDao.getDefaultMetadataItemCollection();

        if (this.associationList == null) {
            this.associationList = new MetadataAssociationList();
        }

        BasicDBList associations = new BasicDBList();
        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                try {
                    String associationId = items.get(i).asText();


                    if (StringUtils.isNotEmpty(associationId)) {
                        AgaveUUID associationUuid = new AgaveUUID(associationId);

                        //calling uuid api to validate
                        if (!validateUuid(associationId)) {
                            if (UUIDType.METADATA == associationUuid.getResourceType()) {
                                throw new MetadataQueryException(
                                        "No metadata resource found with uuid " + associationId);
                            } else if (UUIDType.SCHEMA == associationUuid.getResourceType()) {
                                throw new MetadataQueryException(
                                        "No metadata schema resource found with uuid " + associationId);
                            } else {
                                throw new MetadataQueryException(
                                        "No associated object found with uuid " + associationId);
                            }
                        }

                        if (UUIDType.METADATA == associationUuid.getResourceType()) {
                            BasicDBObject associationQuery = new BasicDBObject("uuid", associationId);
                            associationQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
//                            BasicDBObject associationDBObj = (BasicDBObject) collection.find(associationQuery).limit(1);
                            MetadataItem associationDocument = collection.find(associationQuery).first();

                            if (associationDocument == null) {
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
                } catch (MetadataQueryException e) {
                    throw e;
                } catch (Exception e) {
                    throw new MetadataQueryException(
                            "Unable to parse association ids.", e);
                }
            }
        }

        return associationList;

    }

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

    public void processNotifications() throws NotificationException {
        MetadataRequestNotificationProcessor notificationProcessor = new MetadataRequestNotificationProcessor(
                username,
                this.metadataItem.getUuid());
        notificationProcessor.process(this.notifications);

        this.metadataItem.setNotifications(notificationProcessor.getNotifications());
    }

    public MongoCollection getSchemaCollection(){
        MongoCollection schemaCollection = null;
        try {
            metadataDao = new MetadataDao().getInstance();
            schemaCollection =  metadataDao.getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_SCHEMATA_COLLECTION);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return schemaCollection;
    }

    public MongoCollection getCollection(){
        MongoCollection collection = null;
        try {
            collection = metadataDao.getCollection(Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_COLLECTION);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            collection = null;
        }
        return collection;
    }

    public void clearCollection() throws UnknownHostException {
        metadataDao.clearCollection();
    }

    public Bson getTenantIdQuery(){
        return eq("tenantId", this.metadataItem.getTenantId());
    }

    public Bson getUuidQuery(){
        return eq("uuid", this.metadataItem.getUuid());
    }

    public Bson getReadQuery(){
        Bson docFilter = elemMatch("permissions", and (eq("username", this.username),
                in("permission", Arrays.asList(PermissionType.ALL.toString(), PermissionType.READ.toString(),
                        PermissionType.READ_WRITE.toString(), PermissionType.READ_EXECUTE.toString()))));

        Bson permissionFilter;
        if (bolImplicitPermissions)
            permissionFilter = or (in("owner", Arrays.asList(this.username, Settings.WORLD_USER_USERNAME, Settings.PUBLIC_USER_USERNAME)), docFilter);
        else
            permissionFilter = docFilter;

        return permissionFilter;
//        return or(
//                eq("owner", this.username),
//                elemMatch("permissions",
//                        and(
//                                eq("username", this.username),
//                                nin("permissions", PermissionType.NONE.toString()))));
    }

    public Bson getWriteQuery(){
        List<String> accessibleOwners = new ArrayList<>();
        if (!bolImplicitPermissions) {
            accessibleOwners = Arrays.asList(this.username,
                    Settings.PUBLIC_USER_USERNAME,
                    Settings.WORLD_USER_USERNAME);
        } else {
            accessibleOwners.add(this.username);
        }

        return or(in("owner", accessibleOwners),
                elemMatch("permissions", and(
                        eq("username", this.username),
                        in("permissions", PermissionType.READ_WRITE.toString(),
                                PermissionType.WRITE.toString(), PermissionType.WRITE_EXECUTE.toString(),
                                PermissionType.WRITE_PERMISSION.toString(), PermissionType.READ_WRITE_PERMISSION.toString()))));
    }

    public List<MetadataItem> find(String userQuery) throws MetadataException, MetadataQueryException {
        List<MetadataItem> result = new ArrayList<>();
        try {
            Document doc;

            if (StringUtils.isEmpty(userQuery))
                doc = new Document();
            else
                try {
                    doc = Document.parse(userQuery);
                } catch (Exception e) {
                    throw new MetadataQueryException();
                }

            Bson permissionFilter = and(getTenantIdQuery(), doc, getReadQuery());

            BasicDBObject order = (orderField == null) ? new BasicDBObject() : new BasicDBObject(orderField, orderDirection);

            result = metadataDao.find(this.username, permissionFilter, offset, limit, order);

        } catch (MetadataStoreException e) {
            throw new MetadataException("Unable to find item based on query.", e);
        } catch (MetadataQueryException e) {
            throw new MetadataQueryException("Unable to parse query.");
        }
        return result;
    }

    public List<MetadataItem> findAll() throws UnknownHostException {
        return metadataDao.findAll();
    }

    public MetadataItem updateMetadataItem() throws MetadataException, MetadataStoreException, PermissionException {
        MetadataItem result = metadataDao.find_uuid(and(eq("uuid", this.metadataItem.getUuid()),
                eq("tenantId", this.metadataItem.getTenantId())));

        if (result!=null){
            //item exists, check if user has the correct permissions to update item
            metadataItem.setOwner(result.getOwner());
            MetadataPermission pem = result.getPermissions_User(this.username);
            if (pem == null) {
                if (StringUtils.equals(this.username, result.getOwner())){
                    pem = new MetadataPermission();
                    pem.setPermission(PermissionType.ALL);
                    pem.setGroup(null);
                    pem.setUsername(this.username);
                    pem.setUuid(result.getUuid());
                } else {
                    throw new PermissionException("User does not have the permission to edit this item.");
                }
            }
            checkPermission(pem.getPermission(), this.username, true);
            this.metadataItem = metadataDao.updateMetadata(metadataItem, this.username);

        } else {
            metadataItem.setOwner(this.username);
            this.metadataItem = metadataDao.insert(metadataItem);
        }
        return this.metadataItem;
    }

    /**
     * Check if user has correct permissions, throw Permission Exception if the user doesn't have the
     * correct permission
     * @param pem user's Permission type
     * @param username user to check
     * @param bolWrite true for read/write permission, false for read permission
     * @throws PermissionException
     * @throws MetadataException
     */
    public void checkPermission (PermissionType pem, String username, boolean bolWrite) throws PermissionException, MetadataException {
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

    public void setMetadataPermission(String username, PermissionType pem, String group) throws MetadataStoreException, MetadataException, PermissionException {
        if (StringUtils.isBlank(username)) {
            throw new MetadataException("Invalid username");
        }

//        if (getAuthenticatedUsername().equals(username))
//            return;

        if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
                StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
            boolean worldAdmin = JWTClient.isWorldAdmin();
            boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
            if (!tenantAdmin && !worldAdmin) {
                throw new PermissionException("User does not have permission to edit public metadata item permissions");
            }
        }


        if (pem == null || pem.equals(PermissionType.NONE)) {
            //delete permission if it exists
            metadataDao.deleteUserPermission(this.metadataItem, username);
            NotificationManager.process(this.metadataItem.getUuid(), MetadataEventType.PERMISSION_REVOKE.name(), username);
        } else {
            metadataDao.updatePermission(metadataItem, this.username, pem);
            NotificationManager.process(this.metadataItem.getUuid(), MetadataEventType.PERMISSION_UPDATE.name(), username);
        }

    }

    /**
     * Add/update the user's permission to pem
     * @param username user to update
     * @param group group to be updated
     * @param pem permission to be updated to
     * @throws MetadataException
     */
    public void updatePermissions(String username, String group, PermissionType pem) throws MetadataException {
        MetadataPermission metadataPermission;

        try {
            metadataPermission = new MetadataPermission();
            metadataPermission.setPermission(pem);
            metadataPermission.setUsername(username);
            metadataPermission.setGroup(group);
            metadataPermission.setUuid(metadataItem.getUuid());

            metadataItem.updatePermissions(metadataPermission);
            metadataDao.updatePermission(metadataItem, username, pem);

        } catch (MetadataException | MetadataStoreException e) {
            throw new MetadataException("Unable to update permission.", e);
        }
    }

    /**
     * Remove the permission for the specified user
     * @param user to remove permission for
     * @return MetadataItem with user's permission removed
     * @throws MetadataStoreException
     */
    public MetadataItem deletePermission(String user) throws MetadataStoreException {
        return metadataDao.deleteUserPermission(this.metadataItem, user);
    }

    /**
     * Find all permissions for the user
     * @param user
     * @return
     */
    public List<MetadataItem> findPermission_User(String user, String uuid) throws MetadataStoreException {
        //only the owner or tenantAdmin can access if no permissions are explicitly set
        return metadataDao.find(user, and(eq("uuid", uuid),
                or (eq("owner", user),eq("permissions.username", user))));
    }

    public MetadataItem findPermission_Uuid(String uuid) throws MetadataStoreException {
        List<MetadataPermission> permissionList = new ArrayList<>();
        List<MetadataItem> metadataItemList = metadataDao.find(this.username, eq("uuid", uuid));
        return metadataItemList.get(0);
    }

    public boolean validateUuid(String uuid) {
//        String baseUrl = "http://localhost/uuid/v2/" + uuid;

        Tenant tenant = null;
        try{
            tenant = new TenantDao().findByTenantId(TenancyHelper.getCurrentTenantId());

            URI uri = null;
            URI tenantBaseUrl = URI.create(tenant.getBaseUrl());
            URIBuilder builder = new URIBuilder();
            builder.setScheme(tenantBaseUrl.getHost());
            builder.setPath("uuid/v2");
            builder.setParameter("uuid", uuid);
            uri = builder.build();
            String strUrl = uri.toString();

            HttpResponse response = null;
            HttpClient client = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(strUrl);
            response = client.execute(getRequest);

            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity httpEntity = response.getEntity();
                String apiOutput = EntityUtils.toString(httpEntity);
                System.out.println(apiOutput);
                if (StringUtils.isNotEmpty(apiOutput)) {
                    return true;
                }
            }
        } catch (IOException | TenantException | URISyntaxException e) {
        }
        return false;
    }

}

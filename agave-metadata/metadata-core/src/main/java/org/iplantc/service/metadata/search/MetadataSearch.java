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
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.SortSyntaxException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.util.StringToTime;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataSchemaValidationException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataRequestPermissionProcessor;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.notification.managers.NotificationManager;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.security.Permission;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class MetadataSearch {
//    private String userQuery = "";
    private boolean bolImplicitPermissions = true;
//    private String uuid;
//    private String tenantId;
    private String owner;
//    private String schemaId;
//    private String name;
    private String value;
    private String username;
    private MetadataAssociationList associationList;
    private String orderField;
    private int orderDirection;
    private int offset;
    private int limit;
    private MetadataItem metadataItem;

    MetadataDao metadataDao;

    private Bson dbQuery;
    public MetadataSearch(boolean bolImplicitPermissions,String username, String uuid) {
//        this.userQuery = userQuery;
        this.bolImplicitPermissions = bolImplicitPermissions;
        this.metadataItem = new MetadataItem();
        metadataItem.setUuid(uuid);
        metadataItem.setTenantId(TenancyHelper.getCurrentTenantId());
        this.username = username;
        this.metadataDao = new MetadataDao().getInstance();
    }

//    public String getUserQuery() {
//        return userQuery;
//    }
//
//    public void setUserQuery(String userQuery) {
//        this.userQuery = userQuery;
//    }

    public boolean isBolImplicitPermissions() {
        return bolImplicitPermissions;
    }

    public void setBolImplicitPermissions(boolean bolImplicitPermissions) {
        this.bolImplicitPermissions = bolImplicitPermissions;
    }

//    public String getUuid() {
//        return uuid;
//    }
//
//    public void setUuid(String uuid) {
//        this.uuid = uuid;
//    }
//
//    public String getTenantId() {
//        return tenantId;
//    }
//
//    public void setTenantId(String tenantId) {
//        this.tenantId = tenantId;
//    }
//
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getOwner() {
//        return owner;
        return this.metadataItem.getOwner();
    }

    public void setOwner(String owner) {
        this.metadataItem.setOwner(owner);
//        this.owner = owner;
    }

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }

    public void setMetadataItem(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
    }

    //
//    public String getSchemaId() {
//        return schemaId;
//    }
//
//    public void setSchemaId(String schemaId) {
//        this.schemaId = schemaId;
//    }

    public Bson createDBQuery() {
        dbQuery = and(eq("tenantId", "111"));

        return dbQuery;
    }

    public BasicDBObject parseUserQuery(String userQuery) throws MetadataQueryException, IOException {
        ObjectMapper mapper  = new ObjectMapper();
        ObjectNode mappedValue = mapper.createObjectNode();

        BasicDBObject query;
//        parseValue(userQuery);
        query = BasicDBObject.parse(userQuery);
        for (String key : query.keySet()) {
            if (StringUtils.equals(key, "name")){
//                metadataItem.setName((String)query.get(key));
            } else {
                if (query.get(key) instanceof String) {
                    if (((String) query.get(key)).contains("*")) {
                        try {
                            Pattern regexPattern = Pattern.compile((String) query.getString(key),
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
            ArrayNode permissions = mapper.createArrayNode();
            ArrayNode notifications = mapper.createArrayNode();


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
                    notifications = (ArrayNode) jsonMetadata.get("notifications");
//                    this.metadataItem.setNotifications(notifications);
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

    public MetadataAssociationList checkAssociationIds(ArrayNode items) throws MetadataQueryException, UnknownHostException {
        MongoCollection<MetadataItem> collection = metadataDao.getDefaultMetadataItemCollection();

        if (this.associationList == null) {
            this.associationList = new MetadataAssociationList();
        }

        BasicDBList associations = new BasicDBList();
        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                try {
                    String associationId = (String) items.get(i).asText();
                    if (StringUtils.isEmpty(associationId)) {
                        continue;
                    } else {
                        AgaveUUID associationUuid = new AgaveUUID(associationId);
                        if (UUIDType.METADATA == associationUuid.getResourceType()) {
                            BasicDBObject associationQuery = new BasicDBObject("uuid", associationId);
                            associationQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
//                            BasicDBObject associationDBObj = (BasicDBObject) collection.find(associationQuery).limit(1);
                            MetadataItem associationDocument = (MetadataItem) collection.find(associationQuery).first();

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

    public JsonNode parseValue(String value) throws MetadataQueryException {
        try {
            JsonFactory factory = new ObjectMapper().getFactory();
            JsonNode jsonMetadataNode = factory.createParser(value).readValueAsTree();
            return jsonMetadataNode;
        } catch (IOException e) {
            throw new MetadataQueryException("Unable to parse value.", e);
        }
    }

    public void updatePermissions(String username, String group, PermissionType pem){
        MetadataPermission metadataPermission;

        try {
            metadataPermission = new MetadataPermission();
            metadataPermission.setPermission(pem);
            metadataPermission.setUsername(username);
            metadataPermission.setGroup(group);
            metadataPermission.setUuid(metadataItem.getUuid());

            metadataItem.updatePermissions(metadataPermission);

        } catch (MetadataException e) {
            e.printStackTrace();
        }
    }

    public void checkSchemaId(String schemaId) throws MetadataQueryException, MetadataSchemaValidationException {
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
        }

        return collection;
    }

    public Bson getTenantIdQuery(){
        return eq("tenantId", this.metadataItem.getTenantId());
    }

    public Bson getReadQuery(){
        return or(
                eq("owner", this.username),
                elemMatch("permissions",
                        and(
                                eq("username", this.username),
                                nin("permissions", PermissionType.NONE.toString()))));
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

    public List<MetadataItem> find(String userQuery) throws MetadataException {
        List<MetadataItem> result = new ArrayList<>();
        try {
            //build query
            Bson permissionFilter = and(getTenantIdQuery(), getReadQuery(), parseUserQuery(userQuery));
            MongoCollection<MetadataItem> collection = metadataDao.getDefaultMetadataItemCollection();
            result = metadataDao.find(this.username, permissionFilter, offset, limit, new BasicDBObject(orderField, orderDirection));

            if (result.size() == 0) {
                //check if user has permission
                if (metadataDao.hasRead(this.username, this.metadataItem.getUuid())){
                    //nothing found matching the query
                } else {
                    throw new MetadataException ("User doesn't have permission.");
                }
            }
        } catch (MetadataQueryException | UnknownHostException | MetadataStoreException | MetadataException e) {
            throw new MetadataException("Unable to find item based on query.", e);
        } catch (IOException e) {
            throw new MetadataException("Unable to find item based on query.", e);
        }
        return result;
    }

    public void updateMetadataItem() throws MetadataException, MetadataStoreException {
        //if new metadata item, insert
        if (metadataDao.find(this.username, and(eq("uuid", this.metadataItem.getUuid()),
                eq("tenantId", metadataItem.getTenantId()))).size() == 0){
            metadataDao.insert(metadataItem);
        } else {
            //otherwise update
            metadataDao.updateMetadata(metadataItem, this.username);
        }
    }

    public void setMetadataPermission(PermissionType pem, String username) throws MetadataStoreException, MetadataException, PermissionException {
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

    public List<MetadataItem> findAll() throws UnknownHostException {
            return metadataDao.findAll();
    }

    public void clearCollection() throws UnknownHostException {
        metadataDao.clearCollection();
    }


    public BasicDBObject formatMetadataItem(MetadataItem metadataItem){
        BasicDBObject result;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:SSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        result = new BasicDBObject("uuid", metadataItem.getUuid())
                .append("schemaId", metadataItem.getSchemaId())
                .append("internalUsername", metadataItem.getInternalUsername())
                .append("associationIds", String.valueOf(metadataItem.getAssociations()))
                .append("lastUpdated", formatter.format(metadataItem.getLastUpdated()))
                .append("name", metadataItem.getName())
                .append("value", BasicDBObject.parse(String.valueOf(metadataItem.getValue())))
                .append("created", formatter.format(metadataItem.getCreated()))
                .append("owner", metadataItem.getOwner());
        return result ;
    }

    public DBObject formatMetadataItemResult(MetadataItem metadataItem) throws UUIDException {
        BasicDBObject metadataObject = formatMetadataItem(metadataItem);
        BasicDBObject hal = new BasicDBObject();
        hal.put("self",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                                metadataItem.getUuid()));
        hal.put("permissions",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                                metadataItem.getUuid() + "/pems"));
        hal.put("owner",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + metadataItem.getOwner()));

        if (metadataItem.getAssociations() != null) {
            // TODO: break this into a list of object under the associationIds attribute so
            // we dont' overwrite the objects in the event there are multiple of the same type.
            BasicDBList halAssociationIds = new BasicDBList();

            MetadataAssociationList associationList = metadataItem.getAssociations();

            for (String associatedId : associationList.getAssociatedIds().keySet()){
//            for (String associatedId : metadataItem.getAssociations()) {
                AgaveUUID agaveUUID = new AgaveUUID((String) associatedId);

                try {
                    String resourceUrl = agaveUUID.getObjectReference();
                    BasicDBObject assocResource = new BasicDBObject();
                    assocResource.put("rel", (String) associatedId);
                    assocResource.put("href", TenancyHelper.resolveURLToCurrentTenant(resourceUrl));
                    assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                    halAssociationIds.add(assocResource);
                } catch (UUIDException e) {
                    BasicDBObject assocResource = new BasicDBObject();
                    assocResource.put("rel", (String) associatedId);
                    assocResource.put("href", null);
                    if (agaveUUID != null) {
                        assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                    }
                    halAssociationIds.add(assocResource);
                }
            }

            hal.put("associationIds", halAssociationIds);
        }

        if (metadataItem.getSchemaId() != null && !StringUtils.isEmpty(metadataItem.getSchemaId())) {
            AgaveUUID agaveUUID = new AgaveUUID(metadataItem.getSchemaId());
            hal.append(agaveUUID.getResourceType().name(),
                    new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference())));

        }
        metadataObject.put("_links", hal);
        return metadataObject;

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

}

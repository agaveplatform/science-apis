/**
 *
 */
package org.iplantc.service.metadata.resources;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaCreate;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaDelete;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaList;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaSearch;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.METADATA02;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.SortSyntaxException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.AgaveResource;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.MetadataApplication;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataPermissionDao;
import org.iplantc.service.metadata.events.MetadataEventProcessor;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.managers.MetadataRequestNotificationProcessor;
import org.iplantc.service.metadata.managers.MetadataRequestPermissionProcessor;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.iplantc.service.common.util.SimpleTimer;

import javax.persistence.Basic;

/**
 * Class to handle CRUD operations on metadata entities.
 *
 * @author dooley
 */
@SuppressWarnings("deprecation")
public class MetadataCollection extends AgaveResource {
    private static final Logger log = Logger.getLogger(MetadataCollection.class);

    private String username;
    private String internalUsername;
    private String uuid;
    private String userQuery;
    private boolean includeRecordsWithImplicitPermissions = true;
    private MongoClient mongoClient;
    private DB db;
    private DBCollection collection;
    private DBCollection schemaCollection;
    private MetadataEventProcessor eventProcessor;

    //KL - update to Mongo 4.0
    private MongoCollection mongoCollection;
    private MongoCollection mongoSchemaCollection;
    private MongoDatabase mongoDB;

    /**
     * @param context
     * @param request
     * @param response
     */
    public MetadataCollection(Context context, Request request, Response response) {

        super(context, request, response);
        SimpleTimer st = null;
        if (log.isDebugEnabled()) st = SimpleTimer.start("META instrument : call MetadataCollection constructor");

        this.username = getAuthenticatedUsername();

        this.uuid = (String) request.getAttributes().get("uuid");

        this.eventProcessor = new MetadataEventProcessor();

        Form form = request.getOriginalRef().getQueryAsForm();
        if (form != null) {
            userQuery = (String) form.getFirstValue("q");

            if (!StringUtils.isEmpty(userQuery)) {
                try {
                    userQuery = URLDecoder.decode(userQuery, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    log.error("Invalid URL encoding in URL. Apparently.", e);
                    response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                    response.setEntity(new IplantErrorRepresentation("Invalid URL-encoded Character(s)."));
                }
            }

            // allow admins to de-escelate permissions for querying metadata
            // so they don't get back every record for every user.
            if (AuthorizationHelper.isTenantAdmin(this.username)) {
                // check whether they explicitly ask for unprivileged results..basically query
                // as a normal user
                if (form.getNames().contains("privileged") &&
                    !BooleanUtils.toBoolean((String) form.getFirstValue("privileged"))) {
                    this.includeRecordsWithImplicitPermissions = false;
                }
                // either they did not provide a "privileged" query parameter or it was true
                // either way, they get back all results regardless of ownership
                else {
                    this.includeRecordsWithImplicitPermissions = true;
                }
            }
            // non-admins do not inherit any implicit permissions
            else {
                this.includeRecordsWithImplicitPermissions = false;
            }
        }

        internalUsername = (String) context.getAttributes().get("internalUsername");

        getVariants().add(new Variant(MediaType.APPLICATION_JSON));

        // Set up MongoDB connection
        try {
            mongoClient = ((MetadataApplication) getApplication()).getMongoClient();
            db = mongoClient.getDB(Settings.METADATA_DB_SCHEME);
            // Gets a collection, if it does not exist creates it
            collection = db.getCollection(Settings.METADATA_DB_COLLECTION);
            schemaCollection = db.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);

            //KL
            mongoDB = mongoClient.getDatabase(Settings.METADATA_DB_SCHEME);
            mongoCollection = mongoDB.getCollection(Settings.METADATA_DB_COLLECTION);
            mongoSchemaCollection = mongoDB.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);

        } catch (Throwable e) {
            log.error("Unable to connect to metadata store", e);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store."));
        }

        log.debug(st.getShortStopMsg());
    }

    /**
     * This method represents the HTTP GET action. The input files for the authenticated user are
     * retrieved from the database and sent to the user as a {@link JSONArray JSONArray}
     * of {@link JSONObject JSONObject}.
     */
    @Override
    public Representation represent(Variant variant) throws ResourceException {
        DBCursor cursor = null;
        SimpleTimer st = null;

        //KL
        MongoCursor agg_cursor = null;

        try {
            if (collection == null) {
                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                    "Unable to connect to metadata store. If this problem persists, "
                        + "please contact the system administrators.");
            }

            BasicDBObject query = null;

            //KL - list of arguments for aggregation pipeline
            BasicDBList agg = new BasicDBList();

            if (StringUtils.isEmpty(userQuery)) {
                //AgaveLogServiceClient.log(METADATA02.name(), MetaList.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

                query = new BasicDBObject("tenantId", TenancyHelper.getCurrentTenantId());
                // filter results if querying without implicity permissions
                if (!includeRecordsWithImplicitPermissions) {
                    // permissions are separated from the metadata, so we need to look up available uuid for user.
                    if (log.isDebugEnabled())
                        st = SimpleTimer.start("META instrument : no user query defined, meta perms gathered");
                    List<String> accessibleUuids = MetadataPermissionDao.getUuidOfAllSharedMetataItemReadableByUser(this.username);

                    //KL - add permission find to query
                    //permission match
                    BasicDBObject permType = new BasicDBObject("$nin", Arrays.asList(PermissionType.NONE));
                    BasicDBObject perm = new BasicDBObject("permissions", permType);
                    BasicDBList permList = new BasicDBList();
                    permList.add(perm);
                    permList.add(new BasicDBObject("username", this.username));

                    BasicDBObject elemMatch = new BasicDBObject("permissions", new BasicDBObject("$elemMatch", permList));

                    //---------------------------------

                    if (st != null) log.debug(st.getShortStopMsg());

                    BasicDBList or = new BasicDBList();
                    //KL - add permission to find query
                    //or.add(new BasicDBObject("uuid", new BasicDBObject("$in", accessibleUuids)));
                    or.add(elemMatch);
                    or.add(new BasicDBObject("owner", this.username));

                    BasicDBList queryList = new BasicDBList();
                    queryList.add(query);
                    queryList.add(or);
                    BasicDBObject match = new BasicDBObject("$match", queryList);
                    agg.add(match);

                    //query.append("$or", or);
                    //-------------------

                }

            } else {
                AgaveLogServiceClient.log(METADATA02.name(),
                    MetaSearch.name(),
                    username,
                    "",
                    getRequest().getClientInfo().getUpstreamAddress());
                if (log.isDebugEnabled()) st = SimpleTimer.start("META instrument : user query requested");
                try {
                    query = ((BasicDBObject) JSON.parse(userQuery));
                    for (String key : query.keySet()) {
                        if (query.get(key) instanceof String) {
                            if (((String) query.get(key)).contains("*")) {
                                try {
                                    Pattern regexPattern = Pattern.compile((String) query.getString(key),
                                        Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                                    query.put(key, regexPattern);
                                } catch (Exception e) {
                                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                                        "Invalid regular expression for " + key + " query");
                                }
                            }
                        }
                    }
                    // append tenancy info
                    //query.append("tenantId", TenancyHelper.getCurrentTenantId());
                    //KL
                    BasicDBObject tenantMatch = new BasicDBObject("tenantId", TenancyHelper.getCurrentTenantId());

                    // filter results if querying without implicity permissions
                    if (!includeRecordsWithImplicitPermissions) {
                        // permissions are separated from the metadata, so we need to look up available uuid for user.
                        List<String> accessibleUuids = MetadataPermissionDao.getUuidOfAllSharedMetataItemReadableByUser(
                            this.username);

                        //KL - add permission find to query
                        BasicDBObject permType = new BasicDBObject("$nin", Arrays.asList(PermissionType.NONE));
                        BasicDBObject perm = new BasicDBObject("permissions", permType);
                        BasicDBList permList = new BasicDBList();
                        permList.add(perm);
                        permList.add(new BasicDBObject("username", this.username));

                        BasicDBObject elemMatch = new BasicDBObject("permissions", new BasicDBObject("$elemMatch", permList));

                        //---------------------------------

                        List<String> accessibleOwners = Arrays.asList(this.username,
                            Settings.PUBLIC_USER_USERNAME,
                            Settings.WORLD_USER_USERNAME);
                        BasicDBList or = new BasicDBList();

                        //KL - add permission to find query
                        //or.add(new BasicDBObject("uuid", new BasicDBObject("$in", accessibleUuids)));
                        //or.add(new BasicDBObject("$and", and));
                        //or.add(new BasicDBObject("owner", new BasicDBObject("$in", accessibleOwners)));

                        or.add(elemMatch);
                        or.add(new BasicDBObject("owner", accessibleOwners));

                        BasicDBList queryList = new BasicDBList();
                        queryList.add(query);
                        queryList.add(tenantMatch);
                        queryList.add(or);
                        BasicDBObject match = new BasicDBObject("$match", queryList);
                        agg.add(match);

                        //query.append("$or", or);
                        //----------------------------------

                    }
                } catch (JSONParseException e) {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Malformed JSON Query");
                }
                if (st != null) log.debug(st.getShortStopMsg());
            }

            List<String> sortableFields = Arrays.asList("uuid",
                "tenantId",
                "schemaId",
                "internalUsername",
                "lastUpdated",
                "name",
                "value",
                "created",
                "owner");

            String orderField = getOrderBy("lastUpdated");

            if (StringUtils.isBlank(orderField) || !sortableFields.contains(orderField)) {
                throw new SortSyntaxException("Invalid order field. Please specify one of " +
                    StringUtils.join(sortableFields, ","), new MetadataException("Invalid sort field"));
            }

            int orderDirection = getOrder(AgaveResourceResultOrdering.DESC).isAscending() ? 1 : -1;

            if (log.isDebugEnabled()) st = SimpleTimer.start("query mongodb ...  ");

            //KL - aggregate instead of find - faster results
            agg.add(Aggregates.sort(new BasicDBObject(orderField, orderDirection)));
            agg.add(Aggregates.skip(offset));
            agg.add(Aggregates.limit(limit));

            agg_cursor = mongoCollection.aggregate(agg).cursor();
            //-----------------------------------------------


            cursor = collection.find(query, new BasicDBObject("_id", false))
                               .sort(new BasicDBObject(orderField, orderDirection))
                               .skip(offset)
                               .limit(limit);
            if (st != null) log.debug(st.getShortStopMsg());

            List<DBObject> permittedResults = new ArrayList<DBObject>();
            List<DBObject> agg_permittedResults = new ArrayList<DBObject>();


            if (log.isDebugEnabled()) st = SimpleTimer.start("format the query results ...  ");

            //KL -
            while (agg_cursor.hasNext()) {
                DBObject result = cursor.next ();
                agg_permittedResults.add(result);
            }
            //---------------------------------------------

            for (DBObject result : cursor.toArray()) {
                // permission check is not needed since the list came from
                // a white list of allowsed uuids
                result = formatMetadataObject(result);
                permittedResults.add(result);
            }
            //---------------------------------------------

            if (st != null) log.debug(st.getShortStopMsg());

            return new IplantSuccessRepresentation(agg_permittedResults.toString());
        } catch (SortSyntaxException e) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage(), e);
        } catch (ResourceException e) {
            throw e;
        } catch (Throwable e) {
            throw new ResourceException(org.restlet.data.Status.SERVER_ERROR_INTERNAL,
                "An error occurred while fetching the metadata item. " +
                    "If this problem persists, " +
                    "please contact the system administrators.", e);
        } finally {
            try {
                cursor.close();
                agg_cursor.close();             //KL
            } catch (Exception e) {
            }
        }

    }

    /**
     * HTTP POST for Creating and Updating Metadata
     *
     * @param entity
     */
    @Override
    public void acceptRepresentation(Representation entity) {
        AgaveLogServiceClient.log(METADATA02.name(),
            MetaCreate.name(),
            username,
            "",
            getRequest().getClientInfo().getUpstreamAddress());

        try {
            if (collection == null) {
                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                    "Unable to connect to metadata store. " +
                        "If this problem persists, please contact the system administrators.");
            }

            String name = null;
            String value = null;
            String schemaId = null;
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode items = mapper.createArrayNode();
            ArrayNode permissions = mapper.createArrayNode();
            ArrayNode notifications = mapper.createArrayNode();

            try {
                JsonNode jsonMetadata = super.getPostedEntityAsObjectNode(false);

                if (jsonMetadata.has("name") && jsonMetadata.get("name").isTextual()
                    && !jsonMetadata.get("name").isNull()) {
                    name = jsonMetadata.get("name").asText();
                } else {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "No name attribute specified. Please associate a value with the metadata name.");
                }

                if (jsonMetadata.has("value") && !jsonMetadata.get("value").isNull()) {
                    if (jsonMetadata.get("value").isObject() || jsonMetadata.get("value").isArray())
                        value = jsonMetadata.get("value").toString();
                    else
                        value = jsonMetadata.get("value").asText();
                } else {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "No value attribute specified. Please associate a value with the metadata value.");
                }

                if (jsonMetadata.has("associationIds")) {
                    if (jsonMetadata.get("associationIds").isArray()) {
                        items = (ArrayNode) jsonMetadata.get("associationIds");
                    } else {
                        if (jsonMetadata.get("associationIds").isTextual())
                            items.add(jsonMetadata.get("associationIds").asText());
                    }
                }

                if (jsonMetadata.hasNonNull("notifications")) {
                    if (jsonMetadata.get("notifications").isArray()) {
                        notifications = (ArrayNode) jsonMetadata.get("notifications");
                    } else {
                        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                            "Invalid notifications value. notifications should be an "
                                + "JSON array of notification objects.");
                    }
                }

                if (jsonMetadata.hasNonNull("permissions")) {
                    if (jsonMetadata.get("permissions").isArray()) {
                        permissions = (ArrayNode) jsonMetadata.get("permissions");
                    } else {
                        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                            "Invalid permissions value. permissions should be an "
                                + "JSON array of permission objects.");
                    }
                }

                if (jsonMetadata.has("schemaId") && jsonMetadata.get("schemaId").isTextual()) {
                    schemaId = jsonMetadata.get("schemaId").asText();
                }
            } catch (ResourceException e) {
                throw e;
            } catch (Exception e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                    "Unable to parse form. " + e.getMessage());
            }

            // if a schema is given, validate the metadata against that registered schema
            if (schemaId != null) {
                BasicDBObject schemaQuery = new BasicDBObject("uuid", schemaId);
                schemaQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
                BasicDBObject schemaDBObj = (BasicDBObject) schemaCollection.findOne(schemaQuery);

                // lookup the schema
                if (schemaDBObj == null) {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Specified schema does not exist.");
                }

                // check user permsisions to view the schema
                try {
                    MetadataSchemaPermissionManager schemaPM = new MetadataSchemaPermissionManager(schemaId,
                        (String) schemaDBObj.get("owner"));
                    if (!schemaPM.canRead(username)) {
                        throw new MetadataException("User does not have permission to read metadata schema");
                    }
                } catch (MetadataException e) {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage());
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
                        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                            "Metadata value does not conform to schema. \n" + sb.toString());
                    }
                } catch (ResourceException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Metadata does not conform to schema.");
                }
            }

            // lookup the associated ids to make sure they exist.
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
                                BasicDBObject associationDBObj = (BasicDBObject) collection.findOne(associationQuery);

                                if (associationDBObj == null) {
                                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                                        "No metadata resource found with uuid " + associationId);
                                }
                            } else if (UUIDType.SCHEMA == associationUuid.getResourceType()) {
                                DBCollection schemataCollection = db.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);

                                BasicDBObject associationQuery = new BasicDBObject("uuid", associationId);
                                associationQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
                                BasicDBObject associationDBObj = (BasicDBObject) schemataCollection.findOne(
                                    associationQuery);

                                if (associationDBObj == null) {
                                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                                        "No metadata schema resource found with uuid " + associationId);
                                }
                            } else {
                                try {
                                    associationUuid.getObjectReference();
                                } catch (Exception e) {
                                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                                        "No associated object found with uuid " + associationId);
                                }
                            }

                            associations.add(items.get(i).asText());
                        }
                    } catch (ResourceException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                            "Unable to parse association ids.", e);
                    }
                }
            }

            BasicDBObject doc;
            String timestamp = new DateTime().toString();
            try {
                doc = new BasicDBObject("uuid", uuid)
                    .append("owner", username)
                    .append("tenantId", TenancyHelper.getCurrentTenantId())
                    .append("schemaId", schemaId)
                    .append("internalUsername", internalUsername)
                    .append("associationIds", associations)
                    .append("lastUpdated", timestamp)
                    .append("name", name)
                    .append("value", JSON.parse(value));
            } catch (JSONParseException e) {
                // If value is a String that cannot be parsed into JSON Objects, then store it as a String
                doc = new BasicDBObject("uuid", uuid)
                    .append("owner", username)
                    .append("tenantId", TenancyHelper.getCurrentTenantId())
                    .append("schemaId", schemaId)
                    .append("internalUsername", internalUsername)
                    .append("associationIds", associations)
                    .append("lastUpdated", timestamp)
                    .append("name", name)
                    .append("value", value);
            }

            // If there is no metadata for this oid, there are no permissions to check, so add metadata and make
            // the user the owner.
            uuid = new AgaveUUID(UUIDType.METADATA).toString();
            doc.put("uuid", uuid);
            doc.append("created", timestamp);

            //KL - add any permissions to the requesting user
            BasicDBList pemList = new BasicDBList();
            MetadataRequestPermissionProcessor permissionProcessor = new MetadataRequestPermissionProcessor(username,
                    uuid);
            permissionProcessor.process(permissions);
            List<MetadataPermission> metaPemList = permissionProcessor.getPermissions();

           for (MetadataPermission pem : metaPemList) {
               pemList.add(new BasicDBObject("username", pem.getUsername())
                       .append("group", null)
                       .append("permissions", new BasicDBList().add(pem.getPermission())));
           }
           mongoCollection.insertOne(doc);

           //KL -------------

            collection.insert(doc);

            eventProcessor.processContentEvent(uuid,
                MetadataEventType.CREATED,
                username,
                formatMetadataObject(doc).toString());

            // process any embedded notifications
            MetadataRequestNotificationProcessor notificationProcessor = new MetadataRequestNotificationProcessor(
                username,
                uuid);
            notificationProcessor.process(notifications);

            // add ownership permission to the requesting user
            MetadataPermissionManager pm = new MetadataPermissionManager(uuid, username);
            pm.setPermission(username, "ALL");

            //KL
            //// add any  permission to the requesting user
            //MetadataRequestPermissionProcessor permissionProcessor = new MetadataRequestPermissionProcessor(username,
            //    uuid);
            //permissionProcessor.process(permissions);


            getResponse().setStatus(Status.SUCCESS_CREATED);
            getResponse().setEntity(new IplantSuccessRepresentation(formatMetadataObject(doc).toString()));
            return;
        } catch (ResourceException e) {
            log.error("Failed to add metadata ", e);
            getResponse().setStatus(e.getStatus());
            getResponse().setEntity(new IplantErrorRepresentation(e.getMessage()));
        } catch (Throwable e) {
            log.error("Failed to add metadata ", e);
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
            getResponse().setEntity(new IplantErrorRepresentation(
                "An error occurred while fetching the metadata item. " +
                    "If this problem persists, " +
                    "please contact the system administrators."));
        } finally {
        }
    }

    /**
     * Formats each metadata item returned with Agave decorations
     * @param metadataObject
     * @return
     * @throws UUIDException
     */
    private DBObject formatMetadataObject(DBObject metadataObject) throws UUIDException {
        metadataObject.removeField("_id");
        metadataObject.removeField("tenantId");
        BasicDBObject hal = new BasicDBObject();
        hal.put("self",
            new BasicDBObject("href",
                TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" + metadataObject.get(
                    "uuid")));
        hal.put("permissions",
            new BasicDBObject("href",
                TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" + metadataObject.get(
                    "uuid") + "/pems"));
        hal.put("owner",
            new BasicDBObject("href",
                TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + metadataObject.get("owner")));

        if (metadataObject.containsField("associationIds")) {
            // TODO: break this into a list of object under the associationIds attribute so
            // we dont' overwrite the objects in the event there are multiple of the same type.
            BasicDBList halAssociationIds = new BasicDBList();

            for (Object associatedId : (BasicDBList) metadataObject.get("associationIds")) {
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

        if (metadataObject.get("schemaId") != null && !StringUtils.isEmpty(metadataObject.get("schemaId").toString())) {
            AgaveUUID agaveUUID = new AgaveUUID((String) metadataObject.get("schemaId"));
            hal.append(agaveUUID.getResourceType().name(),
                new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference())));

        }
        metadataObject.put("_links", hal);
        return metadataObject;
    }

    /* (non-Javadoc)
     * @see org.restlet.resource.Resource#allowPost()
     */
    @Override
    public boolean allowPost() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.restlet.resource.Resource#allowDelete()
     */
    @Override
    public boolean allowDelete() {
        return true;
    }

}
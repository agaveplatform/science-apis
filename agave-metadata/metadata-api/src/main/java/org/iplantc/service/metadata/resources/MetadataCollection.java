/**
 *
 */
package org.iplantc.service.metadata.resources;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaCreate;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.METADATA02;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.mongodb.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.SortSyntaxException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.AgaveResource;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.util.SimpleTimer;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.events.MetadataEventProcessor;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.managers.MetadataItemPermissionManager;
import org.iplantc.service.metadata.managers.MetadataRequestNotificationProcessor;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.enumerations.MetadataEventType;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.iplantc.service.metadata.search.JsonHandler;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.data.*;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    private MetadataEventProcessor eventProcessor;


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
//        try {
//            //Mongo connection handled in Metadata Dao
////            mongoClient = ((MetadataApplication) getApplication()).getMongoClient();
////            db = mongoClient.getDB(Settings.METADATA_DB_SCHEME);
////            // Gets a collection, if it does not exist creates it
////            collection = db.getCollection(Settings.METADATA_DB_COLLECTION);
////            schemaCollection = db.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);
////
////            //KL
////            mongoDB = mongoClient.getDatabase(Settings.METADATA_DB_SCHEME);
////            mongoCollection = mongoDB.getCollection(Settings.METADATA_DB_COLLECTION);
////            mongoSchemaCollection = mongoDB.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);
//
//        } catch (Throwable e) {
//            log.error("Unable to connect to metadata store", e);
//            response.setStatus(Status.SERVER_ERROR_INTERNAL);
//            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store."));
//        }

        log.debug(st.getShortStopMsg());
    }

    /**
     * This method represents the HTTP GET action. The input files for the authenticated user are
     * retrieved from the database and sent to the user as a {@link JSONArray JSONArray}
     * of {@link JSONObject JSONObject}.
     */
    @Override
    public Representation represent(Variant variant) throws ResourceException {
        SimpleTimer st = null;

        try {
            MetadataSearch search = new MetadataSearch(this.username);
            if (includeRecordsWithImplicitPermissions)
                search.setAccessibleOwnersImplicit();
            else
                search.setAccessibleOwnersExplicit();

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. If this problem persists, "
//                                + "please contact the system administrators.");
//            }

//            List<MetadataItem> userResults;
            List<DBObject> agg_permittedResults = new ArrayList<>();

            List<String> str_permittedResults = new ArrayList<>();

            try {

                    if (StringUtils.isNotBlank(uuid)) {
                        MetadataItemPermissionManager permissionManager = new MetadataItemPermissionManager(this.username, uuid);
                        if (!permissionManager.canRead())
                            throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN, "User does not have permission to view resource.");
                    }

//                    MetadataItemPermissionManager permissionManager = new MetadataItemPermissionManager(this.username, uuid);

//                    if (permissionManager.canRead()) {

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

                        search.setOrderField(orderField);
                        search.setOrderDirection(orderDirection);
                        search.setLimit(limit);
                        search.setOffset(offset);

                        if (hasJsonPathFilters()) {
                            List<Document> userResults = search.filterFind(userQuery, jsonPathFilters);

                            for (Document metadataDoc : userResults) {
                                str_permittedResults.add(metadataDoc.toJson());
                            }

                        } else {
                            List<MetadataItem> userResults = search.find(userQuery);

                            for (MetadataItem metadataItem : userResults) {
                                MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(metadataItem);
//                            agg_permittedResults.add(metadataItemSerializer.formatMetadataItemResult());
                                str_permittedResults.add(metadataItemSerializer.formatMetadataItemResult().toString());
                            }
//                        str_permittedResults = agg_permittedResults.toString();
                        }
//                        return new IplantSuccessRepresentation(str_permittedResults.toString());

//                    } else {
//                        throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN, "User does not have permission to view resource.");
//                    }
            } catch (MetadataQueryException e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Malformed JSON Query, " + e);
            }

//            return new IplantSuccessRepresentation(agg_permittedResults.toString());
            return new IplantSuccessRepresentation(str_permittedResults.toString());


        } catch (SortSyntaxException e) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage(), e);
        } catch (ResourceException e) {
            throw e;
        } catch (Throwable e) {
            throw new ResourceException(org.restlet.data.Status.SERVER_ERROR_INTERNAL,
                    "An error occurred while fetching the metadata item. " +
                            "If this problem persists, " +
                            "please contact the system administrators. + " + e.getMessage() , e);
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
        String strMetadataItem = "";
        try {
            MetadataSearch search = new MetadataSearch(this.username);
            if (includeRecordsWithImplicitPermissions)
                search.setAccessibleOwnersImplicit();
            else
                search.setAccessibleOwnersExplicit();

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. " +
//                                "If this problem persists, please contact the system administrators.");
//            }
            MetadataItem metadataItem;
            try {
                JsonNode jsonMetadata = super.getPostedEntityAsObjectNode(false);
                JsonHandler jsonHandler = new JsonHandler();
                jsonHandler.parseJsonMetadata(jsonMetadata);
                metadataItem = jsonHandler.getMetadataItem();
            } catch (ResourceException e) {
                throw e;
            } catch (Exception e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Unable to parse form. " + e.getMessage());
            }
            MetadataItem addedMetadataItem;

            try {
                metadataItem.setInternalUsername(internalUsername);
                search.setMetadataItem(metadataItem);
                search.setOwner(this.username);
                addedMetadataItem = search.insertMetadataItem();
                uuid = addedMetadataItem.getUuid();
            } catch (Exception e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Unable to add metadata. " + e.getMessage());
            }

            // process any embedded notifications
            MetadataRequestNotificationProcessor notificationProcessor = new MetadataRequestNotificationProcessor(
                    username,
                    uuid);
            notificationProcessor.process(search.getNotifications());

            MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(addedMetadataItem);
             strMetadataItem = metadataItemSerializer.formatMetadataItemResult().toString();

            eventProcessor.processContentEvent(uuid,
                    MetadataEventType.CREATED,
                    username,
                    strMetadataItem);

            getResponse().setStatus(Status.SUCCESS_CREATED);
            getResponse().setEntity(new IplantSuccessRepresentation(strMetadataItem));
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
                            "please contact the system administrators. added item: " + strMetadataItem + " -- " + e.getMessage()));
        }
    }

    /**
     * Formats each metadata item returned with Agave decorations
     * @param metadataObject the bson object to format
     * @return bson object with hypermedia added and links resolved
     * @throws UUIDException if the associated uuid cannot be resolved.
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
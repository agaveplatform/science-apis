/**
 *
 */
package org.iplantc.service.metadata.resources;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaDelete;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaEdit;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.MetaGetById;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.METADATA02;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.List;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.AgaveResource;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.MetadataApplication;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataPermissionDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.iplantc.service.notification.managers.NotificationManager;
import org.joda.time.DateTime;
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

/**
 * Class to handle CRUD operations on metadata entities.
 *
 * @author dooley
 *
 */
@SuppressWarnings("deprecation")
public class MetadataResource extends AgaveResource {
    private static final Logger log = Logger.getLogger(MetadataResource.class);

    private String username;
    private String internalUsername;
    private String uuid;
    private String userQuery;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection collection;
    private DBCollection schemaCollection;

    //KL - update to Mongo 4.0
    private MongoCollection mongoCollection;
    private MongoCollection mongoSchemaCollection;
    private MongoDatabase mongoDB;

    /**
     * @param context
     * @param request
     * @param response
     */
    public MetadataResource(Context context, Request request, Response response) {
        super(context, request, response);

        this.username = getAuthenticatedUsername();

        uuid = (String) request.getAttributes().get("uuid");

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
        } catch (Exception e) {
            log.error("Unable to connect to metadata store", e);
//        	try { mongoClient.close(); } catch (Exception e1) {}
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store."));
        }
    }

    /**
     * This method represents the HTTP GET action. The input files for the authenticated user are
     * retrieved from the database and sent to the user as a {@link org.json.JSONArray JSONArray}
     * of {@link org.json.JSONObject JSONObject}.
     *
     */
    @Override
    public Representation represent(Variant variant) throws ResourceException {
        DBCursor cursor = null;
        try {

//            BasicDBObject query = null;

            // Include user defined query clauses given within the URL as q=<clauses>
            AgaveLogServiceClient.log(METADATA02.name(), MetaGetById.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

            // do we not want to support general collection queries?
            // How would one browse all their metadata?
            // does that even make sense?

            MetadataSearch search = new MetadataSearch(username);
            search.setAccessibleOwnersExplicit();

            if (search.getCollection() == null) {
                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                        "Unable to connect to metadata store. If this problem persists, "
                                + "please contact the system administrators. Exception: ");
            }

            MetadataItem result = search.findOne();
            if (result != null) {
                MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(result);
                return new IplantSuccessRepresentation(metadataItemSerializer.formatMetadataItemResult().toString());
            } else {
                throw new MetadataException("No metadata item found for user with id " + uuid);
            }

        } catch (Throwable e) {
            log.error("Failed to list metadata " + uuid, e);
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                    "An unexpected error occurred while fetching the metadata item. "
                            + "If this continues, please contact your tenant administrator.", e);
        } finally {
            try {
                cursor.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * HTTP POST for Creating and Updating Metadata
     * @param entity
     */
    @Override
    public void acceptRepresentation(Representation entity) {
        AgaveLogServiceClient.log(METADATA02.name(), MetaEdit.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

        DBCursor cursor = null;

        try {

            String name = null;
            String value = null;
            String schemaId = null;
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode items = mapper.createArrayNode();
            MetadataItem metadataItem = null;

            MetadataSearch search = new MetadataSearch(this.username);
            search.setAccessibleOwnersExplicit();

            try {
                if (search.getCollection() == null) {
                    throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                            "Unable to connect to metadata store. " +
                                    "If this problem persists, please contact the system administrators.");
                }

                JsonNode jsonMetadata = super.getPostedEntityAsObjectNode(false);
                search.parseJsonMetadata(jsonMetadata);

            } catch (ResourceException e) {
                throw e;
            } catch (Exception e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Unable to parse form. " + e.getMessage());
            }

            try {
                search.setUuid(uuid);

                MetadataItem item = search.findOne();
                search.setOwner(item.getOwner());

                metadataItem = search.updateMetadataItem();

                if (metadataItem == null) {
                    throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                            "No metadata item found for user with id " + uuid);
                } else {
                    for (int i = 0; i < items.size(); i++) {
                        String aid = items.get(i).asText();
                        NotificationManager.process(aid, "METADATA_UPDATED", username);
                    }

                    NotificationManager.process(uuid, "UPDATED", username);
                    getResponse().setStatus(Status.SUCCESS_OK);
                }

                MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(metadataItem);
                getResponse().setEntity(new IplantSuccessRepresentation(metadataItemSerializer.formatMetadataItemResult().toString()));


            } catch (PermissionException e) {
                throw new ResourceException(Status.CLIENT_ERROR_UNAUTHORIZED,
                        "User does not have permission to update metadata");
            }

            return;
        } catch (ResourceException e) {
            log.error("Failed to update metadata item " + uuid + ". " + e.getMessage());

            getResponse().setStatus(e.getStatus());
            getResponse().setEntity(new IplantErrorRepresentation(e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to update metadata item " + uuid);

            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
            getResponse().setEntity(new IplantErrorRepresentation("Unable to store the metadata object. " +
                    "If this problem persists, please contact the system administrators."));
        } finally {
            try {
                cursor.close();
            } catch (Exception e1) {
            }
        }
    }

    /**
     * DELETE
     **/
    @Override
    public void removeRepresentations() {
        AgaveLogServiceClient.log(METADATA02.name(), MetaDelete.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

        DBCursor cursor = null;
        MetadataItem metadataItem = null;
        try {

            if (StringUtils.isEmpty(uuid)) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "No object identifier provided.");
            }

            MetadataSearch search = new MetadataSearch(this.username);
            search.setAccessibleOwnersExplicit();

            if (search.getCollection() == null) {
                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                        "Unable to connect to metadata store. " +
                                "If this problem persists, please contact the system administrators.");
            }

            try {
                search.setUuid(uuid);
                metadataItem = search.deleteMetadataItem();

                if (metadataItem == null) {
                    throw new Exception();
                }
                for (String aid : metadataItem.getAssociations().getAssociatedIds().keySet()) {
                    NotificationManager.process((String) aid, "METADATA_DELETED", username);
                }

                NotificationManager.process(uuid, "DELETED", username);

                getResponse().setStatus(Status.SUCCESS_OK);
                getResponse().setEntity(new IplantSuccessRepresentation());

            } catch (PermissionException e) {
                getResponse().setStatus(Status.CLIENT_ERROR_UNAUTHORIZED);
                getResponse().setEntity(new IplantErrorRepresentation(
                        "User does not have permission to update metadata"));
                return;
            }

        } catch (ResourceException e) {
            log.error("Failed to delete metadata item " + uuid + ". " + e.getMessage());

            getResponse().setStatus(e.getStatus());
            getResponse().setEntity(new IplantErrorRepresentation(e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to delete metadata " + uuid, e);

            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
            getResponse().setEntity(new IplantErrorRepresentation("Unable to delete the associated metadata. " +
                    "If this problem persists, please contact the system administrators."));
        } finally {
            try {
                cursor.close();
            } catch (Exception e) {
            }
//	       	try { mongoClient.close(); } catch (Exception e1) {}
        }
    }

    private DBObject formatMetadataObject(DBObject metadataObject) throws UUIDException {
        metadataObject.removeField("_id");
        metadataObject.removeField("tenantId");
        BasicDBObject hal = new BasicDBObject();
        hal.put("self", new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" + metadataObject.get("uuid")));
        hal.put("permissions", new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" + metadataObject.get("uuid") + "/pems"));
        hal.put("owner", new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + metadataObject.get("owner")));

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
            hal.append(agaveUUID.getResourceType().name().toLowerCase(), new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference())));
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

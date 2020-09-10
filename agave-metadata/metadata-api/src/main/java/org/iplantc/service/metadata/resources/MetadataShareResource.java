package org.iplantc.service.metadata.resources;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.clients.AgaveProfileServiceClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.AgaveResource;
import org.iplantc.service.metadata.MetadataApplication;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataPermissionDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.managers.MetadataItemPermissionManager;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.MetadataViews;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.data.*;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import javax.persistence.Basic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.eq;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.*;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.METADATA02;

/**
 * The MetadataShareResource object enables HTTP GET and POST actions on permissions.
 *
 * @author dooley
 */
@SuppressWarnings("deprecation")
public class MetadataShareResource extends AgaveResource {
    private static final Logger log = Logger.getLogger(MetadataShareResource.class);

    private String username; // authenticated user
    private String uuid;  // object id
    private String owner;
    private String sharedUsername; // user receiving permissions


    /**
     * @param context  the request context
     * @param request  the request object
     * @param response the response object
     */
    public MetadataShareResource(Context context, Request request, Response response) {
        super(context, request, response);

        this.username = getAuthenticatedUsername();

        this.uuid = (String) request.getAttributes().get("uuid");

        this.sharedUsername = (String) request.getAttributes().get("user");

        getVariants().add(new Variant(MediaType.APPLICATION_JSON));

        try {
            MetadataSearch search = new MetadataSearch(username);

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. If this problem persists, "
//                                + "please contact the system administrators.");
//            }

            if (!StringUtils.isEmpty(uuid)) {
                //uuid exists, find metadataitem

                MetadataItem result = search.findOne(new Document("uuid", uuid));

                if (result != null) {
                    owner = result.getOwner();
                } else {
                    throw new MetadataException("No metadata item found for user with id " + uuid);
                }
            } else {
                throw new MetadataException("No metadata id provided.");
            }
        } catch (MetadataException e) {
            log.error("Invalid metadata uuid", e);
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            response.setEntity(new IplantErrorRepresentation("Invalid metadata uuid"));

//        } catch (ResourceException e) {
//            log.error("Unable to connect to metadata store ", e);
//            response.setStatus(Status.SERVER_ERROR_INTERNAL);
//            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store. If this problem persists, "
//                    + "please contact the system administrators."));

        } catch (Exception e) {
            log.error("Unable to connect to metadata store", e);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store. If this problem persists, "
                    + "please contact the system administrators."));
        } catch (Throwable e) {
            log.error("Failed to retrieve Metadata permission", e);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            response.setEntity(new IplantErrorRepresentation("Failed to retrieve Metadata permission"));
        }

//        finally {
//        	
//        	try { mongoClient.close(); } catch (Throwable e) {}
//        }
    }

    /**
     * This method represents the HTTP GET action. Gets Perms on specified iod.
     */
    @Override
    public Representation represent(Variant variant) {
        AgaveLogServiceClient.log(METADATA02.name(), MetaPemsList.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

        if (!ServiceUtils.isValid(uuid)) {
            getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            return new IplantErrorRepresentation("No metadata id provided");
        }

        try {
            MetadataSearch search = new MetadataSearch(this.username);
            search.setUuid(uuid);
            search.setAccessibleOwnersExplicit();

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. If this problem persists, "
//                                + "please contact the system administrators. ");
//            }

            List<MetadataItem> permissionResult = new ArrayList<>();
            if (StringUtils.isEmpty(sharedUsername)) {
                //get all permissions
                MetadataItemPermissionManager metadataItemPermissionManager = new MetadataItemPermissionManager(username, uuid);
                permissionResult = metadataItemPermissionManager.findPermission_User(username);
//                permissionResult = search.findPermission_User(username, uuid);

                if (permissionResult == null || permissionResult.size() == 0) {
                    throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
                            "User does not have permission to view this resource");
                }

                StringBuilder jsonPems = new StringBuilder(new MetadataPermission(uuid, owner, PermissionType.ALL).toJSON());

                for (MetadataItem foundMetadataItem : permissionResult) {
                    for (MetadataPermission foundPermission : foundMetadataItem.getPermissions()) {
                        if (!StringUtils.equals(foundPermission.getUsername(), owner)) {
                            jsonPems.append(",").append(foundPermission.toJSON());
                        }
                    }
                }

                return new IplantSuccessRepresentation("[" + jsonPems + "]");
            } else {
                //get single permission
                MetadataPermission foundPermission;
                MetadataItemPermissionManager metadataItemPermissionManager = new MetadataItemPermissionManager(username, uuid);

                if (ServiceUtils.isAdmin(sharedUsername) || StringUtils.equals(owner, sharedUsername)) {
                    foundPermission = new MetadataPermission(uuid, sharedUsername, PermissionType.ALL);
//                    return new IplantSuccessRepresentation(foundPermission.toJSON());
                } else {
                    permissionResult = metadataItemPermissionManager.findPermission_User(username);
//                    permissionResult = search.findPermission_User(sharedUsername, uuid);

                    if (permissionResult == null) {
                        throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
                                "User does not have permission to view this resource");
                    } else if (permissionResult.size() == 0) {
                        throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                                "No permissions found for user " + sharedUsername);
                    } else {
                        foundPermission = permissionResult.get(0).getPermissions_User(sharedUsername);
                    }
                }
                return new IplantSuccessRepresentation(foundPermission.toJSON());
            }


        } catch (ResourceException e) {
            log.error("Unable to retrieve metadata permission", e);
            getResponse().setStatus(e.getStatus());
            return new IplantErrorRepresentation(e.getMessage());
        } catch (Exception e) {
            // Bad request
            log.error("Failed to retrieve metadata permission", e);
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
            return new IplantErrorRepresentation(e.getMessage());
        } catch (Throwable e) {
            log.error("Failed to retrieve metadata permission", e);
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
            return new IplantErrorRepresentation(e.getMessage());
        }

    }

    /**
     * Post action for adding (and overwriting) permissions on a metadata iod
     */
    @Override
    public void acceptRepresentation(Representation entity) {
        try {
            if (StringUtils.isEmpty(uuid)) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "No metadata id provided.");
            }

            String name;
            String sPermission;
            MetadataSearch search = new MetadataSearch(username);

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. " +
//                                "If this problem persists, please contact the system administrators.");
//            }

            JSONObject postPermissionData = super.getPostedEntityAsJsonObject(true);

            if (StringUtils.isEmpty(sharedUsername)) {
                AgaveLogServiceClient.log(METADATA02.name(), MetaPemsCreate.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

                if (postPermissionData.has("username")) {
                    name = postPermissionData.getString("username");
                } else {
                    // a username must be provided either in the form or the body
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                            "No username specified. Please specify a valid user to whom the permission will apply.");
                }
            } else {
                AgaveLogServiceClient.log(METADATA02.name(), MetaPemsUpdate.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

                // name in url and json, if provided, should match
                if (postPermissionData.has("username") &&
                        !StringUtils.equalsIgnoreCase(postPermissionData.getString("username"), sharedUsername)) {
                    throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                            "The username value in the POST body, " + postPermissionData.getString("username") +
                                    ", does not match the username in the URL, " + sharedUsername);
                } else {
                    name = sharedUsername;
                }
            }

            if (postPermissionData.has("permission")) {
                sPermission = postPermissionData.getString("permission");
                if (StringUtils.equalsIgnoreCase(sPermission, "none") ||
                        StringUtils.equalsIgnoreCase(sPermission, "null")) {
                    sPermission = null;
                }
            } else {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Missing permission field. Please specify a valid permission of READ, WRITE, or READ_WRITE.");
            }

            if (!ServiceUtils.isValid(name)) {
                throw new ResourceException(
                        Status.CLIENT_ERROR_BAD_REQUEST, "No user found matching " + name);
            } else {
                // validate the user they are giving permissions to exists
                AgaveProfileServiceClient authClient = new AgaveProfileServiceClient(
                        Settings.IPLANT_PROFILE_SERVICE,
                        Settings.IRODS_USERNAME,
                        Settings.IRODS_PASSWORD);

                if (authClient.getUser(name) == null) {
                    throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                            "No user found matching " + name);
                }
            }

            search.setOwner(owner);
            search.setAccessibleOwnersExplicit();
            search.setUuid(uuid);

//            if (search.getCollection() == null) {
//                throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                        "Unable to connect to metadata store. " +
//                                "If this problem persists, please contact the system administrators.");
//            }

            try {
                MetadataItemPermissionManager metadataItemPermissionManager = new MetadataItemPermissionManager(username, uuid);
//                metadataItemPermissionManager.updatePermissions(name, "", PermissionType.getIfPresent(sPermission))
//                search.updatePermissions(name, "", PermissionType.valueOf(sPermission));

                if (StringUtils.isEmpty(sPermission)) {
                    getResponse().setStatus(Status.SUCCESS_OK);
                } else {
                    getResponse().setStatus(Status.SUCCESS_CREATED);
                }

                MetadataItem updatedItem = search.findOne(eq("uuid", uuid));
                MetadataPermission updatedPermission = updatedItem.getPermissions_User(name);
                if (updatedPermission == null) {
                    updatedPermission = new MetadataPermission(uuid, name, PermissionType.NONE);
                }
                getResponse().setEntity(new IplantSuccessRepresentation(updatedPermission.toJSON()));

//            } catch (PermissionException e) {
//                throw new ResourceException(
//                        Status.CLIENT_ERROR_FORBIDDEN,
//                        e.getMessage(), e);
            } catch (IllegalArgumentException iae) {
                throw new ResourceException(
                        Status.CLIENT_ERROR_BAD_REQUEST,
                        "Invalid permission value. Valid values are: " + PermissionType.supportedValuesAsString());
            }

        } catch (ResourceException e) {
            getResponse().setEntity(
                    new IplantErrorRepresentation(e.getMessage()));
            getResponse().setStatus(e.getStatus());
        } catch (Exception e) {
            getResponse().setEntity(
                    new IplantErrorRepresentation("Failed to update metadata permissions: " + e.getMessage()));
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        }
    }

    /* (non-Javadoc)
     * @see org.restlet.resource.Resource#removeRepresentations()
     */
    @Override
    public void removeRepresentations() throws ResourceException {
        AgaveLogServiceClient.log(METADATA02.name(), MetaPemsDelete.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

        if (!ServiceUtils.isValid(uuid)) {
            getResponse().setEntity(
                    new IplantErrorRepresentation("No metadata id provided"));
            getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            return;
        }

        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();
        search.setUuid(uuid);

//        if (search.getCollection() == null) {
//            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
//                    "Unable to connect to metadata store. " +
//                            "If this problem persists, please contact the system administrators.");
//        }

        try {
            if (StringUtils.isEmpty(sharedUsername)) {
                // clear all permissions
                MetadataItem foundMetadataItem = search.findOne();
                if (foundMetadataItem == null) {
                    throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                            "No metadata item found for user with id " + uuid);
                }

                foundMetadataItem.setPermissions(new ArrayList<MetadataPermission>());
                search.setMetadataItem(foundMetadataItem);
                search.updateMetadataItem();

            } else {
                MetadataItemPermissionManager metadataItemPermissionManager = new MetadataItemPermissionManager(username, uuid);

                // clear for user
//                search.updatePermissions(sharedUsername, "", PermissionType.NONE);
            }

            getResponse().setEntity(new IplantSuccessRepresentation());

        } catch (PermissionException e) {
            throw new ResourceException(
                    Status.CLIENT_ERROR_FORBIDDEN,
                    "User does not have permission to modify this resource.");
        } catch (ResourceException e) {
            throw e;

        } catch (Throwable e) {
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                    "Failed to remove metadata permissions: " + e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.restlet.resource.Resource#allowDelete()
     */
    @Override
    public boolean allowDelete() {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.restlet.resource.Resource#allowGet()
     */
    @Override
    public boolean allowGet() {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.restlet.resource.Resource#allowPost()
     */
    @Override
    public boolean allowPost() {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.restlet.resource.Resource#allowPut()
     */
    @Override
    public boolean allowPut() {
        return false;
    }

    /**
     * Allow the resource to be modified
     *
     * @return true
     */
    public boolean setModifiable() {
        return true;
    }

    /**
     * Allow the resource to be read
     *
     * @return true
     */
    public boolean setReadable() {
        return true;
    }
}

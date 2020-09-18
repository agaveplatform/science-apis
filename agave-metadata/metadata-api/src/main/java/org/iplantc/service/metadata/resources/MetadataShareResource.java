package org.iplantc.service.metadata.resources;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.AgaveResource;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import java.util.List;

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
    }

    /**
     * Convenience method to get the metadat item for this request. Access is determined in each method.
     *
     * @return metadata item with the uuid from the path
     * @throws ResourceException if no matching uuid found
     */
    public MetadataItem getRequestedMetadataItem() throws ResourceException{
        MetadataSearch search = new MetadataSearch(this.username);
        search.setUuid(uuid);
        search.setAccessibleOwnersExplicit();

        MetadataItem metadataItem = search.findOne();
        // clear all permissions
        if (metadataItem == null) {
            throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                    "No metadata item found for user with id " + uuid);
        }

        return metadataItem;
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

            MetadataItem metadataItem = getRequestedMetadataItem();

            MetadataPermissionManager metadataItemPermissionManager = new MetadataPermissionManager(metadataItem, username);

            if (metadataItemPermissionManager.canRead(username)) {
                if (StringUtils.isBlank(sharedUsername)) {
                    //get all permissions
                    List<MetadataPermission> foundPermissions = metadataItem.getPermissions();
                    StringBuilder jsonPems = new StringBuilder(new MetadataPermission(metadataItem.getOwner(), PermissionType.ALL).toJSON(uuid));

                    if (foundPermissions.size() > 0) {
                        for (MetadataPermission foundPermission : foundPermissions) {
                            if (!StringUtils.equals(foundPermission.getUsername(), metadataItem.getOwner())) {
                                jsonPems.append(",").append(foundPermission.toJSON(uuid));
                            }
                        }
                    }

                    return new IplantSuccessRepresentation("[" + jsonPems + "]");
                } else {
                    //get single permission
                    MetadataPermission foundPermission;

                    if (ServiceUtils.isAdmin(sharedUsername) || StringUtils.equals(metadataItem.getOwner(), sharedUsername)) {
                        foundPermission = new MetadataPermission(sharedUsername, PermissionType.ALL);
                    } else {
                        foundPermission = metadataItemPermissionManager.getPermission(sharedUsername);

                        if (PermissionType.NONE == foundPermission.getPermission()) {
                            throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
                                    "No permissions found for user " + sharedUsername);
                        }
                    }

                    return new IplantSuccessRepresentation(foundPermission.toJSON(uuid));
                }
            } else {
                throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
                        "User does not have permission to view this resource.");
            }


        } catch (ResourceException e) {
            log.error("Unable to retrieve metadata permission", e);
            getResponse().setStatus(e.getStatus());
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

            MetadataItem metadataItem = getRequestedMetadataItem();
            MetadataPermissionManager metadataItemPermissionManager = new MetadataPermissionManager(metadataItem, username);

            if (!metadataItemPermissionManager.canWrite(username)) {
                throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
                        "User does not have permission to modify this resource.");
            }

            JSONObject postPermissionData = super.getPostedEntityAsJsonObject(true);

            String pemUsername;
            String sPermission;

            if (StringUtils.isEmpty(sharedUsername)) {
                AgaveLogServiceClient.log(METADATA02.name(), MetaPemsCreate.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());

                if (postPermissionData.has("username")) {
                    pemUsername = postPermissionData.getString("username");
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
                    pemUsername = sharedUsername;
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

            try {

                if (StringUtils.isEmpty(sPermission)) {
                    getResponse().setStatus(Status.SUCCESS_OK);
                } else {
                    getResponse().setStatus(Status.SUCCESS_CREATED);
                }

                metadataItemPermissionManager.setPermission(pemUsername, sPermission);

                getResponse().setEntity(new IplantSuccessRepresentation(metadataItemPermissionManager.getPermission(pemUsername).toJSON(uuid)));

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

       try {
           MetadataItem metadataItem = getRequestedMetadataItem();
           MetadataPermissionManager metadataItemPermissionManager = new MetadataPermissionManager(metadataItem, username);

            if (!metadataItemPermissionManager.canWrite(username)) {
                throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
                        "User does not have permission to modify this resource.");
            }

            if (StringUtils.isBlank(sharedUsername)) {
                metadataItemPermissionManager.clearPermissions();
            } else {
                //clear user's permission
                metadataItemPermissionManager.setPermission(sharedUsername, PermissionType.NONE.name());
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

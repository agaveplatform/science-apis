package org.iplantc.service.metadata.resources;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
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
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
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

import javax.persistence.Basic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.*;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.METADATA02;

/**
 * The MetadataShareResource object enables HTTP GET and POST actions on permissions.
 * 
 * @author dooley
 * 
 */
@SuppressWarnings("deprecation")
public class MetadataShareResource extends AgaveResource {
	private static final Logger	log	= Logger.getLogger(MetadataShareResource.class);

	private String username; // authenticated user
	private String uuid;  // object id
    private String owner;
	private String sharedUsername; // user receiving permissions
    private MongoClient mongoClient;
    private DB db;
    private DBCollection collection;

    //KL - update to Mongo 4.0
	private MongoDatabase mongoDB;
	private MongoCollection mongoCollection;

    /**
	 * @param context the request context
	 * @param request the request object
	 * @param response the response object
	 */
	public MetadataShareResource(Context context, Request request, Response response)
	{
		super(context, request, response);

		this.username = getAuthenticatedUsername();
		

		this.uuid = (String) request.getAttributes().get("uuid");

		this.sharedUsername = (String) request.getAttributes().get("user");
		
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		try
        {
        	mongoClient = ((MetadataApplication)getApplication()).getMongoClient();
        	
            db = mongoClient.getDB(Settings.METADATA_DB_SCHEME);
            // Gets a collection, if it does not exist creates it
            collection = db.getCollection(Settings.METADATA_DB_COLLECTION);

			//KL - update to Mongo 4.0
			mongoDB = mongoClient.getDatabase(Settings.METADATA_DB_SCHEME);
			mongoCollection = mongoDB.getCollection(Settings.METADATA_DB_COLLECTION);


			if (!StringUtils.isEmpty(uuid))
            {
    	        DBObject returnVal = collection.findOne(new BasicDBObject("uuid", uuid));

				//KL - update to Mongo 4.0
				BasicDBList aggList = new BasicDBList();
				aggList.add(new BasicDBObject("$match", new BasicDBObject("uuid", uuid)));

				MongoCursor cursor = mongoCollection.aggregate(aggList).cursor();
				if (cursor.hasNext()) {
					returnVal = (DBObject) cursor.next();
				}

				if (returnVal == null) {
    	            throw new MetadataException("No metadata item found for user with id " + uuid);
    	        }
    	        owner = (String)returnVal.get("owner");
            }
            else
            {
            	throw new MetadataException("No metadata id provided.");
            }
        } catch (MetadataException e) {
        	response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            response.setEntity(new IplantErrorRepresentation(e.getMessage()));
            
        } catch (Exception e) {
            log.error("Unable to connect to metadata store", e);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            response.setEntity(new IplantErrorRepresentation("Unable to connect to metadata store."));
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
	public Representation represent(Variant variant)
	{
		AgaveLogServiceClient.log(METADATA02.name(), MetaPemsList.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());
		
		if (!ServiceUtils.isValid(uuid))
		{
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return new IplantErrorRepresentation("No metadata id provided");
		}

		try
		{
			//KL - permission in metadata doc ----------

			//check mongodb connection
			if (collection == null) {
				throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
						"Unable to connect to metadata store. If this problem persists, "
								+ "please contact the system administrators.");
			}

			//check user can read
			//check user has valid permission to write to uuid
			BasicDBObject query;
			DBCursor cursor = null;
			DBObject firstResult, formattedResult;
			BasicDBList queryList = new BasicDBList();
			BasicDBList matchList = new BasicDBList();
			BasicDBList agg = new BasicDBList();
			Cursor cursor_new = null;

			matchList.add(new BasicDBObject("uuid", uuid));
			matchList.add(new BasicDBObject("tenantId", TenancyHelper.getCurrentTenantId()));

			//owner/admin permission
			//if tenantadmin or owner
			if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
					StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
				boolean worldAdmin = JWTClient.isWorldAdmin();
				boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
				if (!tenantAdmin && !worldAdmin) {
					//user permissions
					//BasicDBList and = new BasicDBList();
					BasicDBObject permType = new BasicDBObject("$nin", Arrays.asList(PermissionType.NONE));
					BasicDBObject perm = new BasicDBObject("permissions", permType);
					BasicDBList permList = new BasicDBList();
					permList.add(perm);
					permList.add(new BasicDBObject("username", this.username));

					BasicDBObject elemMatch = new BasicDBObject("permissions", new BasicDBObject("$elemMatch", permList));

					//can be owner or user
					BasicDBList or = new BasicDBList();
					or.add(new BasicDBObject("owner", this.username));
					or.add(elemMatch);

					queryList.add(new BasicDBObject("$match", or));
					queryList.add(new BasicDBObject("$match", matchList));
					agg.add(queryList);
					agg.add(Aggregates.skip(offset));
					agg.add(Aggregates.limit(limit));

					//query.append("$or", or);
				}
			}
			cursor_new = (Cursor) collection.aggregate(agg);

			List <MetadataPermission> pemList = new ArrayList<MetadataPermission>();

			while (cursor_new.hasNext()) {
				firstResult = cursor_new.next();
				PermissionType resultPem = (PermissionType) firstResult.get("permissions.permissions.0");
				String resultUser = firstResult.get("permissions.username").toString();
				MetadataPermission mp = new MetadataPermission(uuid, resultUser, resultPem);
				pemList.add(mp);
			}

			if (pemList.isEmpty()) {
				//check if user has permissions set up

				BasicDBList permList = new BasicDBList();
				permList.add(new BasicDBObject("permissions.username", this.username));

				agg = new BasicDBList();
				agg.add(new BasicDBObject("$match", matchList));
				agg.add(new BasicDBObject("$match", permList));

				agg.add(Aggregates.skip(offset));
				agg.add(Aggregates.limit(limit));

				cursor_new = (Cursor) collection.aggregate(agg);
				if (cursor_new.hasNext()){
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to view this resource");
				} else {
					throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
							"No permissions found for user " + sharedUsername);
				}
			}

			if (StringUtils.isEmpty(sharedUsername))
			{
				StringBuilder jPems = new StringBuilder(new MetadataPermission(uuid, owner, PermissionType.ALL).toJSON());
				for (MetadataPermission permission: pemList)
				{
					if (!StringUtils.equals(permission.getUsername(), owner)) {
						jPems.append(",").append(permission.toJSON());
					}
				}
				return new IplantSuccessRepresentation("[" + jPems + "]");
			}
			else
			{
				if (ServiceUtils.isAdmin(sharedUsername) || StringUtils.equals(owner, sharedUsername)) {
					MetadataPermission pem = new MetadataPermission(uuid, sharedUsername, PermissionType.ALL);
					return new IplantSuccessRepresentation(pem.toJSON());
				}
			}
			//------------------------------------------

			MetadataPermissionManager pm = new MetadataPermissionManager(uuid, owner);

			if (pm.canRead(username))
			{
				List<MetadataPermission> permissions = MetadataPermissionDao.getByUuid(uuid, offset, limit);

				if (StringUtils.isEmpty(sharedUsername))
				{
					StringBuilder jPems = new StringBuilder(new MetadataPermission(uuid, owner, PermissionType.ALL).toJSON());
					for (MetadataPermission permission: permissions)
	    			{
						if (!StringUtils.equals(permission.getUsername(), owner)) {
							jPems.append(",").append(permission.toJSON());
						}
					}
					return new IplantSuccessRepresentation("[" + jPems + "]");
				}
				else
				{
					if (ServiceUtils.isAdmin(sharedUsername) || StringUtils.equals(owner, sharedUsername))
					{
						MetadataPermission pem = new MetadataPermission(uuid, sharedUsername, PermissionType.ALL);
						return new IplantSuccessRepresentation(pem.toJSON());
					}
					else 
					{
						MetadataPermission pem = MetadataPermissionDao.getByUsernameAndUuid(sharedUsername, uuid);
						if (pem == null) 
						{
							throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
									"No permissions found for user " + sharedUsername);
						}
						else 
						{
							return new IplantSuccessRepresentation(pem.toJSON());
						}
					}
				}
			}
			else
			{
				throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have permission to view this resource");
			}

		}
		catch (ResourceException e)
		{
			getResponse().setStatus(e.getStatus());
			return new IplantErrorRepresentation(e.getMessage());
		}
		catch (Exception e)
		{
			// Bad request
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			return new IplantErrorRepresentation(e.getMessage());
		}
		
	}

	/**
	 * Post action for adding (and overwriting) permissions on a metadata iod
	 * 
	 */
	@Override
	public void acceptRepresentation(Representation entity)
	{
		try
		{
			if (StringUtils.isEmpty(uuid))
			{
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
						"No metadata id provided.");
			}

			//KL - permission in metadata doc ----------

			//check mongodb connection
			if (collection == null) {
				throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
						"Unable to connect to metadata store. If this problem persists, "
								+ "please contact the system administrators.");
			}
			//------------------------------------------

			String name;
            String sPermission;

            JSONObject postPermissionData = super.getPostedEntityAsJsonObject(true);
            
            if (StringUtils.isEmpty(sharedUsername))
            {
            	AgaveLogServiceClient.log(METADATA02.name(), MetaPemsCreate.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());
            	
                if (postPermissionData.has("username")) 
                {
                    name = postPermissionData.getString("username");
            	} 
                else
                {	
                	// a username must be provided either in the form or the body
                	throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
                			"No username specified. Please specify a valid user to whom the permission will apply."); 
                }
            }
            else
            {
            	AgaveLogServiceClient.log(METADATA02.name(), MetaPemsUpdate.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());
            	
            	// name in url and json, if provided, should match
            	if (postPermissionData.has("username") && 
            			!StringUtils.equalsIgnoreCase(postPermissionData.getString("username"), sharedUsername)) 
                {
            		throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
            				"The username value in the POST body, " + postPermissionData.getString("username") + 
                			", does not match the username in the URL, " + sharedUsername);            		
            	} 
                else
                {
                	name = sharedUsername;
                }
            }
            
            if (postPermissionData.has("permission")) 
            {
                sPermission = postPermissionData.getString("permission");
                if (StringUtils.equalsIgnoreCase(sPermission, "none") ||
                		StringUtils.equalsIgnoreCase(sPermission, "null")) {
                	sPermission = null;
                }
            } 
            else 
            {
            	throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
                        "Missing permission field. Please specify a valid permission of READ, WRITE, or READ_WRITE.");
            }
            
			if (!ServiceUtils.isValid(name)) { 
				throw new ResourceException(
					Status.CLIENT_ERROR_BAD_REQUEST, "No user found matching " + name); 
			} 
			else 
			{
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

			/*--------------------------------------------------------*/

			//KL - permission in metadata doc ------------------------

			//check connection
			if (collection == null) {
				throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
						"Unable to connect to metadata store. If this problem persists, "
								+ "please contact the system administrators.");
			}

			//check user has valid permission to write to uuid
			BasicDBObject query;
			MongoCursor cursor = null;
			DBObject firstResult, formattedResult;

			query = new BasicDBObject("uuid", uuid);
			query.append("tenantId", TenancyHelper.getCurrentTenantId());

			//user permissions
			BasicDBObject permType = new BasicDBObject("$nin", Arrays.asList(PermissionType.NONE, PermissionType.READ));
			BasicDBObject pem = new BasicDBObject("username", username)
					.append("permissions", permType);

			//owner/admin permission -- should this be done in else for performance
			//if tenantadmin or owner
			if (StringUtils.equals(Settings.PUBLIC_USER_USERNAME, username) ||
					StringUtils.equals(Settings.WORLD_USER_USERNAME, username)) {
				boolean worldAdmin = JWTClient.isWorldAdmin();
				boolean tenantAdmin = AuthorizationHelper.isTenantAdmin(TenancyHelper.getCurrentEndUser());
				if (!tenantAdmin && !worldAdmin) {
					BasicDBList or = new BasicDBList();
					or.add(new BasicDBObject("owner", username));
					or.add(new BasicDBObject("permissions", new BasicDBObject("$elemMatch", pem)));
					query.append("$or", or);
				}
			}

			BasicDBList aggList = new BasicDBList();
			aggList.add(query);
			//findAndModify handles the find and updating/inserting
			cursor = mongoCollection.aggregate(aggList).cursor();

			if (cursor.hasNext())  {
				//permission found
				firstResult = (DBObject) cursor.next();
			}
			else {
				throw new ResourceException(
						Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have permission to modify this resource.");
			}

			//pull then push to update
			//collection.update({"uuid":uuid, "tenantId":tenantid},{$pull: {"permissions" :{"username":username}}})
			BasicDBObject removeQuery = new BasicDBObject("uuid", uuid);
			removeQuery.append("tenantId", TenancyHelper.getCurrentTenantId());
			BasicDBObject remove = new BasicDBObject("$pull", new BasicDBObject("permissions", new BasicDBObject("username", username)));
			collection.update(removeQuery, remove);

			if (StringUtils.isEmpty(sPermission) || sPermission.equalsIgnoreCase("none")) {
				//remove if permissions is empty/none
				getResponse().setStatus(Status.SUCCESS_OK);
			} else{
				getResponse().setStatus(Status.SUCCESS_CREATED);

				BasicDBObject set = new BasicDBObject("permissions.permissions.0",sPermission);

				query = new BasicDBObject("uuid", uuid);
				query.append("tenantId", TenancyHelper.getCurrentTenantId());
				BasicDBObject updatePem = new BasicDBObject("username", sharedUsername)
						.append("permissions", Arrays.asList(sPermission))
						.append("group", null);

				query.append("permissions", Arrays.asList(updatePem));
				BasicDBList updateAggList = new BasicDBList();

				collection.update(updateAggList, set);
				MetadataPermission metaPem = new MetadataPermission(uuid, sharedUsername, PermissionType.valueOf(sPermission));
				getResponse().setEntity(new IplantSuccessRepresentation(metaPem.toJSON()));
			}

			/*--------------------------------------------------------*/



			MetadataPermissionManager pm = new MetadataPermissionManager(uuid, owner);

			if (pm.canWrite(username))
			{
				// if the permission is null or empty, the permission
				// will be removed
				try 
				{
					pm.setPermission(name, sPermission );
					if (StringUtils.isEmpty(sPermission)) {
						getResponse().setStatus(Status.SUCCESS_OK);
					} else {
						getResponse().setStatus(Status.SUCCESS_CREATED);
					}
					
					MetadataPermission permission = MetadataPermissionDao.getByUsernameAndUuid(name, uuid);
					if (permission == null) {
						permission = new MetadataPermission(uuid, name, PermissionType.NONE);
					}
					
					getResponse().setEntity(new IplantSuccessRepresentation(permission.toJSON()));
				} 
				catch (PermissionException e) {
					throw new ResourceException(
							Status.CLIENT_ERROR_FORBIDDEN,
							e.getMessage(), e);
				}
				catch (IllegalArgumentException iae) {
					throw new ResourceException(
							Status.CLIENT_ERROR_BAD_REQUEST,
							"Invalid permission value. Valid values are: " + PermissionType.supportedValuesAsString());
				}
			}
			else
			{
				throw new ResourceException(
						Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have permission to modify this resource.");
			}

		}
		catch (ResourceException e)
		{
			getResponse().setEntity(
					new IplantErrorRepresentation(e.getMessage()));
			getResponse().setStatus(e.getStatus());
		}
		catch (Exception e)
		{
			getResponse().setEntity(
					new IplantErrorRepresentation("Failed to update metadata permissions: " + e.getMessage()));
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.restlet.resource.Resource#removeRepresentations()
	 */
	@Override
	public void removeRepresentations() throws ResourceException
	{
		AgaveLogServiceClient.log(METADATA02.name(), MetaPemsDelete.name(), username, "", getRequest().getClientInfo().getUpstreamAddress());
		
		if (!ServiceUtils.isValid(uuid))
		{
			getResponse().setEntity(
					new IplantErrorRepresentation("No metadata id provided"));
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return;
		}
		
		try
		{
			MetadataPermissionManager pm = new MetadataPermissionManager(uuid, owner);

			if (pm.canWrite(username))
			{
				if (StringUtils.isEmpty(sharedUsername)) {
					// clear all permissions
					pm.clearPermissions();
				} else { // clear pems for user
					pm.setPermission(sharedUsername, null);
				}
				
				getResponse().setEntity(new IplantSuccessRepresentation());
			}
			else
			{
				throw new ResourceException(
						Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have permission to modify this resource.");
			}
		}
		catch (ResourceException e) {
			throw e;
		}
		catch (Throwable e)
		{
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
	public boolean allowDelete()
	{
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.restlet.resource.Resource#allowGet()
	 */
	@Override
	public boolean allowGet()
	{
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.restlet.resource.Resource#allowPost()
	 */
	@Override
	public boolean allowPost()
	{
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.restlet.resource.Resource#allowPut()
	 */
	@Override
	public boolean allowPut()
	{
		return false;
	}

	/**
	 * Allow the resource to be modified
	 * 
	 * @return true
	 */
	public boolean setModifiable()
	{
		return true;
	}

	/**
	 * Allow the resource to be read
	 * 
	 * @return true
	 */
	public boolean setReadable()
	{
		return true;
	}
}

/**
 * 
 */
package org.iplantc.service.notification.resources.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.representation.AgaveErrorRepresentation;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.notification.dao.FailedNotificationAttemptQueue;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.notification.managers.NotificationPermissionManager;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.notification.model.enumerations.NotificationEventType;
import org.iplantc.service.notification.model.enumerations.NotificationStatusType;
import org.iplantc.service.notification.resources.NotificationResource;
import org.iplantc.service.notification.util.ServiceUtils;
import org.restlet.Request;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.*;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.NOTIFICATIONS02;

/**
 * @author dooley
 *
 */
@Produces(MediaType.APPLICATION_JSON)
@Path("{uuid}")
public class NotificationResourceImpl extends AbstractNotificationResource implements NotificationResource
{
	private static final Logger log = Logger.getLogger(NotificationResourceImpl.class);
	protected NotificationDao dao = new NotificationDao();
	protected NotificationPermissionManager pm = null;
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.notification.resources.NotificationResource#addNotificationFromForm(org.iplantc.service.notification.model.Notification)
	 */
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateNotificationFromForm(
			@PathParam("uuid") String uuid, 
			@FormParam("url") String callbackUrl,
			@FormParam("event") String notificationEvent,
			@FormParam("associatedUuid") String associatedUuid,
			@FormParam("persistent") String persistent,
			@FormParam("status") String status,
			@FormParam("policy.retryStrategyType") String retryStrategyType,
			@FormParam("policy.retryLimit") Integer retryLimit,
			@FormParam("policy.retryRate") String retryRate,
			@FormParam("policy.retryDelay") String retryDelay,
			@FormParam("policy.saveOnFailure") Boolean saveOnFailure)
	{
		AgaveLogServiceClient.log(NOTIFICATIONS02.name(), 
				NotifUpdate.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getUpstreamAddress());
		
		try
		{		
			
			Notification notification = getResourceFromPathValue(uuid);
			
			pm = new NotificationPermissionManager(notification);
			if (!pm.canWrite(getAuthenticatedUsername())) 
			{
				throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have access to view this notification");
			}
			else if (notification.getStatus() == NotificationStatusType.ACTIVE)
			{
				notification.setCallbackUrl(callbackUrl);
				notification.setEvent(notificationEvent);
				notification.setAssociatedUuid(associatedUuid);
				notification.setPersistent(BooleanUtils.toBoolean(persistent));
				

				if (StringUtils.contains(notification.getAssociatedUuid(), "*") && 
		    			!ServiceUtils.isAdmin(getAuthenticatedUsername())) 
		    	{
		    		throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"Administrator permissions are required to set wildcard notifications.");
		    	} 
		    	else 
		    	{
					dao.persist(notification);
					
					NotificationManager.process(notification.getUuid(), 
							NotificationEventType.DELETED.name(), 
							getAuthenticatedUsername(), 
							notification.toJSON());
				
					return Response.ok(new AgaveSuccessRepresentation(notification.toJSON())).build();
		    	}
			}
			else
			{
				throw new ResourceException(Status.CLIENT_ERROR_PRECONDITION_FAILED,
						"Notification has already fired. You can not update past notifications.");
			}
    	} 
		catch (NotificationException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
					"Unable to save notification: " + e.getMessage(), e);
	    } 
		catch (HibernateException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
					"Unable to save notification: " + e.getMessage(), e);
	    }
		catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
					"Failed to save notification: " + e.getMessage(), e);
		}
	}

	@Override
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response updateNotification(@PathParam("uuid") String uuid, Representation input)
	{
		AgaveLogServiceClient.log(NOTIFICATIONS02.name(), 
				NotifUpdate.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getUpstreamAddress());
		
		try
		{		
			Notification notification = dao.findByUuid(uuid);
			
			pm = new NotificationPermissionManager(notification);
			if (!pm.canWrite(getAuthenticatedUsername())) 
			{
				throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have access to view this notification");
			}
			else if (notification.getStatus() == NotificationStatusType.ACTIVE ||
					(notification.getStatus() != NotificationStatusType.INACTIVE && notification.isPersistent()))
			{
				RestletFileUpload fileUpload = new RestletFileUpload(new DiskFileItemFactory());

		        // this list is always empty !!
		        List<FileItem> fileItems = fileUpload.parseRepresentation(input);
		        JsonNode jsonNotification = null;
		        for (FileItem fileItem : fileItems) {
		            if (!fileItem.isFormField()) {
		            	ObjectMapper mapper = new ObjectMapper();
		        		jsonNotification = mapper.readTree(fileItem.getInputStream());
		            	break;
		            }
		        }
		        
				if (jsonNotification == null) {
					throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
							"No notification found in upload form.");
				} 
				else if (!jsonNotification.isObject()) {
					throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
							"Notification should be a valid JSON object.");
				}
		    	else 
		    	{	
					Notification newNotification = Notification.fromJSON(jsonNotification);

					if (StringUtils.contains(newNotification.getAssociatedUuid(), "*") && 
			    			!ServiceUtils.isAdmin(getAuthenticatedUsername())) 
			    	{
			    		throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
								"Administrator permissions are required to set wildcard notifications.");
			    	} 
			    	else 
			    	{
						notification.setCallbackUrl(newNotification.getCallbackUrl());
						notification.setEvent(newNotification.getEvent());
						notification.setAssociatedUuid(newNotification.getAssociatedUuid());
//							notification.setOwner(getAuthenticatedUsername());
						notification.setStatus(newNotification.getStatus());
						notification.setPolicy(newNotification.getPolicy());
						dao.persist(notification);
					
						NotificationManager.process(notification.getUuid(), 
								NotificationEventType.DELETED.name(), 
								getAuthenticatedUsername(), 
								notification.toJSON());
	
						return Response.ok(new AgaveSuccessRepresentation(notification.toJSON())).build();
			    	}
				}
			}
			else
			{
				throw new ResourceException(Status.CLIENT_ERROR_PRECONDITION_FAILED,
						"Notification has already fired. You can not update expired notifications.");
			}
    	} 
		catch (NotificationException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Unable to save notification: " + e.getMessage(), e);
	    } 
		catch (HibernateException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Unable to save notification: " + e.getMessage(), e);
	    }
		catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
					"Failed to save notification: " + e.getMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.notification.resources.NotificationResource#addNotification(java.lang.String, byte[])
	 */
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateNotification(@PathParam("uuid") String uuid, byte[] bytes)
	{
		AgaveLogServiceClient.log(NOTIFICATIONS02.name(), 
				NotifUpdate.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getUpstreamAddress());
		
		try
		{		
			Notification notification = getResourceFromPathValue(uuid);
			
			pm = new NotificationPermissionManager(notification);
			if (!pm.canWrite(getAuthenticatedUsername())) 
			{
				throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have access to view this notification");
			}
			else if (notification.getStatus() == NotificationStatusType.ACTIVE ||
					(notification.getStatus() != NotificationStatusType.INACTIVE && notification.isPersistent()))
			{
				ObjectMapper mapper = new ObjectMapper();
        		JsonNode jsonNotification = mapper.readTree(bytes);
            	
        		Notification newNotification = null;
				if (jsonNotification.isObject()) {
					newNotification = Notification.fromJSON(jsonNotification);
				} 
				else {
					throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
							"Notification should be a valid JSON object.");
				}
				
				if (StringUtils.contains(newNotification.getAssociatedUuid(), "*") && 
		    			!ServiceUtils.isAdmin(getAuthenticatedUsername())) {
		    		throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"Administrator permissions are required to set wildcard notifications.");
		    	} 
		    	else {
//		    		newNotification.setId(notification.getId());
//		    		newNotification.setUuid(notification.getUuid());
//		    		newNotification.setCreated(notification.getCreated());
//		    		newNotification.setLastUpdated(notification.getLastUpdated());
//		    		newNotification.setOwner(notification.getOwner());
//		    		newNotification.setVisible(notification.isVisible());
		    		
		    		notification.setCallbackUrl(newNotification.getCallbackUrl());
					notification.setEvent(newNotification.getEvent());
					notification.setAssociatedUuid(newNotification.getAssociatedUuid());
					notification.setStatus(newNotification.getStatus());
					notification.setPolicy(newNotification.getPolicy());
					notification.setPersistent(newNotification.isPersistent());
//					notification = dao.merge(notification);
					dao.update(notification);
					
					
					NotificationManager.process(notification.getUuid(), 
							NotificationEventType.UPDATED.name(), 
							getAuthenticatedUsername(), 
							notification.toJSON());
				
					return Response.ok(new AgaveSuccessRepresentation(notification.toJSON())).build();
		    	}
			}
			else
			{
				return Response.status(Status.CLIENT_ERROR_PRECONDITION_FAILED.getCode())
						.entity(new AgaveErrorRepresentation(
								"Notification has already fired. You can not update past notifications."))
						.build();
			}
    	} 
		catch (NotificationException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Invalid notification description provided. " + e.getMessage(), e);
	    } 
		catch (ResourceException e) {
			throw e;
		} 
		catch (Throwable e) {
			log.error("Failed to update notification " + uuid, e);
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
					"An unexpected error occurred while attempting to update this notification. "
					+ "If this problem persists, please contact your tenant admin.", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.notification.resources.NotificationResource#getNotification(java.lang.String)
	 */
	@Override
	@GET
	public Response getNotification(@PathParam("uuid") String uuid)
	{
		try
		{
			// redirect if there is a trailing slash
			if (StringUtils.isEmpty(uuid)) {
				Reference redirectReference = Request.getCurrent().getOriginalRef().clone();
				redirectReference.setPath(StringUtils.removeEnd(redirectReference.getPath(), "/"));
				return Response.temporaryRedirect(redirectReference.toUri()).build();
			}
			else {
				AgaveLogServiceClient.log(NOTIFICATIONS02.name(), 
						NotifGetById.name(), 
						getAuthenticatedUsername(), "", 
						Request.getCurrent().getClientInfo().getUpstreamAddress());
				
				
				Notification notification = getResourceFromPathValue(uuid);

				return Response.ok(new AgaveSuccessRepresentation(notification.toJSON())).build();
			}
		}
		catch (ResourceException e) {
			throw e;
		}
		catch (Exception e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Failed to retrieve notification: " + e.getMessage(), e);
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.notification.resources.NotificationResource#deleteNotification()
	 */
	@Override
	@DELETE
	public Response deleteNotification(@PathParam("uuid") String uuid)
	{
		AgaveLogServiceClient.log(NOTIFICATIONS02.name(), 
				NotifDelete.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getUpstreamAddress());
		
		try
		{
			Notification notification = getResourceFromPathValue(uuid);
			
			pm = new NotificationPermissionManager(notification);
			if (pm.canWrite(getAuthenticatedUsername())) 
			{
				notification.setVisible(false);
				notification.setStatus(NotificationStatusType.INACTIVE);
				notification.setLastUpdated(new Date());
				dao.update(notification);
				
				try 
				{
					FailedNotificationAttemptQueue.getInstance().removeAll(uuid);
				} 
				catch (Exception e) {
					log.error("Failed to delete the failed delivery queue for notification " + uuid, e);
				}
				
				NotificationManager.process(notification.getUuid(), 
											NotificationEventType.DELETED.name(), 
											getAuthenticatedUsername(), 
											notification.toJSON());
				
				return Response.ok(new AgaveSuccessRepresentation()).build();
			} 
			else 
			{
				throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
						"User does not have access to delete this notification");
			}
		}
		catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
					"Failed to delete notification: " + e.getMessage(), e);
		}
	}

}

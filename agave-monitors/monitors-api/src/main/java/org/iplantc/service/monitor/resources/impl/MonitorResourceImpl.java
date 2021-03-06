/**
 * 
 */
package org.iplantc.service.monitor.resources.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.hibernate.HibernateException;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.representation.AgaveErrorRepresentation;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.common.restlet.resource.AbstractAgaveResource;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.events.MonitorEventProcessor;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.managers.MonitorManager;
import org.iplantc.service.monitor.managers.MonitorPermissionManager;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.enumeration.MonitorEventType;
import org.iplantc.service.monitor.resources.MonitorResource;
import org.restlet.Request;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;

import static org.iplantc.service.common.clients.AgaveLogServiceClient.ActivityKeys.*;
import static org.iplantc.service.common.clients.AgaveLogServiceClient.ServiceKeys.MONITORS02;

/**
 * @author dooley
 *
 */
@Path("{monitorUuid}")
@Produces(MediaType.APPLICATION_JSON)
public class MonitorResourceImpl extends AbstractAgaveResource implements MonitorResource
{
	private static final ObjectMapper mapper = new ObjectMapper();
	protected MonitorDao dao = new MonitorDao();
	protected MonitorManager monitorManager = new MonitorManager();
	protected MonitorPermissionManager pm = null;
	protected MonitorEventProcessor eventProcessor = new MonitorEventProcessor();
	
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.monitor.resources.MonitorResource#addMonitorFromForm(org.iplantc.service.monitor.model.Monitor)
	 */
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateMonitorFromForm(
			@PathParam("monitorUuid") String monitorUuid, 
			@FormParam("target") String systemId,
			@FormParam("frequency") String frequency,
			@FormParam("updateSystemStatus") String updateSystemStatus,
			@FormParam("internalUsername") String internalUsername,
			@FormParam("active") String active)
	{
		try
		{
			AgaveLogServiceClient.log(MONITORS02.name(),
					MonitorUpdate.name(),
					getAuthenticatedUsername(), "",
					Request.getCurrent().getClientInfo().getAddress());

			Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);

			if (monitor == null) {
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						"No monitor found with id " + monitorUuid);
			}
			else
			{
				if (! new MonitorPermissionManager(monitor).canWrite(getAuthenticatedUsername()))
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to view this monitor");
				}
				else
				{
					ObjectNode jsonMonitor = mapper.createObjectNode()
							.put("target", systemId)
							.put("frequency", NumberUtils.toInt(frequency))
							.put("updateSystemStatus", StringUtils.equalsIgnoreCase(updateSystemStatus, "true") ||
													   StringUtils.equalsIgnoreCase(updateSystemStatus, "1"))
							.put("internalUsername", internalUsername)
							.put("active", StringUtils.equalsIgnoreCase(active, "true") ||
										   StringUtils.equalsIgnoreCase(active, "1"));

					boolean wasActive = monitor.isActive();

					monitor = Monitor.fromJSON(jsonMonitor, monitor, getAuthenticatedUsername());

					dao.persist(monitor);

					if (wasActive && !monitor.isActive()) {
						eventProcessor.processContentEvent(monitor, MonitorEventType.DISABLED, getAuthenticatedUsername());
					} else if (!wasActive && monitor.isActive()) {
						eventProcessor.processContentEvent(monitor, MonitorEventType.ENABLED, getAuthenticatedUsername());
					}



					return Response.status(javax.ws.rs.core.Response.Status.OK)
							.entity(new AgaveSuccessRepresentation(monitor.toJSON()))
							.build();
				}
			}
    	} 
		catch (MonitorException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (HibernateException|IllegalArgumentException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
					"Unable to save monitor: " + e.getMessage(), e);
	    } 
		catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
					"Failed to save monitor: " + e.getMessage(), e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}
	
	@Override
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response updateMonitor(@PathParam("monitorUuid") String monitorUuid, Representation input)
	{
		try
		{	
		    AgaveLogServiceClient.log(MONITORS02.name(), 
	                MonitorUpdate.name(), 
	                getAuthenticatedUsername(), "", 
	                Request.getCurrent().getClientInfo().getAddress());
	        
			Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);
			
			if (monitor == null) {
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						"No monitor found with uuid " + monitorUuid);
			}
			else
			{
				pm = new MonitorPermissionManager(monitor);
				if (!pm.canWrite(getAuthenticatedUsername())) 
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to view this monitor");
				}
				else
				{
				    JsonNode jsonMonitor = getPostedContentAsJsonNode(input);
			        
					if (jsonMonitor == null || jsonMonitor.size() == 0) {
						throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
								"No monitor provided.");
					} 
			    	else 
			    	{	
			    		boolean wasActive = monitor.isActive();
						
						monitor = Monitor.fromJSON(jsonMonitor, monitor, getAuthenticatedUsername());
			    		
						dao.persist(monitor);
						
						if (wasActive && !monitor.isActive()) {
							eventProcessor.processContentEvent(monitor, MonitorEventType.DISABLED, getAuthenticatedUsername());
						} else if (!wasActive && monitor.isActive()) {
							eventProcessor.processContentEvent(monitor, MonitorEventType.ENABLED, getAuthenticatedUsername());
						}
						
						return Response.status(javax.ws.rs.core.Response.Status.OK)
								.entity(new AgaveSuccessRepresentation(monitor.toJSON()))
								.build();
					}
				}
			}
    	} 
		catch (MonitorException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException | HibernateException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Unable to save monitor: " + e.getMessage(), e);
	    } catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
					"Failed to save monitor: " + e.getMessage(), e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}
	
	@Override
	@PUT
	public Response store(@PathParam("monitorUuid") String monitorUuid)
	{
		try
		{	
		    Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);
			
			if (monitor == null) {
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						"No monitor found with uuid " + monitorUuid);
			}
			else
			{
				pm = new MonitorPermissionManager(monitor);
				if (!pm.canWrite(getAuthenticatedUsername())) 
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to manage this monitor");
				}
				else
				{
				    JsonNode jsonAction = getPostedContentAsJsonNode(Request.getCurrent().getEntity());
			        
					if (jsonAction == null || jsonAction.size() == 0) {
						throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
								"No action provided. Please provide a valid action "
								+ "to perform on the montior. Valid actions are: enabled, and disable");
					} 
					else if (jsonAction.hasNonNull("action") && jsonAction.get("action").isValueNode()) {
						
			    		String action = jsonAction.get("action").textValue();
						
			    		if (StringUtils.equalsIgnoreCase(action, "enable")) {
			    			if (!monitor.isActive()) {
				    			monitor.setActive(true);
				    			monitor.setLastUpdated(new Date());
				    			dao.persist(monitor);
				    			eventProcessor.processContentEvent(monitor, MonitorEventType.ENABLED, getAuthenticatedUsername());
			    			}
			    		} 
			    		else if (StringUtils.equalsIgnoreCase(action, "disable")) {
			    			if (monitor.isActive()) {
				    			monitor.setActive(false);
				    			monitor.setLastUpdated(new Date());
				    			dao.persist(monitor);
				    			eventProcessor.processContentEvent(monitor, MonitorEventType.DISABLED, getAuthenticatedUsername());
			    			}
			    		} 
			    		else {
			    			return Response.status(javax.ws.rs.core.Response.Status.BAD_REQUEST)
									.entity(new AgaveErrorRepresentation("No action provided. Please provide a valid action "
											+ "to perform on the montior. Valid actions are: enabled, and disable"))
									.build();
			    		}
						
						return Response.status(javax.ws.rs.core.Response.Status.OK)
								.entity(new AgaveSuccessRepresentation(monitor.toJSON()))
								.build();
					}
					else 
			    	{	
						return Response.status(javax.ws.rs.core.Response.Status.BAD_REQUEST)
								.entity(new AgaveErrorRepresentation("No action provided. Please provide a valid action "
										+ "to perform on the montior. Valid actions are: enabled, and disable"))
								.build();
			    	}
				}
			}
    	} 
		catch (MonitorException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException | HibernateException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Unable to save monitor: " + e.getMessage(), e);
	    } catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
					"Failed to save monitor: " + e.getMessage(), e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.monitor.resources.MonitorResource#addMonitor(java.lang.String, byte[])
	 */
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateMonitor(@PathParam("monitorUuid") String monitorUuid, byte[] bytes)
	{
		
		try
		{
			AgaveLogServiceClient.log(MONITORS02.name(),
				MonitorUpdate.name(),
				getAuthenticatedUsername(), "",
				Request.getCurrent().getClientInfo().getAddress());

			Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);

			if (monitor == null) {
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						"No monitor found with uuid " + monitorUuid);
			}
			else
			{
				pm = new MonitorPermissionManager(monitor);
				if (!pm.canWrite(getAuthenticatedUsername()))
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to view this monitor");
				}
				else
				{
					JsonNode jsonMonitor = mapper.readTree(bytes);

					boolean wasActive = monitor.isActive();

					monitor = Monitor.fromJSON(jsonMonitor, monitor, getAuthenticatedUsername());

					dao.persist(monitor);

					if (wasActive && !monitor.isActive()) {
						eventProcessor.processContentEvent(monitor, MonitorEventType.DISABLED, getAuthenticatedUsername());
					} else if (!wasActive && monitor.isActive()) {
						eventProcessor.processContentEvent(monitor, MonitorEventType.ENABLED, getAuthenticatedUsername());
					}

					return Response.status(javax.ws.rs.core.Response.Status.OK)
							.entity(new AgaveSuccessRepresentation(monitor.toJSON()))
							.build();
				}
			}
    	} 
		catch (MonitorException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					e.getMessage(), e);
		}
		catch (IllegalArgumentException | HibernateException e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Unable to save monitor: " + e.getMessage(), e);
	    } catch (ResourceException e) {
			throw e;
		} 
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
					"Failed to save monitor", e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.monitor.resources.MonitorResource#getMonitor(java.lang.String)
	 */
	@Override
	@GET
	public Response getMonitor(@PathParam("monitorUuid") String monitorUuid)
	{
		AgaveLogServiceClient.log(MONITORS02.name(), 
				MonitorGetById.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getAddress());
		
		try
		{
			Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);
			
			if (monitor == null) 
			{
				throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
						"No monitor with the given id found");
			} 
			else  
			{
				pm = new MonitorPermissionManager(monitor);
				if (pm.canRead(getAuthenticatedUsername())) 
				{
					return Response.ok(new AgaveSuccessRepresentation(monitor.toJSON())).build();
				} 
				else 
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to view this monitor");
				}
			}
		}
		catch (ResourceException e) {
			throw e;
		}
		catch (Exception e) {
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					"Failed to retrieve monitor", e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.monitor.resources.MonitorResource#deleteMonitor()
	 */
	@Override
	@DELETE
	public Response deleteMonitor(@PathParam("monitorUuid") String monitorUuid)
	{
		AgaveLogServiceClient.log(MONITORS02.name(), 
				MonitorDelete.name(), 
				getAuthenticatedUsername(), "", 
				Request.getCurrent().getClientInfo().getAddress());
		
		try
		{
			Monitor monitor = dao.findByUuidWithinSessionTenant(monitorUuid);
			
			if (monitor == null) 
			{
				throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
						"No monitor with the given id found");
			} 
			else
			{
				pm = new MonitorPermissionManager(monitor);
				if (pm.canWrite(getAuthenticatedUsername())) 
				{
					monitorManager.delete(monitor, getAuthenticatedUsername());

					return Response.ok(new AgaveSuccessRepresentation()).build();
				} 
				else 
				{
					throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
							"User does not have permission to delete this monitor");
				}
			}
		}
		catch (ResourceException e) {
			throw e;
		}
		catch (Exception e) {
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL, "Failed to delete monitor", e);
		}
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}

}

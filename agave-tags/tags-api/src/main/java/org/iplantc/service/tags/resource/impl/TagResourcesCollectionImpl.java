/**
 * 
 */
package org.iplantc.service.tags.resource.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.tags.exceptions.TagException;
import org.iplantc.service.tags.exceptions.TagValidationException;
import org.iplantc.service.tags.managers.TaggedResourceManager;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.TaggedResource;
import org.iplantc.service.tags.resource.TagResourcesCollection;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * @author dooley
 *
 */
@Path("{entityId}/associations")
public class TagResourcesCollectionImpl extends AbstractTagCollection implements TagResourcesCollection {
    
	private static final Logger log = Logger.getLogger(TagResourcesCollectionImpl.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	
    public TagResourcesCollectionImpl() {}
    
    /* (non-Javadoc)
     * @see org.iplantc.service.tags.resource.TagResourcesCollection#getTagAssociationIds(java.lang.String)
     */
    @Override
	public Response getTagAssociationIds(@PathParam("entityId") String entityId) throws Exception {
		
		logUsage(AgaveLogServiceClient.ActivityKeys.TagResourcesList);
        
		try
        {
        	Tag tag = getResourceFromPathValue(entityId);

    		String json = mapper.writerWithType(new TypeReference<List<TaggedResource>>() {})
					.writeValueAsString(tag.getTaggedResources());
        	
    		return Response.ok(new AgaveSuccessRepresentation(json)).build();
            
        }
        catch (ResourceException e) {
            throw e;
        }
        catch (Throwable e) {
        	log.error("Failed to retrieve tag " + entityId, e);
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
            		"An unexpected error occurred while fetching associatinoIds for tag  " + entityId + ". "
            				+ "If this continues, please contact your tenant administrator.", e);
        }
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
		
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.tags.resource.TagResourcesCollection#clearTagAssociationIds(java.lang.String)
	 */
	@Override
	public Response clearTagAssociationIds(@PathParam("entityId") String entityId) throws Exception {
		
		logUsage(AgaveLogServiceClient.ActivityKeys.TagResourceDelete);
        
        try
        {
        	Tag tag = getResourceFromPathValue(entityId);
        	
        	TaggedResourceManager manager = new TaggedResourceManager(tag);
        	manager.clearAllFromTag(getAuthenticatedUsername());
        	
        	return Response.ok().entity(new AgaveSuccessRepresentation("[]")).build();
        }
        catch (TagException e) {
        	log.error(e);
        	throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
                    "Failed to add tag. If this problem persists, please contact your administrator.");
        }
        catch (TagValidationException e) {
        	throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage());
        }
        catch (Exception e) {
        	log.error("Failed to delete tag " + entityId, e);
        	throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
        			"An unexpected error occurred while removing associationIds for tag  " + entityId + ". "
            				+ "If this continues, please contact your tenant administrator.", e);
        }
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.tags.resource.TagResourcesCollection#addTagAssociationIds(java.lang.String, org.restlet.representation.Representation)
	 */
	@Override
	public Response addTagAssociationIds(@PathParam("entityId") String entityId, Representation input) {
		
		logUsage(AgaveLogServiceClient.ActivityKeys.TagResourceUpdate);
        
		try
        {
        	Tag tag = getResourceFromPathValue(entityId);
        	JsonNode json = getPostedContentAsJsonNode(input);  	
        	
        	TaggedResourceManager manager = new TaggedResourceManager(tag);
        	List<TaggedResource> updatedTags = manager.addAllToTag(json, getAuthenticatedUsername());

        	return Response.ok(new AgaveSuccessRepresentation(mapper.writeValueAsString(updatedTags))).build();
            
        }
        catch (ResourceException e) {
            throw e;
        }
        catch (Throwable e) {
        	log.error("Failed to retrieve tag " + entityId, e);
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
            		"An unexpected error occurred while adding associatinoIds for tag  " + entityId + ". "
            				+ "If this continues, please contact your tenant administrator.", e);
        }
		finally {
			try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
		}
	}
}

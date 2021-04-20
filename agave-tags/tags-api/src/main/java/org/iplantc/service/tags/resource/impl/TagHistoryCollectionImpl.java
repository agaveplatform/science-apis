/**
 * 
 */
package org.iplantc.service.tags.resource.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Logger;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.tags.dao.TagEventDao;
import org.iplantc.service.tags.model.Tag;
import org.iplantc.service.tags.model.TagEvent;
import org.iplantc.service.tags.resource.TagHistoryCollection;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Returns a collection of history records for a {@link Tag}. Search
 * and pagination are support. All actions are readonly.
 * @author dooley
 *
 */
@Path("{entityId}/history")
public class TagHistoryCollectionImpl extends AbstractTagCollection implements TagHistoryCollection {
    
    private static final Logger log = Logger.getLogger(TagHistoryCollectionImpl.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response getTagEvents(@PathParam("entityId") String entityId) {
        logUsage(AgaveLogServiceClient.ActivityKeys.AppsHistoryList);
        
        try 
        {
            Tag tag = getResourceFromPathValue(entityId);
            
            List<TagEvent> events = new TagEventDao().getByTagUuid(tag.getUuid(), getLimit(), getOffset());

            ArrayNode history = mapper.createArrayNode();
            for(TagEvent event: events) {
            	JsonNode json = mapper.readTree(event.toJSON());
                history.add(json);
            }
            
            return Response.ok(new AgaveSuccessRepresentation(history.toString())).build();
        }
        catch (ResourceException e) {
            throw e;
        }
        catch (Throwable e) {
        	log.error("Failed to retrieve history for tag " + entityId, e);
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
            		"An unexpected error occurred while fetching history for tag  " + entityId + ". "
                			+ "If this continues, please contact your tenant administrator.", e);
        }
        finally {
            try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
        }
    }
}

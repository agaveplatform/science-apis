/**
 * 
 */
package org.iplantc.service.monitor.resources.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Logger;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.model.AgaveEntityEvent;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;

import javax.ws.rs.core.Response;
import java.util.List;


/**
 * @author dooley
 *
 */
public abstract class AbstractEntityEventCollection<T extends AgaveEntityEvent,V extends Enum<?>> extends AbstractEntityEventResource<T,V> {

	private static final Logger log = Logger.getLogger(AbstractEntityEventCollection.class);
    static final ObjectMapper mapper = new ObjectMapper();

    public AbstractEntityEventCollection() {}
    
    public Response getEntityEvents(String entityId) {
        
    	logUsage(AgaveLogServiceClient.ActivityKeys.MonitorHistoryList);
        
        try 
        {
            getEntityFromPathValue(entityId);
            
            List<T> events = getEntityEventDao().getEntityEventByEntityUuid(entityId, getLimit(), getOffset());
            
            ArrayNode history = mapper.createArrayNode();
            for(T event: events) {
                history.add(mapper.valueToTree(event));
            }
            
            return Response.ok(new AgaveSuccessRepresentation(history.toString())).build();
        }
        catch (ResourceException e) {
            throw e;
        } catch (Exception e) {
        	log.error("Failed to query history for resource " + entityId, e);
            throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
            		"Failed to retrieve resource history. If this persists, "
            				+ "please contact your system administrator", e);
        } finally {
            try { HibernateUtil.closeSession(); } catch (Throwable ignored) {}
        }
    }
    
//    /**
//     * Parses url query looking for a search string
//     * @return
//     */
//    protected Map<SearchTerm, Object> getQueryParameters() 
//    {
//        Request currentRequest = Request.getCurrent();
//        Reference ref = currentRequest.getOriginalRef();
//        Form form = ref.getQueryAsForm();
//        if (form != null && !form.isEmpty()) {
//            return new MonitorFilter().filterCriteria(form.getValuesMap());
//        } else {
//            return new HashMap<SearchTerm, Object>();
//        }
//    }

}

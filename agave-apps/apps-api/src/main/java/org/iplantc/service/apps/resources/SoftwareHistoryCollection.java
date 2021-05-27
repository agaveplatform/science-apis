/**
 * 
 */
package org.iplantc.service.apps.resources;

import org.iplantc.service.apps.model.SoftwareEvent;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

/**
 * Returns a collection of history records for a {@link SoftwareEvent}. Search
 * and pagination are support. All actions are readonly.
 * @author dooley
 *
 */
@Path("{softwareId}/history")
public interface SoftwareHistoryCollection {
    
    @GET
    Response getSofwareEvents(@PathParam("softwareId") String softwareId);
    
}

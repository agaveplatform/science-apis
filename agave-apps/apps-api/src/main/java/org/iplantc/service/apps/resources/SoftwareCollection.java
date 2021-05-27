/**
 * 
 */
package org.iplantc.service.apps.resources;

import org.iplantc.service.apps.model.Software;
import org.restlet.representation.Representation;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * Returns a collection of {@link Software} records for the authenticated user. Search
 * and pagination are support. Accepts new Software registrations.
 * @author dooley
 *
 */
@Path("")
public interface SoftwareCollection {
    
	@GET
    Response getSoftwareCollection();
	
	@POST
    Response addSoftware(Representation input);
}

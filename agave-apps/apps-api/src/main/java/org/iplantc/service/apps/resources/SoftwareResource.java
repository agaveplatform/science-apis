/**
 * 
 */
package org.iplantc.service.apps.resources;

import org.iplantc.service.apps.model.Software;
import org.restlet.representation.Representation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Supports management, publishing, etc of a {@link Software} records.
 * @author dooley
 *
 */
@Path("/{softwareId}")
@Produces("application/json")
public interface SoftwareResource {
    
    @GET
    Response getSoftware(@PathParam("softwareId") String softwareId);
    
    @POST
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
    Response update(@PathParam("softwareId") String softwareId,
                    Representation input);
	
	@PUT
	@Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED})
    Response manage(@PathParam("softwareId") String softwareId);
    
	@DELETE
    Response remove(@PathParam("uuid") String uuid);
}

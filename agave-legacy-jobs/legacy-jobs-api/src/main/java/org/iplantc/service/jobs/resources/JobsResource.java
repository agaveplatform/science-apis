/**
 *
 */
package org.iplantc.service.jobs.resources;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.common.resource.SearchableAgaveResource;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.dto.JobDTO;
import org.iplantc.service.jobs.search.JobSearchFilter;
import org.joda.time.DateTime;
import org.json.JSONStringer;
import org.json.JSONWriter;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * The JobResource object enables HTTP GET and POST actions on contrast jobs.
 * This resource is primarily used for submitting jobs and viewing a sample HTML
 * job submission form.
 *
 * @author dooley
 *
 */
public class JobsResource extends SearchableAgaveResource<JobSearchFilter> {
	private static final Logger	log	= Logger.getLogger(JobsResource.class);

	private String internalUsername;

//	private List<String> jobAttributes = new ArrayList<String>();
//
//	static {
//		for(Field field : Job.class.getFields()) {
//			jobAttributes.add(field.getName());
//		}
//	}
//
	/**
	 * @param context
	 * @param request
	 * @param response
	 */
	public JobsResource(Context context, Request request, Response response)
	{
		super(context, request, response);

		internalUsername = (String) context.getAttributes().get("internalUsername");

		getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

	/**
	 * This method represents the HTTP GET action. A list of jobs is retrieved
	 * from the service database and serialized to a {@link org.json.JSONArray
	 * JSONArray} of {@link org.json.JSONObject JSONObject}. On error, a HTTP
	 * {@link org.restlet.data.Status#SERVER_ERROR_INTERNAL 500} code is sent.
	 */
	@Override
	public Representation represent(Variant variant) throws ResourceException
	{
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.JOBS02.name(),
				AgaveLogServiceClient.ActivityKeys.JobsList.name(),
				getAuthenticatedUsername(), "", getRequest().getClientInfo().getUpstreamAddress());

		try
		{
			Map<SearchTerm, Object> queryParameters = getQueryParameters();

			if (queryParameters.isEmpty()) {
				List<Job> jobs = JobDao.getByUsername(getAuthenticatedUsername(), offset, limit, getSortOrder(AgaveResourceResultOrdering.DESC), getSortOrderSearchTerm());
				if (hasJsonPathFilters()) {
					ObjectMapper mapper = new ObjectMapper();
					ArrayNode json = mapper.createArrayNode();
				
					for(Job job: jobs)
					{
						json.add(mapper.readTree(job.toJSON()));
					}
					
					return new IplantSuccessRepresentation(json.toString());
				}
				else {
					JSONWriter writer = new JSONStringer();
					writer.array();
		
					for(Job job: jobs)
					{
		//				Job job = jobs.get(i);
						writer.object()
							.key("id").value(job.getUuid())
							.key("name").value(job.getName())
							.key("owner").value(job.getOwner())
							.key("systemId").value(job.getSystem())
							.key("appId").value(job.getAppId())
							.key("created").value(new DateTime(job.getCreated()).toString())
							.key("status").value(job.getStatus())
							.key("remoteStarted").value(job.getRemoteStarted() == null ? null : new DateTime(job.getRemoteStarted()).toString())
							.key("ended").value(job.getEnded() == null ? null : new DateTime(job.getEnded()).toString())
							.key("_links").object()
					        	.key("self").object()
					        		.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE) + job.getUuid())
						        .endObject()
						        .key("archiveData").object()
				        			.key("href").value(TenancyHelper.resolveURLToCurrentTenant(job.getArchiveUrl()))
						        .endObject()
					       .endObject()
				        .endObject();
					}
		
					writer.endArray();
		
					return new IplantSuccessRepresentation(writer.toString());
				}
			} 
			else {
				List<JobDTO> jobs = JobDao.findMatching(getAuthenticatedUsername(), queryParameters, offset, limit, getSortOrder(AgaveResourceResultOrdering.DESC), getSortOrderSearchTerm());
				ObjectMapper mapper = new ObjectMapper();
				if (hasJsonPathFilters()) {
//					return new IplantSuccessRepresentation(mapper.valueToTree(jobs));
					return new IplantSuccessRepresentation(mapper.writeValueAsString(jobs));
				}
				else {
					JSONWriter writer = new JSONStringer();
					writer.array();
					
					for(JobDTO job: jobs)
					{
		//				Job job = jobs.get(i);
						writer.object()
							.key("id").value(job.getUuid())
							.key("name").value(job.getName())
							.key("owner").value(job.getOwner())
							.key("systemId").value(job.getExecution_system())
							.key("appId").value(job.getSoftware_name())
							.key("created").value(new DateTime(job.getCreated()).toString())
							.key("status").value(job.getStatus())
							.key("remoteStarted").value(job.getRemote_started() == null ? null : new DateTime(job.getRemote_started()).toString())
							.key("ended").value(job.getEnded() == null ? null : new DateTime(job.getEnded()).toString())
							.key("_links").object()
					        	.key("self").object()
					        		.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE) + job.getUuid())
						        .endObject()
						        .key("archiveData").object()
				        			.key("href").value(TenancyHelper.resolveURLToCurrentTenant(job.getArchiveUrl()))
						        .endObject()
					       .endObject()
				        .endObject();
					}
		
					writer.endArray();
					return new IplantSuccessRepresentation(writer.toString());
				}
			}	
		}
		catch (HibernateException e) {
				log.error("Failed to fetch job listings from db.", e);
				getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
						return new IplantErrorRepresentation("Unable to fetch job records.");
		}
		catch (Exception e)
		{
			log.error("Failed to fetch job listings from db.", e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			return new IplantErrorRepresentation(e.getMessage());
		}
	}

	
	@Override
	public boolean allowDelete()
	{
		return false;
	}

	@Override
	public boolean allowGet()
	{
		return true;
	}

	@Override
	public boolean allowPost()
	{
		return false;
	}

	@Override
	public boolean allowPut()
	{
		return false;
	}

	@Override
	public JobSearchFilter getAgaveResourceSearchFilter() {
		return new JobSearchFilter();
	}
}

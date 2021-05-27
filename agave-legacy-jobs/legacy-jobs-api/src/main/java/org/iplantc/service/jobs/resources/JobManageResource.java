/**
 * 
 */
package org.iplantc.service.jobs.resources;

import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.representation.IplantErrorRepresentation;
import org.iplantc.service.common.representation.IplantSuccessRepresentation;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobDependencyException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.exceptions.JobTerminationException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.JobPermissionManager;
import org.iplantc.service.jobs.model.Job;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Parameter;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

/**
 * The JobManageResource is the job management interface for users. Through the
 * actions bound to this class, users can obtain individual job
 * description(GET) and kill jobs (DELETE).
 * 
 * @author dooley
 * 
 */
public class JobManageResource extends AbstractJobResource {
	private static final Logger	log	= Logger.getLogger(JobManageResource.class);

	private String				sJobId;
	private String				internalUsername;
	private Job					job;

	/**
	 * @param context
	 * @param request
	 * @param response
	 */
	public JobManageResource(Context context, Request request, Response response)
	{
		super(context, request, response);

		this.username = getAuthenticatedUsername();
		
		this.sJobId = (String) request.getAttributes().get("jobid");
		
		this.internalUsername = (String) context.getAttributes().get("internalUsername");
		
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}
	
	

	/* (non-Javadoc)
	 * @see org.restlet.resource.Resource#acceptRepresentation(org.restlet.resource.Representation)
	 */
	

	/**
	 * This method represents the HTTP GET action. Using the job id from the
	 * URL, the job information is retrieved from the databse and sent to the
	 * user as a {@link org.json.JSONObject JSONObject}. If the job id is
	 * invalid for any reason, a HTTP
	 * {@link org.restlet.data.Status#CLIENT_ERROR_BAD_REQUEST 400} code is
	 * sent. If an internal error occurs due to connectivity issues, etc, a
	 * {@link org.restlet.data.Status#SERVER_ERROR_INTERNAL 500} code is sent.
	 */
	@Override
	public Representation represent(Variant variant) throws ResourceException
	{

		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.JOBS02.name(), 
				AgaveLogServiceClient.ActivityKeys.JobsGetByID.name(), 
				username, "", getRequest().getClientInfo().getUpstreamAddress());
		
		if (!ServiceUtils.isValid(sJobId))
		{
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return new IplantErrorRepresentation("Job id cannot be empty");
		}

		try
		{
			job = JobDao.getByUuid(sJobId);
			if (job == null) 
			{
	            String msg = "No job found with job id " + sJobId + "."; 
	            log.error(msg);
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
				return new IplantErrorRepresentation(msg);
			}
			else if (!job.isVisible()) {
                String msg = "Job with uuid " + sJobId + " is not visible.";
                log.error(msg);
                getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                return new IplantErrorRepresentation(msg);
			}
			else if (new JobPermissionManager(job, username).canRead(username))
			{
				return new IplantSuccessRepresentation(job.toJSON());
			}
			else
			{
				getResponse().setStatus(Status.CLIENT_ERROR_FORBIDDEN);
				return new IplantErrorRepresentation(
						"User does not have permission to view this job");
			}
		}
		catch (JobException e)
		{
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			return new IplantErrorRepresentation("Invalid job id "
					+ e.getMessage());
		}
		catch (Exception e)
		{
			// can't set a stopped job back to running. Bad request
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			return new IplantErrorRepresentation(e.getMessage());
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
		return false;
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
		return false;
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

	@SuppressWarnings("unused")
	private Hashtable<String, String> parseForm(Form form)
	{
		Hashtable<String, String> table = new Hashtable<String, String>();

		for (Parameter p : form)
		{
			// boolean foundKey = false;
			String key = "";
			String[] lines = p.getValue().split("\\n");
			for (String line : lines)
			{
				if (line.indexOf(",") == 0)
				{
					line = line.substring(2);
				}
				line = line.replaceAll("\\r", "");
				if (line.startsWith("--") || line.equals(""))
				{
					continue;
				}
				else
				{
					if (line.startsWith("\""))
					{
						key = line.replaceAll("\"", "");
					}
					else if (line.startsWith("Content-Disposition"))
					{
						key = line.substring(line.indexOf("=") + 1);
						key = key.replaceAll("\"", "");
						// foundKey = true;
					}
					else
					{
						table.put(key, line);
					}
				}
			}
		}
		return table;
	}

}

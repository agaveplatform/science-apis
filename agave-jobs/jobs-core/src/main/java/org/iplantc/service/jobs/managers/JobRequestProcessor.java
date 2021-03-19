package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.exceptions.SoftwareException;
import org.iplantc.service.apps.managers.ApplicationManager;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.exceptions.NoMatchingBatchQueueException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobArchivePathMacroType;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Handles processing and validation of job requests.
 * Generally speaking, this will delegate most of the work to the other *QueueProcessor
 * classes which have field-specific behavior.
 * 
 * @see JobRequestInputProcessor
 * @see JobRequestParameterProcessor
 * @see JobRequestNotificationProcessor
 * @see JobRequestQueueProcessor
 *
 * @author dooley
 *
 */
public class JobRequestProcessor {
    
    private static final Logger log = Logger.getLogger(JobRequestProcessor.class);

	protected String username;
	protected String internalUsername;
	protected JobRequestQueueProcessor queueProcessor;
	protected JobRequestInputProcessor inputProcessor;
	protected JobRequestParameterProcessor parameterProcessor;
	protected JobRequestNotificationProcessor notificationProcessor;
	protected ExecutionSystem executionSystem;
	protected Software software;
	
	public JobRequestProcessor() {}
		
	/**
	 * 
	 */
	public JobRequestProcessor(String jobRequestOwner, String internalUsername) {
		this.username = jobRequestOwner;
		this.internalUsername = internalUsername;
	}
	
	/**
	 * Takes a JsonNode representing a job request and parses it into a job object.
	 *
	 * @param json a JsonNode containing the job request
	 * @return validated job object ready for submission
	 * @throws JobProcessingException of the job request cannot be validated
	 */
	public Job processJob(JsonNode json)
	throws JobProcessingException
	{
	    HashMap<String, Object> jobRequestMap = new HashMap<String, Object>();

		String currentKey = null;

		try
		{
		    try {
				Iterator<String> fields = json.fieldNames();
				while(fields.hasNext()) {
		            String key = fields.next();

				    if (StringUtils.isEmpty(key)) continue;

				    currentKey = key;

				    if (key.equals("notifications")) {
					    continue;
				    }
				
				    if (key.equals("callbackUrl")) {
					    continue;
				    }

				    if (key.equals("dependencies"))
				    {
				        String msg = "Job dependencies are not yet supported.";
				        log.error(msg);
					    throw new JobProcessingException(400, msg);
				    }

				    JsonNode child = json.get(key);

				    if (child.isNull()) {
				        jobRequestMap.put(key, null);
				    }
				    else if (child.isNumber())
				    {
				        jobRequestMap.put(key, child.asText());
				    }
				    else if (child.isObject())
				    {
					    Iterator<String> childFields = child.fieldNames();
					    while(childFields.hasNext())
					    {
						    String childKey = childFields.next();
						    JsonNode childchild = child.path(childKey);
						    if (StringUtils.isEmpty(childKey) || childchild.isNull() || childchild.isMissingNode()) {
							    continue;
						    }
						    else if (childchild.isDouble()) {
						        jobRequestMap.put(childKey, childchild.decimalValue().toPlainString());
						    }
						    else if (childchild.isNumber())
						    {
						        jobRequestMap.put(childKey, childchild.longValue());
						    }
						    else if (childchild.isArray()) {
						        List<String> arrayValues = new ArrayList<String>();
								for (JsonNode argValue : childchild) {
									if (argValue.isNull() || argValue.isMissingNode()) {
										continue;
									} else {
										arrayValues.add(argValue.asText());
									}
								}
							    jobRequestMap.put(childKey, StringUtils.join(arrayValues, Settings.AGAVE_SERIALIZED_LIST_DELIMITER));
						    }
						    else if (childchild.isTextual()) {
						        jobRequestMap.put(childKey, childchild.textValue());
						    }
						    else if (childchild.isBoolean()) {
						        jobRequestMap.put(childKey, childchild.asBoolean() ? "true" : "false");
						    }
					    }
				    }
				    else
				    {
				        jobRequestMap.put(key, json.get(key).asText());
				    }
		        }
		    }
		    catch (Exception e) {
                String msg = "Assignment for json key " + currentKey + " failed: " +
                             e.getMessage();
                log.error(msg);
                throw e;
		    }

   			Job job = null;
   			try {
   				job = processJob(jobRequestMap);
   			} catch (Exception e) {
				String msg = "Job processing failed: " + e.getMessage();
				log.error(msg);
				throw e;
			}
   			
   			setNotificationProcessor(new JobRequestNotificationProcessor(getUsername(), job));
   			
			if (json.has("notifications")) {
				try {
					getNotificationProcessor().process(json.get("notifications"));
				} catch (Exception e) {
					String input = "";
					try { input = json.get("notifications").toString(); } catch (Exception ignored) {}
					String msg = "General notification processing failed with input [" +
								 input + "]\n" + e.getMessage();
					log.error(msg);
					throw e;
				}
			}
			else if (json.has("callbackUrl")) {
				try {
					getNotificationProcessor().process(json.get("callbackUrl"));
				} catch (Exception e) {
					String input = "";
					try {input = json.get("callbackUrl").toString();} catch (Exception ignored) {}
					String msg = "Callback notification processing failed with input [" +
								 input + "]\n" + e.getMessage();
					log.error(msg);
					throw e;
				}
			}
			
			for (Notification notification: getNotificationProcessor().getNotifications()) {
				try {
					job.addNotification(notification);
				} catch (Exception e) {
					String msg = "Add notification to job failure: " + e.getMessage();
					log.error(msg);
					throw e;
				}
			}

			// If the job request had notification configured for job creation
			// they could not have fired yet. Here we explicitly add them.
			for (JobEvent jobEvent: job.getEvents()) {
			    try {
			        JobEventProcessor eventProcessor = new JobEventProcessor(jobEvent);
			        eventProcessor.process();
			    } catch (Exception e) {
					String msg = "Failure to process job event " + jobEvent.getUuid() +
								 " for tenant " + jobEvent.getTenantId() + ": " + e.getMessage();
					log.error(msg);
					throw e;
				}
			}

			return job;
		}
		catch (NotificationException e) {
//		    log.error(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
			throw new JobProcessingException(500, e);
		}
		catch (JobProcessingException e) {
//		    log.error(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
			throw e;
		}
		catch (SoftwareException e) {
//		    log.error(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
			throw new JobProcessingException(400, e);
		}
		catch (Throwable e) {
//		    log.error(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
			throw new JobProcessingException(400,
					"Job processing failed with exception type " +
					e.getClass().getSimpleName() + ": " + e.getMessage(), e);
		}
	}
	
	/**
	 * Takes a JsonNode representing a job request and parses it into a job object.
	 *
	 * @param json a JsonNode containing the job request
	 * @return validated job object ready for submission
	 * @throws JobProcessingException of the resubmitted job cannot be validated
	 */
	public Job processResubmissionJob(JsonNode json)
	throws JobProcessingException
	{
	    return processJob(json);
	}

	/**
	 * Takes a Form representing a job request and parses it into a job object. This is a
	 * stripped down, unstructured version of the other processJob method.
	 *
	 * @param jobRequestMap a map representing the job request form
	 * @return validated job instance ready for submission
	 * @throws JobProcessingException when validation fails unexpectedly
	 */
	public Job processJob(Map<String, Object> jobRequestMap)
	throws JobProcessingException
	{
	    Job job = new Job();

		// validate the user gave a valid job name
		String name = processNameRequest(jobRequestMap);
		
		// validate the user gave a valid software description
		setSoftware(processSoftwareName(jobRequestMap));

		// validate the optional execution system matches the software execution system
		setExecutionSystem(processExecutionSystemRequest(jobRequestMap, getSoftware()));

		/* **************************************************************************
		 **						Batch Parameter Selection 						  **
		 ***************************************************************************/

		String currentParameter = null;
		String queueName = null;
		BatchQueue jobQueue = null;
		Long nodeCount = null;
		Double memoryPerNode = null;
		String requestedTime = null;
		Long processorsPerNode = null;

		try
		{
			/* ********************************* Queue Selection *****************************************/

			currentParameter = "batchQueue";
			setQueueProcessor(new JobRequestQueueProcessor(getSoftware(), getExecutionSystem(), getUsername()));

			// we pass the queue processor the job request and let it validate the resource request and handle
			// queue selection. If no queue can be matched with the request, a NoMatchingBatchQueueException
			// exception will be thrown. If a queue was matched, we can use the matching BatchQueue in our job
			// and get the final, resolved resource requests from the queue processor.
			getQueueProcessor().process(jobRequestMap);

			jobQueue = getQueueProcessor().getMatchingBatchQueue();

			// node count defaults to 1 if not specified in the request, so no need to null check here.
			nodeCount = getQueueProcessor().getResourceRequest().getMaxNodes();

			// processors per node defaults to 1 if not specified in the request, so no need to null check here.
			processorsPerNode = getQueueProcessor().getResourceRequest().getMaxProcessorsPerNode();

			// memory can be omitted from the request and software. If so, then we assign the max memory per node of
			// for the queue to be safe.
			memoryPerNode = getQueueProcessor().getResourceRequest().getMaxMemoryPerNode();
			if (memoryPerNode == null) memoryPerNode = jobQueue.getMaxMemoryPerNode();

			// requeted time can be ommitted from the request and software. If so, then we assign the max runtime of
			// the queue to be safe.
			requestedTime = getQueueProcessor().getResourceRequest().getMaxRequestedTime();
			if (requestedTime == null) requestedTime = jobQueue.getMaxRequestedTime();

		}
		catch (JobProcessingException e)
		{
			throw e;
		}
		catch (NoMatchingBatchQueueException e) {
			throw new JobProcessingException(400, e.getMessage());
		}
		catch (Exception e) {
			throw new JobProcessingException(400, "Invalid " + currentParameter + " value.", e);
		}

		/* **************************************************************************
		 **						Verifying remote connectivity 					  **
		 ***************************************************************************/

		checkExecutionSystemLogin(getExecutionSystem());
		checkExecutionSystemStorage(getExecutionSystem());

		/* **************************************************************************
		 **						Verifying Input Parmaeters						  **
		 ***************************************************************************/

		// Verify the inputs by their keys given in the SoftwareInputs
		// in the Software object. 
		getInputProcessor().process(jobRequestMap);
		ObjectNode jobInputs = getInputProcessor().getJobInputs();
		
		/* **************************************************************************
		 **						Verifying  Parameters							  **
		 ***************************************************************************/
		getParameterProcessor().process(jobRequestMap);
		ObjectNode jobParameters = getParameterProcessor().getJobParameters();
		
		/* **************************************************************************
         **                 Create and assign job data                            **
         ***************************************************************************/

        try
        {
            // create a job object
            job.setName(name);
            job.setOwner(getUsername());
            job.setSoftwareName(getSoftware().getUniqueName());
            job.setInternalUsername(getInternalUsername());
            job.setSystem(getSoftware().getExecutionSystem().getSystemId());
            job.setSchedulerType(getSoftware().getExecutionSystem().getScheduler());
            job.setExecutionType(getSoftware().getExecutionType());
            job.setBatchQueue(jobQueue.getName());
            job.setNodeCount(nodeCount);
            job.setProcessorsPerNode(processorsPerNode);
            job.setMemoryPerNode(memoryPerNode);
            job.setMaxRunTime(requestedTime);
            job.setInputsAsJsonObject(jobInputs);
            job.setParametersAsJsonObject(jobParameters);
            job.setSubmitTime(new DateTime().toDate());
        }
        catch (JobException e) {
            throw new JobProcessingException(500, e.getMessage(), e);
        }
        
        /* **************************************************************************
		 **						End Batch Queue Selection 						  **
		 ***************************************************************************/

        
        /* **************************************************************************
		 **						Verifying optional notifications				  **
		 ***************************************************************************/
        
		processCallbackUrlRequest(jobRequestMap, job);

		/* **************************************************************************
		 **						Verifying archive configuration					  **
		 ***************************************************************************/

        // determine whether the user wanted to archiving the output
		boolean archiveOutput = processArchiveOutputRequest(jobRequestMap);
		job.setArchiveOutput(archiveOutput);
		
		// find the archive system 
		RemoteSystem archiveSystem = processArchiveSystemRequest(archiveOutput, jobRequestMap);
		job.setArchiveSystem(archiveSystem);
		
		// process the path now that we have all the info to resolve runtime job macros used
		// in a user-supplied archive path.
		String archivePath = processArchivePath(archiveOutput, jobRequestMap, archiveSystem, job);
		
		try
        {
		    job.setArchivePath(archivePath);

            // persisting the job makes it available to the job queue
            // for submission
		    DateTime created = new DateTime();
		    job.setCreated(created.toDate());
            JobDao.persist(job);
            job.setCreated(created.toDate());
            job.setStatus(JobStatusType.PENDING, JobStatusType.PENDING.getDescription());
            JobDao.persist(job);

            return job;
        }
        catch (Throwable e)
        {
            throw new JobProcessingException(500, "Failed to submit the request to the job queue.", e);
        }
	}

	/**
	 * Validates the callbackUrl parameter into a notification. This is only supported on job request form submissions
	 * which do not support structured data.
	 * @param jobRequestMap the job request form
	 * @param job the current job object
	 * @throws JobProcessingException if the url is invalid or cannot be generated.
	 */
	public void processCallbackUrlRequest(Map<String, Object> jobRequestMap, Job job) throws JobProcessingException
	{
		String defaultNotificationCallback = null;
		if (jobRequestMap.containsKey("callbackUrl")) {
			defaultNotificationCallback = (String)jobRequestMap.get("callbackUrl");
		} else if (jobRequestMap.containsKey("callbackURL")) {
			defaultNotificationCallback = (String)jobRequestMap.get("callbackURL");
		} else if (jobRequestMap.containsKey("notifications")) {
			defaultNotificationCallback = (String)jobRequestMap.get("notifications");
		}
		
		try {
			setNotificationProcessor(new JobRequestNotificationProcessor(getUsername(), job));
			
			if (StringUtils.isEmpty(defaultNotificationCallback)) {
				// nothing to do here. continue on
			}
			else {
				if (StringUtils.startsWithAny(defaultNotificationCallback,  new String[]{ "{", "[" })) {
					ObjectMapper mapper = new ObjectMapper();
					getNotificationProcessor().process(mapper.readTree(defaultNotificationCallback));
				}
				else {
					getNotificationProcessor().process(defaultNotificationCallback);
				}
			
				for (Notification n: getNotificationProcessor().getNotifications()) {
	                job.addNotification(n);
	            }
			}
		}
		catch (NotificationException e) {
            throw new JobProcessingException(500, "Failed to assign notification to job", e);
        } catch (IOException e) {
        	throw new JobProcessingException(400, "Unable to parse notification address provided in callbackUrl", e);
		}
	}

	/**
	 * Calculates the archive path of the job. If not supplied by the user, the path is generated for them using the
	 * job name and timestamp.
	 *
	 * @param archiveOutput should the job directory be archived
	 * @param jobRequestMap the job request form
	 * @param archiveSystem the system to which the job will be archived.
	 * @param job the current job object
	 * @return the remote path to which the job will be archived.
	 * @throws JobProcessingException
	 */
	public String processArchivePath(boolean archiveOutput, Map<String, Object> jobRequestMap, RemoteSystem archiveSystem, Job job) 
	throws JobProcessingException 
	{
		String archivePath = null;
		if (archiveOutput) {
			
		    if (jobRequestMap.containsKey("archivePath")) {
		    
    		    archivePath = (String)jobRequestMap.get("archivePath");

    		    if (StringUtils.isNotEmpty(archivePath)) {
    		    	
                    // resolve any macros from the user-supplied archive path into valid values based
                    // on the job request and use those
                    archivePath = JobArchivePathMacroType.resolveMacrosInPath(job, archivePath);

                    createArchivePath(archiveSystem, archivePath);
    			}
		    }
		}

		if (StringUtils.isEmpty(archivePath)) {
			archivePath = this.getUsername() + "/archive/jobs/job-" + job.getUuid();
		}
		
		return archivePath;
	}

	/**
	 * Creates the job archive directory if not already present
	 * @param archiveSystem the system to which the job output will be archived
	 * @param archivePath the agave relative path on the remote system
	 * @throws JobProcessingException if the remote directory cannot be created.
	 */
	public boolean createArchivePath(RemoteSystem archiveSystem, String archivePath) throws JobProcessingException
	{
		RemoteDataClient remoteDataClient = null;
		try
		{
		    remoteDataClient = archiveSystem.getRemoteDataClient(getInternalUsername());
		    remoteDataClient.authenticate();

		    LogicalFile logicalFile = LogicalFileDao.findBySystemAndPath(archiveSystem, archivePath);
		    PermissionManager pm = new PermissionManager(archiveSystem, remoteDataClient, logicalFile, getUsername());

		    if (!pm.canWrite(remoteDataClient.resolvePath(archivePath)))
		    {
		        throw new JobProcessingException(403,
		                "User does not have permission to access the provided archive path " + archivePath);
		    }
		    else
		    {
		        if (!remoteDataClient.doesExist(archivePath))
		        {
		            if (!remoteDataClient.mkdirs(archivePath, getUsername())) {
		                throw new JobProcessingException(400,
		                        "Unable to create job archive directory " + archivePath);
		            }
		        }
		        else
		        {
		            if (!remoteDataClient.isDirectory(archivePath))
		            {
		                throw new JobProcessingException(400,
		                        "Archive path is not a folder");
		            }
		        }
		    }
		    
		    return true;
		}
		catch (JobProcessingException e) {
		    throw e;
		}
		catch (RemoteDataException e) {
		    int httpcode = 500;
		    if (e.getMessage().contains("No credentials associated")) {
		        httpcode = 400;
		    }
		    throw new JobProcessingException(httpcode, e.getMessage(), e);
		}
		catch (Throwable e) {
		    throw new JobProcessingException(500, "Could not verify archive path", e);
		}
		finally {
		    try {
				if (remoteDataClient != null) {
					remoteDataClient.disconnect();
				}
			} catch (Exception ignored) {}
		}
	}

	/**
	 * Processes the archive system value in the job request form
	 * @param jobRequestMap the job request form
	 * @return the system to which the job will be archived.
	 * @throws JobProcessingException if the user does not have access
	 * @throws SystemException if the sytem is not found, or offline
	 */
	protected RemoteSystem processArchiveSystemRequest(boolean archiveOutput, Map<String, Object> jobRequestMap) 
	throws JobProcessingException, SystemException {
		
		RemoteSystem archiveSystem;
		SystemManager systemManager = new SystemManager();

		if (archiveOutput && jobRequestMap.containsKey("archiveSystem"))
	    {
			// lookup the user system
			String archiveSystemId = (String)jobRequestMap.get("archiveSystem");
			archiveSystem = new SystemDao().findUserSystemBySystemId(getUsername(), archiveSystemId, RemoteSystemType.STORAGE);
			if (archiveSystem == null) {
				throw new JobProcessingException(400,
						"No storage system found matching " + archiveSystem + " for " + getUsername());
			}
		}
		else
		{
		    // grab the user's default storage system
            archiveSystem = systemManager.getUserDefaultStorageSystem(getUsername());

            if (archiveOutput && archiveSystem == null) {
				throw new JobProcessingException(400,
						"Invalid archiveSystem. No archiveSystem was provided and you "
						+ "have no public or private default storage system configured. "
						+ "Please specify a valid system id for archiveSystem or configure "
						+ "a default storage system.");
			}
		}
		return archiveSystem;
	}

	/**
	 * Should the job output be archived? Default true;
	 * 
	 * @param jobRequestMap the job form
	 * @return true if the request to archive the output was true, false otherwise
	 */
	protected boolean processArchiveOutputRequest(Map<String, Object> jobRequestMap) {
		if (jobRequestMap.containsKey("archive") 
				&& StringUtils.isNotEmpty((String)jobRequestMap.get("archive"))) {
			
		    String doArchive = (String)jobRequestMap.get("archive");

			return BooleanUtils.toBoolean(doArchive) || doArchive.equals("1");
		}
		
		return true;
	}

	protected JobManager getJobManager() {
		return new JobManager();
	}

	/**
	 * Can we login to the remote system?
	 *
	 * @param executionSystem the system on which the job will run
	 * @throws JobProcessingException if the system is not accessible, auth fails, etc.
	 */
	public boolean checkExecutionSystemLogin(ExecutionSystem executionSystem) throws JobProcessingException {
		JobManager manager = getJobManager();
		try {
			// look up the last update time. We can skip the check if it's been access in the last few minutes.
			Instant lastAccess = manager.getLastSuccessfulSystemAccess(executionSystem);

			if (lastAccess == null) {
				AuthConfig authConfig = getExecutionSystem().getLoginConfig().getAuthConfigForInternalUsername(getInternalUsername());

				//When a system is cloned, a authConfig is not associated with it by default. It needs to be separately set.
				if (authConfig == null) {
					String msg1 = "Null authConfig encountered.";
					String msg2 = " There are no credentials configured for the system: " + getExecutionSystem().getSystemId() + "." +
							" Please update (POST) the system to configure credentials using the URL systems/v2/" + getExecutionSystem().getSystemId()
							+ "/credentials.";
					log.error(msg1 + msg2);
					throw new JobProcessingException(412, msg2);
				}

				String salt = getExecutionSystem().getEncryptionKeyForAuthConfig(authConfig);
				if (authConfig.isCredentialExpired(salt)) {
					String msg = " Credentials of the " + (authConfig.isSystemDefault() ? "Default user" : "Internal user " + getInternalUsername())
							+ " for the system " + getExecutionSystem().getSystemId() + " have expired."
							+ " Please update the system with a valid " + getExecutionSystem().getLoginConfig().getType()
							+ " of execution credentials for the execution system and resubmit the job.";
					log.error(msg);
					throw new JobProcessingException(412, msg);
				}

				try {
					if (getExecutionSystem().getRemoteSubmissionClient(getInternalUsername()).canAuthentication()) {
						// set the lat successful update time on this system
						manager.setLastSuccessfulSystemAccess(executionSystem, Instant.now());
					} else {
						String msg = "Unable to authenticate to " + getExecutionSystem().getSystemId();
						log.error(msg);
						throw new RemoteExecutionException(msg);
					}
				} catch (Throwable e) {
					String msg = "Unable to authenticate to " + getExecutionSystem().getSystemId() + " with the " +
							(authConfig.isSystemDefault() ? "default " : "internal user " + getInternalUsername()) +
							"credential. Please check the " + getExecutionSystem().getLoginConfig().getType() +
							" execution credential for the execution system and resubmit the job.";
					log.error(msg, e);
					throw new JobProcessingException(412, msg);
				}
			}
		} catch (JobProcessingException e) {
			// clear the last successful update time as it failed here
			manager.setLastSuccessfulSystemAccess(executionSystem, null);
			throw e;
		}
		
		return true;
	}
	
	/**
	 * Validates the storage credentials of the execution system.
	 * @param executionSystem the system on which the job will be run
	 * @throws JobProcessingException if the system credentials are bad or the system is unavailable
	 */
	public boolean checkExecutionSystemStorage(ExecutionSystem executionSystem) throws JobProcessingException {
		JobManager manager = getJobManager();
		try {
			// look up the last update time. We can skip the check if it's been access in the last few minutes.
			Instant lastAccess = manager.getLastSuccessfulSystemAccess(executionSystem);

			AuthConfig authConfig = getExecutionSystem().getStorageConfig().getAuthConfigForInternalUsername(getInternalUsername());
			String salt = getExecutionSystem().getEncryptionKeyForAuthConfig(authConfig);
			if (authConfig.isCredentialExpired(salt))
			{
				String msg = "Credential for " + getExecutionSystem().getSystemId() + " is not active." +
						" Please add a valid " + getExecutionSystem().getStorageConfig().getType() +
						" storage credential for the execution system and resubmit the job.";
				log.error(msg);
				throw new JobProcessingException(412, msg);
			}

			RemoteDataClient remoteExecutionDataClient = null;
			try {
				remoteExecutionDataClient = getExecutionSystem().getRemoteDataClient(getInternalUsername());
				remoteExecutionDataClient.authenticate();
				// set the lat successful update time on this system
				manager.setLastSuccessfulSystemAccess(executionSystem, Instant.now());
			} catch (Throwable e) {
				String msg = "Unable to authenticate to " + getExecutionSystem().getSystemId() + " with the " +
						(authConfig.isSystemDefault() ? "default " : "internal user " + getInternalUsername()) +
						"credential. Please check the " + getExecutionSystem().getLoginConfig().getType() +
						" execution credential for the execution system and resubmit the job.";
				log.error(msg, e);
				throw new JobProcessingException(412, msg);
			} finally {
				try {
					if (remoteExecutionDataClient != null) {
						remoteExecutionDataClient.disconnect();
					}
				} catch (Exception ignored) {}
			}
		} catch (JobProcessingException e) {
			// clear the last successful update time as it failed here
			manager.setLastSuccessfulSystemAccess(executionSystem, null);
			throw e;
		}

		return true;

	}

	/**
	 * Validates the sofware name was valid and the user has permission to use it.
	 * @param jobRequestMap the job request form
	 * @return the software requested by the job request
	 * @throws JobProcessingException if the software choice is invalid or unavailable
	 */
	public Software processSoftwareName(Map<String, Object> jobRequestMap) 
	throws JobProcessingException 
	{
		String softwareName = null;
		if (jobRequestMap.containsKey("appId")) {
			softwareName = (String)jobRequestMap.get("appId");
		} else {
			softwareName = (String)jobRequestMap.get("softwareName");
		}

		if (StringUtils.isEmpty(softwareName)) {
			throw new JobProcessingException(400,
					"appId cannot be empty");
		}
		else if (StringUtils.length(softwareName) > 80) {
			throw new JobProcessingException(400,
					"appId must be less than 80 characters");
		}
		else if (!softwareName.contains("-") || softwareName.endsWith("-"))
		{
			throw new JobProcessingException(400,
					"Invalid appId. " +
					"Please specify an app using its unique id. " +
					"The unique id is defined by the app name " +
					"and version separated by a hyphen. eg. example-1.0");
		}
		
		Software software = SoftwareDao.getSoftwareByUniqueName(softwareName.trim());

		if (software == null) {
			throw new JobProcessingException(400, "No app found matching " + softwareName + " for " + getUsername());
		}
		else if (!isSoftwareInvokableByUser(software, getUsername())) {
			throw new JobProcessingException(403, "Permission denied. You do not have permission to access this app");
		}
		
		return software;
	}
	
	/**
	 * Checks for availability of software for the user requesting the job.
	 * Delegates to {@link ApplicationManager#isInvokableByUser(Software, String)}.
	 * Wrapped here for testability.
	 * 
	 * @param software the software to check
	 * @param username the user to check
	 * @return true if the user has access to the software and user or better role on the execution system
	 */
	public boolean isSoftwareInvokableByUser(Software software, String username) {
		return ApplicationManager.isInvokableByUser(software, username);
	}

	/**
	 * Enforces basic naming conventions on the job name
	 * @param jobRequestMap the job request
	 * @return the trimmed job name
	 * @throws JobProcessingException if the name fails validation
	 */
	public String processNameRequest(Map<String, Object> jobRequestMap)
	throws JobProcessingException 
	{
		String name = null;
		if (jobRequestMap.containsKey("name")) {
			name = (String)jobRequestMap.get("name");
		} else {
			name = (String)jobRequestMap.get("jobName");
		}

		if (StringUtils.isEmpty(name)) {
			throw new JobProcessingException(400,
					"Job name cannot be empty.");
		}
		else if (StringUtils.length(name) > 64) {
			throw new JobProcessingException(400,
					"Job name must be less than 64 characters.");
		}
		else {
			name = name.trim();
		}
		
		return name;
	}

//	/**
//	 * Checks the node count, defaulting to the {@link Software#getDefaultNodes()} if not supplied by the job request.
//	 * @param software the software to run
//	 * @param userNodeCount the user requested node count
//	 * @return the number of nodes to include in the job request
//	 * @throws JobProcessingException if the number of nodes is not possible
//	 */
//	public Long processNodeCountRequest(Software software, String userNodeCount)
//	throws JobProcessingException
//	{
//		Long nodeCount;
//		if (StringUtils.isEmpty(userNodeCount))
//		{
//			// use the software default queue if present
//			if (software.getDefaultNodes() != null && software.getDefaultNodes() != -1) {
//				nodeCount = software.getDefaultNodes();
//			}
//			else
//			{
//				// use a single node otherwise
//				nodeCount = 1L;
//			}
//		}
//		else
//		{
//			nodeCount = NumberUtils.toLong(userNodeCount);
//		}
//
//		if (nodeCount < 1)
//		{
//			throw new JobProcessingException(400,
//					"Invalid " + (StringUtils.isEmpty(userNodeCount) ? "" : "default ") +
//					"nodeCount. If specified, nodeCount must be a positive integer value.");
//		}
//
////		nodeCount = pTable.containsKey("nodeCount") ? Long.parseLong(pTable.get("nodeCount")) : software.getDefaultNodes();
////		if (nodeCount < 1) {
////			throw new JobProcessingException(400,
////					"Invalid nodeCount value. nodeCount must be a positive integer value.");
////		}
//
//		return nodeCount;
//	}

	/**
	 * Processes the execution system
	 * @param jobRequestMap the job request form
	 * @param software the software requested by the job
	 * @return the system on which the job will run
	 * @throws JobProcessingException if the execution system cannot be processed
	 */
	public ExecutionSystem processExecutionSystemRequest(Map<String, Object> jobRequestMap, Software software)
	throws JobProcessingException 
	{	
		String exeSystemName = (String)jobRequestMap.get("executionSystem");
		if (jobRequestMap.containsKey("executionSystem")) {
			exeSystemName = (String)jobRequestMap.get("executionSystem");
		} else {
			exeSystemName = (String)jobRequestMap.get("executionHost");
		}
		
		ExecutionSystem executionSystem = software.getExecutionSystem();
		if (StringUtils.length(exeSystemName) > 64) {
			throw new JobProcessingException(400,
					"executionSystem must be less than 80 characters");
		}
		else if (!StringUtils.isEmpty(exeSystemName) && !StringUtils.equals(exeSystemName, executionSystem.getSystemId())) {
			throw new JobProcessingException(403,
					"Invalid execution system. Apps are registered to run on a specific execution system. If specified, " +
					"the execution system must match the execution system in the app description. The execution system " +
					"for " + software.getName() + " is " + software.getExecutionSystem().getSystemId() + ".");
		}
		return executionSystem;
	}

	/**
	 * @return the queueProcessor
	 */
	public JobRequestQueueProcessor getQueueProcessor() {
		return queueProcessor;
	}

	/**
	 * @param queueProcessor the queueProcessor to set
	 */
	public void setQueueProcessor(JobRequestQueueProcessor queueProcessor) {
		this.queueProcessor = queueProcessor;
	}

	/**
	 * @return the inputProcessor
	 */
	public JobRequestInputProcessor getInputProcessor() {
		if (this.inputProcessor == null) {
			this.inputProcessor = new JobRequestInputProcessor(getUsername(), getInternalUsername(), getSoftware());
		}
		return inputProcessor;
	}

	/**
	 * @param inputProcessor the inputProcessor to set
	 */
	public void setInputProcessor(JobRequestInputProcessor inputProcessor) {
		this.inputProcessor = inputProcessor;
	}

	/**
	 * @return the parameterProcessor
	 */
	public JobRequestParameterProcessor getParameterProcessor() {
		if (this.parameterProcessor == null) {
			this.parameterProcessor = new JobRequestParameterProcessor(getSoftware());
		}
		return this.parameterProcessor;
	}

	/**
	 * @param parameterProcessor the parameterProcessor to set
	 */
	public void setParameterProcessor(JobRequestParameterProcessor parameterProcessor) {
		this.parameterProcessor = parameterProcessor;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the internalUsername
	 */
	public String getInternalUsername() {
		return internalUsername;
	}

	/**
	 * @param internalUsername the internalUsername to set
	 */
	public void setInternalUsername(String internalUsername) {
		this.internalUsername = internalUsername;
	}

	/**
	 * @return the notificationProcessor
	 */
	public JobRequestNotificationProcessor getNotificationProcessor() {
		return notificationProcessor;
	}

	/**
	 * @param notificationProcessor the notificationProcessor to set
	 */
	public void setNotificationProcessor(JobRequestNotificationProcessor notificationProcessor) {
		this.notificationProcessor = notificationProcessor;
	}

	/**
	 * @return the executionSystem
	 */
	public ExecutionSystem getExecutionSystem() {
		return executionSystem;
	}

	/**
	 * @param executionSystem the executionSystem to set
	 */
	public void setExecutionSystem(ExecutionSystem executionSystem) {
		this.executionSystem = executionSystem;
	}

	/**
	 * @return the software
	 */
	public Software getSoftware() {
		return software;
	}

	/**
	 * @param software the software to set
	 */
	public void setSoftware(Software software) {
		this.software = software;
	}
}

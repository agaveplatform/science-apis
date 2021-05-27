/**
 * 
 */
package org.iplantc.service.jobs.model;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.Version;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Logger;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.ParamDef;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobEventProcessingException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.managers.JobEventProcessor;
import org.iplantc.service.jobs.model.dto.JobDTO;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.util.ServiceUtils;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.model.BatchQueue;
import org.joda.time.DateTime;
import org.json.JSONException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author dooley
 * 
 */
@Entity
@Table(name = "aloe_jobs")
@FilterDef(name="jobTenantFilter", parameters=@ParamDef(name="tenantId", type="string"))
@Filters(@Filter(name="jobTenantFilter", condition="tenant_id=:tenantId"))
public class Job {
	
	private static final Logger log = Logger.getLogger(Job.class);
	
	public static final String DEFAULT_MAX_HOURS_STRING_ENCODED = "01:00:00";
	public enum JobOutcome {FINISHED, FAILED, FAILED_SKIP_ARCHIVE}
	
	
	private Long				id;
	private String				uuid; 			// Unique id not based on database id
	private String				name;			// Human-readable name for this
												// job. Defaults to $application
	private String				owner;			// username of the user
												// submitting the job
	private String				internalUsername; // Username of InternalUser tied
												// tied to this job.
	private String				systemId;		// System on which this job ran.
												// Type is specified by the
												// software
	private String				appId;	// Unique name of an application
												// as given in the /apps/list
	private Integer				nodeCount;		// Requested nodes to use
	private Integer			    processorsPerNode; // Requested processors per node
	private Float				memoryPerNode;	// Requested memory per node
	private String				remoteQueue;	// Queue used to run the job
	private Float				maxHours;		// Requested time needed to run the job in 00:00:00 format
	
	private String				outputPath;		// relative path of the job's output folder
	
	private boolean				archive;	     // Boolean. If 'true' stage
												// job/output to $IPLANTHOME/job
	private String				archivePath;	// Override default location for
												// archiving job output
	private String		        archiveSystem;	// System on which to archive the output.
												// Uses iPlant Data Store by default.
	private String				workPath;		// Override default location for
												// archiving job output
	
	private Integer				statusChecks = 0; // number of times this job has been actively checked. 
												// used in exponential backoff
	private String				updateToken;	// Token needed to validate all
												// callbacks to update job
												// status
	private String				inputs;			// JSON encoded list of inputs
	private String				parameters;		// JSON encoded list of
												// parameters
	private String				remoteJobId;		// Local job or process id of
												// the job on the remote system
	private String				schedulerJobId; // Optional Unique job id given
												// by the scheduler if used.
	private Float				charge;			// Charge for this job against
												// the user's allocation
	private Date				remoteSubmitted;		// Date and time job was
												// submitted to the remote
												// system
	private Date				remoteStarted;		// Date and time job was started
	private Date				ended;		// Date and time job completed
	private String				lastStatusMessage;	// Error message of this job
												// execution
	private Date			  	lastUpdated;	// Last date and time job status
												// was updated
	private Date			   	created;		// Date job request was
												// submitted
	private Integer				submitRetries;		// Number of attempts to resubmit this job
	private boolean				visible;		// Can a user see the job?
	private Integer				version = 0;	// Entity version used for optimistic locking
	private String				tenantId;		// current api tenant
	private List<JobEvent>		events = new ArrayList<JobEvent>(); // complete history of events for this job
	
	
	/**
	 * From Aloe Job Model
	 */
	
	  private String          tenantQueue;           // Tenant worker queue on which this job was placed
	  private JobStatusType   status = JobStatusType.ACCEPTED;  // Current state of job
	 
	  
	  private Date         		accepted;              // Time job was accepted
	  
	  private String          	appUuid;               // The uuid of the appId name used to check referential integrity in DB
	  
	  private Date         		remoteEnded;           // Time job finished executing on remote system
	  private JobOutcome      	remoteOutcome;         // Best approximation of remote job's execution outcome
	 
	  private Integer         	remoteStatusChecks;    // Number of successful times the remote system was queried for job status 
	  private Integer         	failedStatusChecks;    // Number of failed times the remote system was queried for job status
	  private Date         		lastStatusCheck;       // Last update to either status check fields
	  
	  private Integer            blockedCount;          // Number of times job has transitioned to BLOCKED
	  
	  private String           roles;
	 	  
	
	public Job()
	{
		Date createDate = new DateTime().toDate();
		setCreated(createDate);
		setLastUpdated(createDate);
		updateToken = UUID.randomUUID().toString();
		nodeCount = new Integer(1);
		processorsPerNode = new Integer(1);
		memoryPerNode = new Float(1);
		maxHours = runTimeToHours(BatchQueue.DEFAULT_MAX_RUN_TIME);
		submitRetries = 0;
		archive = true;
		visible = true;
		
		tenantId = TenancyHelper.getCurrentTenantId();
		uuid = new AgaveUUID(UUIDType.JOB).toString();
	}
	
	@Column(name = "tenant_queue", nullable = false, length = 255)
	public String getTenantQueue() {
		return tenantQueue;
	}


	public void setTenantQueue(String tenantQueue) {
		this.tenantQueue = tenantQueue;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "accepted", nullable = false)
	public Date getAccepted() {
		//return Date.from(accepted);
		return accepted;
	}


	public void setAccepted(Date accepted) {
		this.accepted = accepted;
	}


	@Column(name = "app_uuid", nullable = false, length = 64)
	public String getAppUuid() {
		return appUuid;
	}


	public void setAppUuid(String appUuid) {
		this.appUuid = appUuid;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "remote_ended", nullable = true)
	public Date getRemoteEnded() {
		return remoteEnded;
	}


	public void setRemoteEnded(Date remoteEnded) {
		this.remoteEnded = remoteEnded;
	}

	@Enumerated(EnumType.STRING)
	@Column(name = "remote_outcome", nullable = true)
	public JobOutcome getRemoteOutcome() {
		return remoteOutcome;
	}


	public void setRemoteOutcome(JobOutcome remoteOutcome) {
		this.remoteOutcome = remoteOutcome;
	}
    
	@Column(name = "remote_status_checks", nullable = false, length = 11)
	public int getRemoteStatusChecks() {
		return remoteStatusChecks;
	}


	public void setRemoteStatusChecks(int remoteStatusChecks) {
		this.remoteStatusChecks = remoteStatusChecks;
	}

	@Column(name = "failed_status_checks", nullable = false, length = 11)
	public int getFailedStatusChecks() {
		return failedStatusChecks;
	}


	public void setFailedStatusChecks(int failedStatusChecks) {
		this.failedStatusChecks = failedStatusChecks;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "last_status_check", nullable = true)
	public Date getLastStatusCheck() {
		return lastStatusCheck;
	}


	public void setLastStatusCheck(Date lastStatusCheck) {
		this.lastStatusCheck = lastStatusCheck;
	}

	@Column(name = "blocked_count", nullable = false, length = 11)
	public Integer getBlockedCount() {
		return blockedCount;
	}


	public void setBlockedCount(int blockedCount) {
		this.blockedCount = blockedCount;
	}

	@Column(name = "roles", nullable = true, length =4096)
	public String getRoles() {
		return roles;
	}
	
	public void setRoles(String roles) {
		this.roles = roles;
	}

	/**
	 * @return the id
	 */
	
	@Id
	@GeneratedValue
	@Column(name = "id", unique = true, nullable = false)
	public Long getId()
	{
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(Long id)
	{
		this.id = id;
	}

	/**
	 * @return the uuid
	 */
	@Column(name = "uuid", nullable = false, length = 64, unique = true)
	public String getUuid()
	{
		return uuid;
	}

	/**
	 * @param uuid the uuid to set
	 */
	public void setUuid(String uuid)
	{
		this.uuid = uuid;
	}
	
	/**
	 * @return the name
	 */
	@Column(name = "name", nullable = false, length = 64)
	public String getName()
	{
		return name;
	}

	/**
	 * @param name the name to set
	 * @throws JobException 
	 */
	public void setName(String name) throws JobException
	{
		if (!StringUtils.isEmpty(name) && name.length() > 64) {
			throw new JobException("'job.name' must be less than 64 characters");
		}
		
		this.name = name;
	}

	/**
	 * @return the owner
	 */
	@Column(name = "owner", nullable = false, length = 32)
	public String getOwner()
	{
		return owner;
	}

	/**
	 * @param owner
	 *            the owner to set
	 * @throws JobException 
	 */
	public void setOwner(String owner) throws JobException
	{
		if (!StringUtils.isEmpty(owner) && owner.length() > 32) {
			throw new JobException("'job.owner' must be less than 32 characters");
		}
		
		this.owner = owner;
	}

	/**
	 * @return the internalUsername
	 */
	@Transient
	//@Column(name = "internal_username", nullable = true, length = 32)
	public String getInternalUsername()
	{
		return internalUsername;
	}

	/**
	 * @param internalUsername the internalUsername to set
	 * @throws JobException 
	 */
	@Transient
	public void setInternalUsername(String internalUsername) throws JobException
	{
		if (!StringUtils.isEmpty(internalUsername) && internalUsername.length() > 32) {
			throw new JobException("'job.internalUsername' must be less than 32 characters");
		}
		
		this.internalUsername = internalUsername;
	}

	/**
	 * @return the system
	 */
	@Column(name = "system_id", nullable = true, length = 64)
	public String getSystem()
	{
		return systemId;
	}

	/**
	 * @param system
	 *            the system to set
	 * @throws JobException 
	 */
	public void setSystem(String systemId) throws JobException
	{
		if (!StringUtils.isEmpty(systemId) && systemId.length() > 64) {
			throw new JobException("'job.system' must be less than 64 characters");
		}
		
		this.systemId = systemId;
	}

	/**
	 * @return the softwareName
	 */
	@Column(name = "app_id", nullable = false, length = 80)
	public String getAppId()
	{
		return appId;
	}

	/**
	 * @param softwareName
	 *            the softwareName to set
	 * @throws JobException 
	 */
	public void setAppId(String appId) throws JobException
	{
		if (!StringUtils.isEmpty(appId) && appId.length() > 80) {
			throw new JobException("'job.software' must be less than 80 characters");
		}
		
		this.appId = appId;
	}
	
	/**
	 * @return the batchQueue
	 */
	@Column(name = "remote_queue", nullable = true, length = 255)
	public String getRemoteQueue()
	{
		return remoteQueue;
	}

	/**
	 * @param queueRequest
	 *            the queueRequest to set
	 * @throws JobException 
	 */
	public void setRemoteQueue(String remoteQueue) throws JobException
	{
		if (!StringUtils.isEmpty(remoteQueue) && remoteQueue.length() > 255) {
			throw new JobException("'job.remoteQueue' must be less than 255 characters");
		}
		
		this.remoteQueue = remoteQueue;
	}
	
	/**
	 * @return the nodeCount
	 */
	@Column(name = "node_count", nullable = false)
	public Integer getNodeCount()
	{
		return nodeCount;
	}

	/**
	 * @param nodeCount
	 *            the nodeCount to set
	 */
	public void setNodeCount(Integer nodeCount) 
	{
		this.nodeCount = nodeCount;
	}

	/**
	 * @return the processorsPerNode
	 */
	@Column(name = "processor_count", nullable = false)
	public Integer getProcessorsPerNode()
	{
		return processorsPerNode;
	}

	/**
	 * @param processorCount
	 *            the processorCount to set
	 */
	public void setProcessorsPerNode(Integer processorsPerNode) 
	{
		
		this.processorsPerNode = processorsPerNode;
	}

	/**
	 * @return the memoryPerNode
	 */
	@Column(name = "memory_gb", nullable = false)
	public Float getMemoryPerNode()
	{
		return memoryPerNode;
	}

	/**
	 * @param memoryPerNode
	 *            the memoryPerNode to set
	 */
	public void setMemoryPerNode(Float memoryPerNode)
	{
		this.memoryPerNode = memoryPerNode;
	}

//	/**
//	 * @return the callbackUrl
//	 */
//	@Column(name = "callback_url", nullable = true, length = 255)
//	public String getCallbackUrl()
//	{
//		return callbackUrl;
//	}
//
//	/**
//	 * @param callbackUrl
//	 *            the callbackUrl to set
//	 * @throws JobException 
//	 */
//	public void setCallbackUrl(String callbackUrl) throws JobException
//	{
//		if (!StringUtils.isEmpty(callbackUrl) && callbackUrl.length() > 255) {
//			throw new JobException("'job.callbackUrl' must be less than 255 characters");
//		}
//		
//		this.callbackUrl = callbackUrl;
//	}

	/**
	 * @param outputPath the outputPath to set
	 * @throws JobException 
	 */
	@Transient
	public void setOutputPath(String outputPath) throws JobException
	{
		if (!StringUtils.isEmpty(outputPath) && outputPath.length() > 255) {
			throw new JobException("'job.outputPath' must be less than 255 characters");
		}
		
		this.outputPath = outputPath;
	}

	/**
	 * @return the outputPath
	 */
	@Transient
	//@Column(name = "output_path", nullable = true, length = 255)
	public String getOutputPath()
	{
		return outputPath;
	}

	/**
	 * @return the archiveOutput
	 */
	@Column(name = "archive", nullable = false, columnDefinition = "TINYINT(1)")
	public Boolean isArchive()
	{
		return archive;
	}

	/**
	 * @param archiveOutput
	 *            the archiveOutput to set
	 */
	public void setArchive(Boolean archive)
	{
		this.archive= archive;
	}

	/**
	 * @return the archivePath
	 */
	@Column(name = "archive_path", nullable = true, length = 255)
	public String getArchivePath()
	{
		return archivePath;
	}

	/**
	 * @param archivePath
	 *            the archivePath to set
	 * @throws JobException 
	 */
	public void setArchivePath(String archivePath) throws JobException
	{
		if (!StringUtils.isEmpty(archivePath) && archivePath.length() > 255) {
			throw new JobException("'job.archivePath' must be less than 255 characters");
		}
		
		this.archivePath = archivePath;
	}

	/**
	 * @return the archiveSystem
	 */
	//@ManyToOne(fetch = FetchType.EAGER)
    //@JoinColumn(name = "archive_system_id", referencedColumnName = "id")
	@Column(name = "archive_system_id", nullable = true, length = 64)
    public String getArchiveSystem()
	{
		return archiveSystem;
	}

	/**
	 * @param archiveSystem the archiveSystem to set
	 */
	public void setArchiveSystem(String archiveSystem)
	{
		this.archiveSystem = archiveSystem;
	}

	/**
	 * @return the workPath
	 */
	@Column(name = "work_path", nullable = true, length = 255)
	public String getWorkPath()
	{
		return workPath;
	}

	/**
	 * @param workPath
	 *            the workPath to set
	 * @throws JobException 
	 */
	public void setWorkPath(String workPath) throws JobException
	{
		if (!StringUtils.isEmpty(workPath) && workPath.length() > 255) {
			throw new JobException("'job.workPath' must be less than 255 characters");
		}
		
		this.workPath = workPath;
	}

	/**
	 * @return the status
	 */
	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 32)
	public JobStatusType getStatus()
	{
		return status;
	}
	
	/**
	 * Returns number of times this job's status has been checked
	 * by the monitoring tasks. This is used to calculate the exponential
	 * backoff used to throttle status checks on long-running jobs.
	 * 
	 * @return Integer
	 */
	@Transient
	//@Column(name = "status_checks", nullable = false)
	public Integer getStatusChecks() {
		return statusChecks;
	}

	/**
	 * @param statusChecks
	 */
	@Transient
	public void setStatusChecks(Integer statusChecks) {
		this.statusChecks = statusChecks;
	}

	/**
	 * Returns a list of job events in the history of this job.
	 * 
	 * @return
	 */
	@Transient
	@OneToMany(cascade = {CascadeType.ALL}, mappedBy = "job", fetch=FetchType.EAGER, orphanRemoval=true)
	public List<JobEvent> getEvents() {
		return events;
	}
	
	/**
	 * @param events
	 */
	@Transient
	public void setEvents(List<JobEvent> events) {
		this.events = events;
	}
	
	/**
	 * Adds an event to the history of this job. This will automatically
	 * be saved with the job when the job is persisted.
	 * 
	 * @param event
	 */
	@Transient
	public void addEvent(JobEvent event) {
		event.setJob(this);
		this.events.add(event);
		JobEventProcessor jep;
        try {
            jep = new JobEventProcessor(event);
            jep.process();
        } catch (JobEventProcessingException e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * Convenience endpoint to add a notification to this job. Notification.persistent
	 * defaults to false. 
	 * 
	 * @param event String notification event that will trigger the callback
	 * @param callbackUrl A URL or email address that will be triggered by the event
	 */
	public void addNotification(String event, String callback) throws NotificationException 
	{
		addNotification(event, callback, false);
	}
	
	/**
	 * Convenience endpoint to add a notification to this job. 
	 * 
	 * @param event String notification event that will trigger the callback
	 * @param callbackUrl A URL or email address that will be triggered by the event
	 * @param persistent Whether this notification should expire after the first successful trigger
	 */
	@Transient
	public void addNotification(String event, String callback, boolean persistent) 
	throws NotificationException 
	{
		Notification notification = new Notification(event, callback);
		notification.setOwner(owner);
		notification.setAssociatedUuid(uuid);
		notification.setPersistent(persistent);
		
		addNotification(notification);
	}
	
	/**
	 * Convenience endpoint to add a notification to this job. 
	 * 
	 * @param notification A notification event to associate with this job. The current
	 * jobs owner and uuid will be added to the notification.
	 */
	@Transient
	public void addNotification(Notification notification) throws NotificationException 
	{
		notification.setOwner(owner);
		notification.setAssociatedUuid(uuid);
		new NotificationDao().persist(notification);
	}
	
	/**
	 * Sets the job status and creates an job history event with 
	 * the given status and message;
	 * 
	 * @param status
	 * @param message
	 */
	@Transient
	public void setStatus(JobStatusType status, String message) throws JobException
	{
		setStatus(status, new JobEvent(this, status, message, getOwner()));
//		// avoid adding duplicate entries over and over from watch 
//		// and monitoring queue updates.
//		if (!this.status.equals(status) || !StringUtils.equals(getErrorMessage(), message)) {
//			// we don't want the job status being updated after the job is deleted as we
//			// already move it to a terminal state when it's deleted. Here we check for 
//			// job deletion and, then if visible, propagate the event. Otherwise, we 
//			// simply add it to the history for reference and move on.
//			if (this.isVisible()) {
//				setStatus(status);
//				setErrorMessage(message);
//				addEvent(new JobEvent(status, message, getOwner()));
//			}
//			else {
//				message += " Event will be ignored because job has been deleted.";
//				this.events.add(new JobEvent(this, status, message, getOwner()));
//			}
//		} else {
////			log.debug("Ignoring status update to " + status + " with same message");
//		}
	}
	
	/**
	 * Sets the job status and associates the job history event with 
	 * the job;
	 * 
	 * @param status
	 * @param message
	 */
	@Transient
	public void setStatus(JobStatusType status, JobEvent event) throws JobException
	{
		// avoid adding duplicate entries over and over from watch 
		// and monitoring queue updates.
		if (!this.status.equals(status) || !StringUtils.equals(getLastStatusMessage(), event.getDescription())) {
			// we don't want the job status being updated after the job is deleted as we
			// already move it to a terminal state when it's deleted. Here we check for 
			// job deletion and, then if visible, propagate the event. Otherwise, we 
			// simply add it to the history for reference and move on.
			if (this.isVisible()) {
				setStatus(status);
				setLastStatusMessage(event.getDescription());
				addEvent(event);
			}
			else {
				event.setDescription(event.getDescription() + " Event will be ignored because job has been deleted.");
				this.events.add(event);
			}
		} else {
//			log.debug("Ignoring status update to " + status + " with same message");
		}
	}
	
	/**
	 * @param status
	 *            the status to set
	 */
	private void setStatus(JobStatusType status)
	{
		this.status = status;
	}

	/**
	 * @return the updateToken
	 */
	@Column(name = "update_token", nullable = false, length = 64)
	public String getUpdateToken()
	{
		return updateToken;
	}

	/**
	 * @param updateToken
	 *            the updateToken to set
	 * @throws JobException 
	 */
	public void setUpdateToken(String updateToken) throws JobException
	{
		if (!StringUtils.isEmpty(updateToken) && updateToken.length() > 64) {
			throw new JobException("'job.updateToken' must be less than 64 characters");
		}
		
		this.updateToken = updateToken;
	}

	/**
	 * @return the inputs
	 */
	@Column(name = "inputs", nullable = true, columnDefinition = "TEXT")
	public String getInputs()
	{
		return inputs;
	}

//	public void setInputsAsMap(Map<String, String> map) throws JobException
//	{
//		try 
//		{
//			ObjectMapper mapper = new ObjectMapper();
//			ObjectNode json = mapper.createObjectNode();
//			for (String paramKey : map.keySet())
//			{
//				json.put(paramKey, StringUtils.trim(map.get(paramKey).toString()));
//			}
//			inputs = json.toString();
//		}
//		catch (Exception e) {
//			throw new JobException("Failed to parse job parameters", e);
//		}
//	}

//	@Transient
//	public Map<String, String> getInputsAsMap() throws JobException
//	{
//		try {
//			Map<String, String> map = new HashMap<String, String>();
//			if (!ServiceUtils.isValid(inputs))
//				return map;
//			ObjectMapper mapper = new ObjectMapper();
//			JsonNode json = mapper.readTree(inputs);
//			for (Iterator<String> inputIterator = json.fieldNames(); inputIterator.hasNext();)
//			{
//				String inputKey = inputIterator.next();
//				map.put(inputKey, StringUtils.trim(json.get(inputKey).textValue()));
//			}
//			return map;
//		}
//		catch (Exception e) {
//			throw new JobException("Failed to parse job inputs", e);
//		}
//	}
	
	@Transient
	public JsonNode getInputsAsJsonObject() 
	{
		ObjectMapper mapper = new ObjectMapper();
		try 
		{
		  if (StringUtils.isEmpty(getInputs())) {
				return mapper.createObjectNode();
			}
			else
			{
				return mapper.readTree(getInputs());
			}
		}
		catch (Exception e) {
			log.error("Failed to parse job inputs", e);
			return mapper.createObjectNode();
		}
	}
	
	@Transient
	public void setInputsAsJsonObject(JsonNode json) throws JobException
	{
		if (json == null || json.isNull() || json.size() == 0) {
			setInputs(null);
		} else {
			setInputs(json.toString());
		}
	}

	/**
	 * @param inputs
	 *            the inputs to set
	 * @throws JobException 
	 */
	private void setInputs(String inputs) throws JobException
	{
		if (!StringUtils.isEmpty(inputs) && inputs.length() > 16384) {
			throw new JobException("'job.inputs' must be less than 16384 characters");
		}
		
		this.inputs = inputs;
	}

	/**
	 * @return the parameters
	 */
	@Column(name = "parameters", nullable = true, columnDefinition = "TEXT")
	public String getParameters()
	{
		return parameters;
	}
	
	@Transient
	public JsonNode getParametersAsJsonObject() 
	{
		ObjectMapper mapper = new ObjectMapper();
		try 
		{
			if (StringUtils.isEmpty(getParameters()))
			{
				return mapper.createObjectNode();
			}
			else
			{
				return mapper.readTree(getParameters());
			}
		}
		catch (Exception e) {
			log.error("Failed to parse job parameters", e);
			return mapper.createObjectNode();
		}
	}
	
	@Transient
	public void setParametersAsJsonObject(JsonNode json) throws JobException
	{
		if (json == null || json.isNull() || json.size() == 0) {
			setParameters(null);
		} else {
			setParameters(json.toString());
		}
	}

//	@Transient
//	@Deprecated
//	public Map<String, Object> getParametersAsMap() throws JobException
//	{
//		try 
//		{	
//			Map<String, Object> map = new HashMap<String, Object>();
//			JsonNode json = getParametersAsJsonObject();
//			for (Iterator<String> parameterIterator = json.fieldNames(); parameterIterator.hasNext();)
//			{
//				String parameterKey = parameterIterator.next();
//				map.put(parameterKey, json.get(parameterKey).textValue());
//			}
//			return map;
//		}
//		catch (Exception e) {
//			throw new JobException("Failed to parse job parameters", e);
//		}
//	}
//
//	@Deprecated
//	public void setParametersAsMap(Map<String, Object> map)
//	throws JobException
//	{
//		try 
//		{
//			ObjectMapper mapper = new ObjectMapper();
//			ObjectNode json = mapper.createObjectNode();
//			for (String paramKey : map.keySet())
//			{
//				json.put(paramKey, map.get(paramKey).toString());
//			}
//			parameters = json.toString();
//		}
//		catch (Exception e) {
//			throw new JobException("Failed to parse job parameters", e);
//		}
//	}
	
//	public void setParametersAsJsonNode(JsonNode json)
//	throws JobException
//	{
//		try 
//		{
//			parameters = json.toString();
//		}
//		catch (Exception e) {
//			throw new JobException("Failed to parse job parameters", e);
//		}
//	}

	/**
	 * @param parameters
	 *            the parameters to set
	 * @throws JobException 
	 */
	private void setParameters(String parameters) throws JobException
	{
		if (!StringUtils.isEmpty(parameters) && parameters.length() > 16384) {
			throw new JobException("'job.parameters' must be less than 16384 characters");
		}
		
		this.parameters = parameters;
	}

	/**
	 * @return the localJobId
	 */
	@Column(name = "remote_job_id", nullable = true, length = 255)
	public String getLocalJobId()
	{
		return remoteJobId;
	}
	
	@Transient
	public String getNumericLocalJobId() {
		String numericLocalJobId = null;
		String[] tokens = StringUtils.split(getLocalJobId(), ".");
		if (tokens != null && tokens.length > 0) {
			for (String token: tokens) {
				if (NumberUtils.isDigits(token)) {
					numericLocalJobId = token; 
					break;
				}
			}
		}
		
		return numericLocalJobId;
	}

	/**
	 * @param localJobId
	 *            the localJobId to set
	 * @throws JobException 
	 */
	public void setLocalJobId(String remoteJobId) throws JobException
	{
		if (!StringUtils.isEmpty(remoteJobId) && remoteJobId.length() > 255) {
			throw new JobException("'job.localId' must be less than 255 characters");
		}
		
		this.remoteJobId = remoteJobId;
	}

	/**
	 * @return the schedulerJobId
	 */
	@Column(name = "remote_sched_id", nullable = true, length = 255)
	public String getSchedulerJobId()
	{
		return schedulerJobId;
	}

	/**
	 * @param schedulerJobId
	 *            the schedulerJobId to set
	 * @throws JobException 
	 */
	public void setSchedulerJobId(String schedulerJobId) throws JobException
	{
		if (!StringUtils.isEmpty(schedulerJobId) && schedulerJobId.length() > 255) {
			throw new JobException("'job.schedulerJobId' must be less than 255 characters");
		}
		
		this.schedulerJobId = schedulerJobId;
	}

	/**
	 * @return the charge
	 */
	@Transient
	//@Column(name = "charge")
	public Float getCharge()
	{
		return charge;
	}

	/**
	 * @param charge
	 *            the charge to set
	 */
	@Transient
	public void setCharge(Float charge)
	{
		this.charge = charge;
	}

	/**
	 * @return the submitTime
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "remote_submitted", nullable = true, length = 3)
	public Date getRemoteSubmitted()
	{
		return remoteSubmitted;
	}

	/**
	 * @param submitTime
	 *            the submitTime to set
	 */
	public void setRemoteSubmitted(Date remoteSubmitted)
	{
		this.remoteSubmitted = remoteSubmitted;
	}

	/**
	 * @param maxHours the maxHours to set
	 */
	public void setMaxHours(Float maxHours)
	{
		this.maxHours = maxHours;
	}

	/**
	 * @return the maxRunTime
	 */
	@Column(name = "max_hours", nullable = false)
	public Float getMaxHours()
	{
		return maxHours;
	}

	/**
	 * @return the startTime
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "remote_started", nullable = true)
	public Date getRemoteStarted()
	{
		return remoteStarted;
	}

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	public void setRemoteStarted(Date remoteStarted)
	{
		this.remoteStarted = remoteStarted;
	}

	/**
	 * @return the endTime
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "ended", nullable = true)
	public Date getEnded()
	{
		return ended;
	}

	/**
	 * @param ended
	 *            the endTime to set
	 */
	public void setEnded(Date ended)
	{
		this.ended = ended;
	}

	/**
	 * @param errorMessage
	 *            the errorMessage to set
	 * @throws JobException 
	 */
	public void setLastStatusMessage(String lastStatusMessage) throws JobException
	{
		if (!StringUtils.isEmpty(lastStatusMessage) && lastStatusMessage.length() > 16384) {
			throw new JobException("'job.message' must be less than 16384 characters");
		}
		
		this.lastStatusMessage = lastStatusMessage;
	}

	/**
	 * @return the lastStatusMessage
	 */
	@Column(name = "last_message", nullable = true, length = 2048)
	public String getLastStatusMessage()
	{
		return lastStatusMessage;
	}

	

	//@Transient
	public void setLastUpdated(Date lastUpdated)
	{
		this.lastUpdated = new DateTime(lastUpdated).withMillisOfSecond(0).toDate();
	}
	/**
	 * @param lastUpdated
	 *            the lastUpdated to set
	 */
	/*public void setLastUpdated(Instant lastUpdated)
	{
		this.lastUpdated = new DateTime(lastUpdated).withMillisOfSecond(0).toDate().toInstant();
	}*/

	/**
	 * @return the lastUpdated
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "last_updated", nullable = false)
	public Date getLastUpdated()
	{
		return lastUpdated;
	}
	
	//@Transient
	public void setCreated(Date created) {
		this.created = new DateTime(created).withMillisOfSecond(0).toDate();
	}
	
	/**
	 * @param created
	 *            the created to set
	 */
	/*public void setCreated(Instant created)
	{
		this.created = new DateTime(created).withMillisOfSecond(0).toDate().toInstant();
	}*/

	/**
	 * @return the created
	 */
	//@Temporal(TemporalType.TIMESTAMP)
	/*@Column(name = "created", nullable = true)
	public Instant getCreated()
	{
		return created;
	}*/
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "created", nullable = true)
	public Date getCreated()
	{
			return created;
	}

	/**
	 * @param visible the visible to set
	 */
	public void setVisible(Boolean visible)
	{
		this.visible = visible;
	}

	/**
	 * @return the visible
	 */
	@Column(name = "visible", columnDefinition = "TINYINT(1)")
	public Boolean isVisible()
	{
		return visible;
	}
	
	/**
	 * @return the retries
	 */
	@Column(name = "remote_submit_retries", nullable = false, length=11)
	public Integer getSubmitRetries()
	{
		return submitRetries;
	}

	/**
	 * @param retries the retries to set
	 */
	public void setSubmitRetries(Integer submitRetries)
	{
		this.submitRetries = submitRetries;
	}
	
	/**
	 * @return the version
	 */
	@Transient
	@Version
   // @Column(name="OPTLOCK")
    public Integer getVersion() {
		return version;
	}
	
	/**
	 * @param version the current version
	 */
	@Transient
	public void setVersion(Integer version) {
		this.version = version;
	}

	/**
	 * @return the tenantId
	 */
	@Column(name = "tenant_id", nullable=false, length = 128)
	public String getTenantId()
	{
		return tenantId;
	}

	/**
	 * @param tenantId the tenantId to set
	 */
	public void setTenantId(String tenantId)
	{
		this.tenantId = tenantId;
	}

	@Transient
	public String getArchiveUrl() 
	{
		if (isFinished() && archive) {
			return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE, getTenantId()) + 
					"listings/system/" + getArchiveSystem() + "/" + UrlPathEscaper.escape(getArchivePath());
		} else {
			return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE, getTenantId()) + 
					uuid + "/outputs/listings";
		}
	}
	
	@Transient
	public String getArchiveCanonicalUrl() 
	{
		if (isFinished() && archive) {
			return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE, getTenantId()) + 
					"listings/system/" + getArchiveSystem() + "/" + UrlPathEscaper.escape(getArchivePath());
		} else {
			return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE, getTenantId()) + 
					"listings/system/" + getSystem() + "/" + UrlPathEscaper.escape(getWorkPath());
		}
	}

	@Transient
	public boolean isFinished()
	{
		return JobStatusType.isFinished(status);
	}

	@Transient
	public boolean isSubmitting()
	{
		return JobStatusType.isSubmitting(status);
	}

	@Transient
	public boolean isRunning()
	{
		return JobStatusType.isRunning(status);
	}

	@Transient
	public boolean isArchived()
	{
		return JobStatusType.isArchived(status);
	}
	
	@Transient
	public boolean isFailed()
	{
		return JobStatusType.isFailed(status);
	}
	
	@Transient
	public boolean equals(Object o)
	{
		if (o instanceof Job) {
			return ( name.equals( ( (Job) o ).name)
					&& owner.equals( ( (Job) o ).owner)
					&& updateToken.equals( ( (Job) o ).updateToken) 
					&& appId.equals( ( (Job) o ).appId) );
		}
		else if (o instanceof JobDTO) {
			return uuid.equals( ( (JobDTO) o ).getUuid());
		} 
		else {
			return false;
		}
	}

	@Transient
	@JsonProperty("_links")
	private ObjectNode getHypermedia() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode linksObject = mapper.createObjectNode();
		
		linksObject.set("self", (ObjectNode)mapper.createObjectNode()
    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE, getTenantId()) + getUuid()));
		
		linksObject.set("app", mapper.createObjectNode()
    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_APPS_SERVICE, getTenantId()) + getAppId()));
		
		linksObject.set("executionSystem", mapper.createObjectNode()
    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_SYSTEM_SERVICE, getTenantId()) + getSystem()));
		
		if(getArchiveSystem() == null) {
			linksObject.set("archiveSystem", mapper.createObjectNode());
		} else {
		linksObject.set("archiveSystem", mapper.createObjectNode()
        		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_SYSTEM_SERVICE, getTenantId()) + 
        				getArchiveSystem()));
		}
		
		linksObject.set("archiveData", mapper.createObjectNode()
    		.put("href", getArchiveUrl()));
    	
		linksObject.set("owner", mapper.createObjectNode()
			.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE, getTenantId()) + owner));
		
		linksObject.set("permissions", mapper.createObjectNode()
    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE, getTenantId()) + uuid + "/pems"));
        
		linksObject.set("history", mapper.createObjectNode()
			.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_JOB_SERVICE, getTenantId()) + uuid + "/history"));
	    
		linksObject.set("metadata", mapper.createObjectNode()
			.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE, getTenantId()) + "data/?q=" + URLEncoder.encode("{\"associationIds\":\"" + uuid + "\"}")));
		
		linksObject.set("notifications", mapper.createObjectNode()
			.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE, getTenantId()) + "?associatedUuid=" + uuid));
		
    	return linksObject;
	}
	
	@JsonValue
	public String toJSON() throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectNode json = mapper.createObjectNode()
			.put("id", uuid)
			.put("name", name)
			.put("tenantId", tenantId)
			.put("tenantQueue", tenantQueue)
			.put("status", status.name())
			.put("lastStatusMessage", getLastStatusMessage())
			.put("accepted", new DateTime(accepted).toString())
		    .put("created", getCreated() == null ? null : new DateTime(created).toString())
		    .put("ended", getEnded() == null ? null : new DateTime(getEnded()).toString())
			.put("lastUpdated", new DateTime(lastUpdated).toString())
			.put("owner", owner)
			.put("roles", roles)
			.put("systemId", systemId)
			.put("appId", appId)
			.put("appUuid", appUuid)
			.put("workPath", workPath)
			.put("archive", archive)
			.put("archivePath", archivePath)
		    .put("archiveSystem", archiveSystem)
		    .put("nodeCount", nodeCount)
		    .put("processorsPerNode", processorsPerNode)
		    .put("memoryPerNode", memoryPerNode)
		    .put("maxHours", maxHours);
		json.set("inputs", getInputsAsJsonObject());
		json.set("parameters", getParametersAsJsonObject());
			
		json.put("remoteJobId", remoteJobId)
		    .put("schedulerJobId", schedulerJobId)
		    .put("remoteQueue", remoteQueue)
		    .put("remoteSubmitted", getRemoteSubmitted() == null ? null : new DateTime(getRemoteSubmitted()).toString())
		    .put("remoteStarted", getRemoteStarted() == null ? null : new DateTime(getRemoteStarted()).toString())
	        .put("remoteEnded", getRemoteEnded() == null ? null : new DateTime(getRemoteEnded()).toString())
	        .put("remoteOutcome", getRemoteOutcome() == null? null : remoteOutcome.toString())
			.put("submitRetries", submitRetries)
			.put("remoteStatusChecks",remoteStatusChecks)
			.put("failedStatusChecks",failedStatusChecks)
			.put("lastStatusCheck", getLastStatusCheck() == null? null : new DateTime(lastStatusCheck).toString())
			.put("blockedCount", blockedCount)
			.put("visible", visible);
		    		
    	json.set("_links", getHypermedia());
    	
		return json.toString();
	}

	public Job copy() throws JSONException, JobException
	{
		Job job = new Job();
		job.setName(name);
		job.setOwner(owner);
		job.status = JobStatusType.PENDING;
		job.lastStatusMessage = "Job resumitted for execution from job " + getUuid();
		job.setAppId(appId);
		job.setSystem(systemId);
		job.setNodeCount(nodeCount);
		job.setRemoteQueue(remoteQueue);
		job.setProcessorsPerNode(processorsPerNode);
		job.setMemoryPerNode(memoryPerNode);
		job.setInputs(getInputs());
		job.setParameters(getParameters());
		job.setMaxHours(maxHours);
		job.setRemoteSubmitted(new DateTime().toDate());
		
		job.setArchive(isArchive());
		job.setArchiveSystem(getArchiveSystem());
		
		if (StringUtils.isEmpty(getArchivePath())) {
			job.setArchivePath(getOwner() + "/archive/jobs/job-" + job.getUuid() + "-" + Slug.toSlug(getName()));
		}
		else		
		{
			if (StringUtils.contains(getArchivePath(), getUuid())) {
				job.setArchivePath(StringUtils.replaceChars(getArchivePath(), getUuid(), job.getUuid()));
			} else {
				job.setArchivePath(getOwner() + "/archive/jobs/job-" + job.getUuid() + "-" + Slug.toSlug(getName()));
			}
		}
		
		return job;
	}

	@Transient
	public Object getValueForAttributeName(String attribute) throws JobException, JSONException
	{
		Object value = null;
		
		if (attribute.equalsIgnoreCase("id")) {
			value = id;
		} else if (attribute.equalsIgnoreCase("name")) {
			value = name;
		} else if (attribute.equalsIgnoreCase("tenantId")) { 
			value = tenantId;
		} else if (attribute.equalsIgnoreCase("tenantQueue")) { 
			value = tenantQueue;
		} else if (attribute.equalsIgnoreCase("status")) {
			value = status;
		} else if (attribute.equalsIgnoreCase("lastStatusMessage")) { 
			value = lastStatusMessage;
		} else if (attribute.equalsIgnoreCase("accepted")) { 
			value = accepted;
		} else if (attribute.equalsIgnoreCase("created")) { 
			value = created;
		} else if (attribute.equalsIgnoreCase("lastUpdated")) {
			value = lastUpdated;
		} else if (attribute.equalsIgnoreCase("owner")) {
			value = owner;
		} else if (attribute.equalsIgnoreCase("roles")) {
			value = roles;
		} else if (attribute.equalsIgnoreCase("systemId")) {
			value = systemId;
		} else if (attribute.equalsIgnoreCase("appId")) {
			value = appId;
		} else if (attribute.equalsIgnoreCase("appUuid")) {
			value = appUuid;
		} else if (attribute.equalsIgnoreCase("workPath")) {
			value = workPath ;
		} else if (attribute.equalsIgnoreCase("archive")) {
			return archive;
		} else if (attribute.equalsIgnoreCase("archivePath")) {
			value = archivePath;
		} else if (attribute.equalsIgnoreCase("archiveSystem")) {
			value = archiveSystem;
		} else if (attribute.equalsIgnoreCase("nodeCount")) {
			value = nodeCount;
		} else if (attribute.equalsIgnoreCase("processorsPerNode")) {
			value = processorsPerNode;
		} else if (attribute.equalsIgnoreCase("memoryPerNode")) {
			value = memoryPerNode;
		} else if (attribute.equalsIgnoreCase("maxHours")) {
			value = maxHours;
		} else if (attribute.equalsIgnoreCase("remoteJobId")) {
			value = remoteJobId;
		} else if (attribute.equalsIgnoreCase("schedulerJobId")) {
			value = schedulerJobId;
		} else if (attribute.equalsIgnoreCase("remoteQueue")) {
			value = remoteQueue;
		} else if (attribute.equalsIgnoreCase("remoteOutcome")) {
			value = remoteOutcome;
		} else if (attribute.equalsIgnoreCase("submitRetries")) {
			value = submitRetries;
		} else if (attribute.equalsIgnoreCase("remoteStatusChecks")) {
			value = remoteStatusChecks;
		} else if (attribute.equalsIgnoreCase("failedStatusChecks")) {
			value = failedStatusChecks;
		}  else if (attribute.equalsIgnoreCase("lastStatusCheck")) {
			value = lastStatusCheck;
		} else if (attribute.equalsIgnoreCase("blockedCount")) {
			value = blockedCount;
		} else if (attribute.equalsIgnoreCase("visible")) {
			value = visible;
		} else if (attribute.equalsIgnoreCase("remoteSubmitted")) {
			value = remoteSubmitted;
		} else if (attribute.equalsIgnoreCase("remoteStarted")) {
			value = remoteStarted;
		} else if (attribute.equalsIgnoreCase("remoteEnded")) {
			value = remoteEnded;
		} else if (attribute.equalsIgnoreCase("ended")) {
			value = ended ;
		} else if (attribute.equalsIgnoreCase("inputs")) {
			value = getInputsAsJsonObject().toString();
		} else {
			throw new JobException("Unrecognized job attribute " + attribute);
		}
		
		return value;
	}

	/**
	 * Calculates when a running job should have completed by.
	 * 
	 * @return
	 */
	public Date calculateExpirationDate() 
	{
		// if it's got an end time, return that
		if (getEnded() != null) 
		{
			return new DateTime(getEnded()).toDate();
		}
		else 
		{
			DateTime jobExpirationDate = null;
		
			// use when job actually started if available
			if (getRemoteStarted() != null) 
			{
				jobExpirationDate = new DateTime(getRemoteStarted());
			} 
			// otherwise use the worst case start time
			else
			{
				// created + 7 days to stage inputs + 7 days to submit
				jobExpirationDate = new DateTime(getCreated()).plusDays(7).plusDays(7);
			}
			
			// now adjust for run time. When no upper limit is given, use the max queue time
			if (StringUtils.isEmpty(hoursToRunTime(getMaxHours())))
			{
				// max queue length is 1000 hrs ~ 42 days
				return jobExpirationDate.plusHours(1000).toDate(); 
			} 
			// if we have a max run time, parse that and add it to the start time
			else 
			{
				String[] runTimeTokens = hoursToRunTime(getMaxHours()).split(":");
				
				if (runTimeTokens.length > 2) {
					jobExpirationDate = jobExpirationDate.plusHours(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[0]), 0));
					jobExpirationDate = jobExpirationDate.plusMinutes(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[1]), 0));
					jobExpirationDate = jobExpirationDate.plusSeconds(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[2]), 0));
				}
				else if (runTimeTokens.length == 2) 
				{
					jobExpirationDate = jobExpirationDate.plusMinutes(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[0]), 0));
					jobExpirationDate = jobExpirationDate.plusSeconds(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[1]), 0));
				}
				else if (runTimeTokens.length == 1) {
					jobExpirationDate = jobExpirationDate.plusSeconds(NumberUtils.toInt(ServiceUtils.trimLeadingZeros(runTimeTokens[0]), 0));
				}
				
				// give a buffer of 1 day1 just in case of something weird going on
				return jobExpirationDate.plusDays(1).toDate();
			}
		}
	}
	
	public String toString() {
		return String.format("%s - %s - %s", 
				getUuid(),
				getStatus().name(),
				getLastUpdated());
	}

	/**
	 * Serializes to json with notifications embedded in hypermedia response.
	 * @param notifications
	 * @return
	 * @throws IOException 
	 */
	public String toJsonWithNotifications(List<Notification> notifications)
	throws JsonProcessingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode notifArray = mapper.createArrayNode();
		
		if (notifications != null) {
			String baseNotificationUrl = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE);
			for(Notification n: notifications) {
				notifArray.add(mapper.createObjectNode()
						.put("href", baseNotificationUrl + n.getUuid())
						.put("title", n.getEvent()));
			}
		}
				
		ObjectNode hypermedia = getHypermedia();
		hypermedia.put("notification", notifArray);
		
		ObjectNode json = mapper.createObjectNode()
				.put("id", uuid)
				.put("name", name)
				.put("tenantId", tenantId)
				.put("tenantQueue", tenantQueue)
				.put("status", status.name())
				.put("lastStatusMessage", getLastStatusMessage())
				.put("accepted", new DateTime(accepted).toString())
			    .put("created", getCreated() == null ? null : new DateTime(created).toString())
			    .put("ended", getEnded() == null ? null : new DateTime(getEnded()).toString())
				.put("lastUpdated", new DateTime(lastUpdated).toString())
				.put("owner", owner)
				.put("roles", roles)
				.put("systemId", systemId)
				.put("appId", appId)
				.put("appUuid", appUuid)
				.put("workPath", workPath)
				.put("archive", archive)
				.put("archivePath", archivePath)
			    .put("archiveSystem", archiveSystem)
			    .put("nodeCount", nodeCount)
			    .put("processorsPerNode", processorsPerNode)
			    .put("memoryPerNode", memoryPerNode)
			    .put("maxHours", maxHours);
			json.set("inputs", getInputsAsJsonObject());
			json.set("parameters", getParametersAsJsonObject());
				
			json.put("remoteJobId", remoteJobId)
			    .put("schedulerJobId", schedulerJobId)
			    .put("remoteQueue", remoteQueue)
			    .put("remoteSubmitted", getRemoteSubmitted() == null ? null : new DateTime(getRemoteSubmitted()).toString())
			    .put("remoteStarted", getRemoteStarted() == null ? null : new DateTime(getRemoteStarted()).toString())
		        .put("remoteEnded", getRemoteEnded() == null ? null : new DateTime(getRemoteEnded()).toString())
		        .put("remoteOutcome", remoteOutcome.toString())
				.put("submitRetries", submitRetries)
				.put("remoteStatusChecks",remoteStatusChecks)
				.put("failedStatusChecks",failedStatusChecks)
				.put("lastStatusCheck", getLastStatusCheck() == null? null : new DateTime(lastStatusCheck).toString())
				.put("blockedCount", blockedCount)
				.put("visible", visible);
					
		json.set("_links", hypermedia);
	    	
		return json.toString();
	}
	
	/**
	 * Equality check for collection existence checks when marshalling between {@link Job} and 
	 * {@link JobDTO} objects.
	 * @param dto a marshalled version of a {@link Job} object.
	 * @return true if the uuid match. false otherwise.
	 */
	public boolean equals(JobDTO dto) {
		return (dto != null && StringUtils.equals(getUuid(), dto.getUuid()));
	}
	
	 /* ---------------------------------------------------------------------------- */
	  /* runTimeToHours:                                                              */
	  /* ---------------------------------------------------------------------------- */
	  /** Convert the HH:MM:SS formatted user input for maximum runtime to a number of 
	   * hours.  When translation succeeds, the resulting hours are rounded.  When the
	   * translation cannot complete, or when the time input is null, a default value of 
	   * 0 is returned.  Later processing will assign a non-zero maximum run time if 
	   * necessary. 
	   * 
	   * @param time as a string in HH:MM:SS format or null
	   * @return the number of hours as a float
	   */
	  public static float runTimeToHours(String time)
	  {
	      // Return 1 if the input is not valid,
	      // otherwise parse the string.
	      if (StringUtils.isBlank(time)) return 0;
	      String[] components = time.split(":");
	      
	      // Initialize the hours accumulator.
	      float temp = 0;
	      
	      // The first component is always represents hours.
	      if (components.length > 0)
	          try {temp = Integer.valueOf(components[0]);}
	              catch (NumberFormatException e) {
	                  String msg = "INVALID_PARAMETER : Job runTimeToHours time [0]"+ time;
	                  log.warn(msg);
	                  return 0; // there's no point in continuing
	              }
	          
	      // The second component is always represents minutes.
	      if (components.length > 1)
	          try {temp += (Integer.valueOf(components[1]) / 60f);}
	              catch (NumberFormatException e) {
	                  String msg = "INVALID_PARAMETER: Job runTimeToHours: time[1]" + time ;
	                  log.warn(msg);
	              }
	      
	      // The third component is always represents seconds.
	      // (This is precision taken beyond what's necessary,
	      // but it's kinda fun...)
	      if (components.length > 2)
	          try {temp += (Integer.valueOf(components[2]) / 3600f);}
	              catch (NumberFormatException e) {
	                  String msg = "ALOE_INVALID_PARAMETER: Job runTimeToHours: time[2]" +time;
	                  log.warn(msg);
	              }
	      
	      // Let's either return the default or create something reasonable by rounding.
	      float hours = 0;
	      if (Float.isFinite(temp)) 
	        if (temp == 0) {/* nop */}
	          else if (temp <= .1f) hours = .1f; 
	            else hours = Precision.round(temp, 2, BigDecimal.ROUND_HALF_UP);
	      
	      return hours;
	  }

	
	/* ---------------------------------------------------------------------------- */
	  /* hoursToRunTime:                                                              */
	  /* ---------------------------------------------------------------------------- */
	  /** Convert hours expressed as a float into an HH:MM:SS formatted string.  Each
	   * component is padded with a leading zero if necessary.  The current 
	   * implementation always assigns "00" to the seconds component.  The hours
	   * component is at least two digit, but can be longer depending on the 
	   * magnitude of the input.
	   * 
	   * @param the number of hours as a float
	   * @return time as a string in HH:MM:SS format
	   */
	public static String hoursToRunTime(float hours)
	  {
	      // We preserve zero values.
	      if (hours == 0) return "00:00:00";
	      
	      // Always return something.
	      if ((hours < 0) || Float.isInfinite(hours) || Float.isNaN(hours)) 
	          return Job.DEFAULT_MAX_HOURS_STRING_ENCODED;
	      
	      // Get the integral and fractional parts of the float.
	      long  iPart = (long) hours;
	      float fPart = hours - iPart;
	      
	      // Construct the HH:MM:SS string with the minutes rounded to an integer 
	      // using the fractional part and seconds always zero.
	      String s = String.format("%02d", iPart);
	      s += ":" + String.format("%02d", (int)(Precision.round(fPart*60f, 2))); 
	      s += ":00";
	      return s;
	  }
	  
	
}

	
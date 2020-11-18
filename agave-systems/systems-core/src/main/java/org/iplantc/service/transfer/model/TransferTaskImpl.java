package org.iplantc.service.transfer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.ParamDef;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.AgaveUriUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.events.TransferTaskEventProcessor;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;

import javax.persistence.*;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static javax.persistence.GenerationType.IDENTITY;
/**
 * Container class to hold records of current and scheduled transfers.
 * Transfers are fine-grained entities and can be reused throughout
 * the api by different services needing to move data. Ideally they will
 * be executed by a pool of transfer worker processes, but it is conceivable
 * that a syncronous transfer may occur, in which case the parent process
 * should update the task themself.
 * @author dooley
 */
@Entity
@Table(name = "transfertasks")
@FilterDef(name="transferTaskTenantFilter", parameters=@ParamDef(name="tenantId", type="string"))
@Filters(@Filter(name="transferTaskTenantFilter", condition="tenant_id=:tenantId"))
public class TransferTaskImpl implements TransferTask {
    
    private static final Logger log = Logger.getLogger(TransferTaskImpl.class);
    
	private Long id;
	private String source;
	private String dest;
	private String owner;
	private String eventId;
	private int attempts = 0;


	private TransferStatusType status = TransferStatusType.QUEUED;
	private long totalSize = 0;
	private long totalFiles = 0;
	private long totalSkippedFiles = 0;
	private long bytesTransferred = 0;
	private double transferRate = 0;
	private String tenantId;
	private TransferTaskImpl parentTask;
	private TransferTaskImpl rootTask;
	private Instant startTime;
	private Instant endTime;
	private Instant created = Instant.now();
	private Instant lastUpdated = Instant.now();
	private String uuid;
	private Integer version = 0;
	
	public TransferTaskImpl() {
		tenantId = TenancyHelper.getCurrentTenantId();
		setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
	}

	public TransferTaskImpl(String source, String dest)
	{
		this();
		this.source = source;
		this.dest = dest;
	}
	
	public TransferTaskImpl(String source, String dest, String owner, TransferTaskImpl parentTask, TransferTaskImpl rootTask)
	{
		this(source, dest);
		this.parentTask = parentTask;
		this.rootTask = rootTask;
		this.owner = owner;
	}

	/**
	 * @return the id
	 */
	@Id
	@GeneratedValue(strategy = IDENTITY)
	@Column(name = "id", unique = true, nullable = false)
	public Long getId()
	{
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Long id)
	{
		this.id = id;
	}

	/**
	 * @return the attempts
	 */
	@Column(name = "attempts")
	public int getAttempts()
	{
		return attempts;
	}

	/**
	 * @param attempts the attempts to set
	 */
	public void setAttempts(int attempts)
	{
		this.attempts = attempts;
	}

	/**
	 * @return the totalSize
	 */
	@Column(name = "total_size")
	public long getTotalSize()
	{
		return totalSize;
	}

	/**
	 * @param totalSize the totalSize to set
	 */
	public void setTotalSize(long totalSize)
	{
		this.totalSize = totalSize;
	}

	/**
	 * @return the bytesTransferred
	 */
	@Column(name = "bytes_transferred")
	public long getBytesTransferred()
	{
		return bytesTransferred;
	}

	/**
	 * @param bytesTransferred the bytesTransferred to set
	 */
	public void setBytesTransferred(long bytesTransferred)
	{
		this.bytesTransferred = bytesTransferred;
	}

	/**
	 * @return the parentTaskId
	 */
	@OneToOne(fetch = FetchType.EAGER)
	public TransferTaskImpl getParentTask()
	{
		return parentTask;
	}

	/**
	 * @param parentTask the parentTaskId to set
	 */
	public void setParentTask(TransferTaskImpl parentTask)
	{
		this.parentTask = parentTask;
	}

	/**
	 * @return the rootTask
	 */
	@OneToOne(fetch = FetchType.EAGER)
	public TransferTaskImpl getRootTask()
	{
		return rootTask;
	}

	/**
	 * @param rootTask the rootTask to set
	 */
	public void setRootTask(TransferTaskImpl rootTask)
	{
		this.rootTask = rootTask;
	}

	/**
	 * @return the source
	 */
	@Override
	@Column(name = "source", nullable=false, length=2048)
	public String getSource()
	{
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(String source)
	{
		this.source = source;
	}

	/**
	 * @return the dest
	 */
	@Override
	@Column(name = "dest", nullable=false, length=2048)
	public String getDest()
	{
		return dest;
	}

	/**
	 * @param dest the dest to set
	 */
	public void setDest(String dest)
	{
		this.dest = dest;
	}

	/**
	 * @return the owner
	 */
	@Override
	@Column(name = "owner", nullable = false, length = 32)
	public String getOwner()
	{
		return owner;
	}

	/**
	 * @param owner the owner to set
	 */
	public void setOwner(String owner)
	{
		this.owner = owner;
	}

	/**
	 * @return the eventId
	 */
	@Column(name = "event_id", nullable=true, length=255)
	public String getEventId()
	{
		return eventId;
	}

	/**
	 * @param eventId the eventId to set
	 */
	public void setEventId(String eventId)
	{
		this.eventId = eventId;
	}

	/**
	 * @return the status
	 */
	@Enumerated(EnumType.STRING)
	@Column(name = "status", length = 16)
	public TransferStatusType getStatus()
	{
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(TransferStatusType status)
	{  
		this.status = status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(String status)
	{
		this.status = TransferStatusType.valueOf(status);
	}

	/**
	 * @return the startTime
	 */
	@Column(name = "start_time", length = 16)
	public Instant getStartTime()
	{
		return startTime;
	}

	/**
	 * @param startTime the startTime to set
	 */
	public void setStartTime(Instant startTime)
	{
		this.startTime = startTime;
	}

	/**
	 * @return the endTime
	 */
	@Column(name = "end_time", length = 16)
	public Instant getEndTime()
	{
		return endTime;
	}

	/**
	 * @param endTime the endTime to set
	 */
	public void setEndTime(Instant endTime)
	{
		this.endTime = endTime;
	}

	/**
	 * @return the transferRate
	 */
	@Column(name = "transfer_rate")
	public double getTransferRate()
	{
		return transferRate;
	}

	/**
	 * @param transferRate the transferRate to set
	 */
	public void setTransferRate(double transferRate)
	{
		this.transferRate = transferRate;
	}
	
	/**
	 * Calculates the transfer rate by using the bytes transferred divided by the
	 * elapsed time in seconds.
	 *
	 * @return transfer rate in bytes per second
	 */
	public double calculateTransferRate()
	{
		Instant start = getStartTime() == null ? getCreated() : getStartTime();

		Instant end = (getEndTime() == null || getStatus() == TransferStatusType.TRANSFERRING) ? Instant.now() : getEndTime();
		double milliseconds = end.toEpochMilli() - start.toEpochMilli();
		if (milliseconds > 0) {
			return getBytesTransferred() / (milliseconds/1000.0);
		} else {
			return (double)0.0;
		}
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

	/**
	 * @return uuid for this transfer task
	 */
	@Column(name = "uuid", nullable = false, length = 255, unique=true)
	public String getUuid() {
		return uuid;
	}

	/**
	 * @param uuid
	 */
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	/**
	 * @return the created
	 */
	@Column(name = "created", nullable = false, length = 19)
	public Instant getCreated()
	{
		return created;
	}

	/**
	 * @param created the created to set
	 */
	public void setCreated(Instant created)
	{
		this.created = created;
	}

	/**
	 * @return the lastUpdated
	 */
	@Column(name = "last_updated", nullable = false, length = 19)
	public Instant getLastUpdated()
	{
		return lastUpdated;
	}

	/**
	 * @param lastUpdated the lastUpdated to set
	 */
	public void setLastUpdated(Instant lastUpdated)
	{
		this.lastUpdated = lastUpdated;
	}
	
	/**
	 * @return the totalFiles
	 */
	@Column(name = "total_files", nullable=false)
	public long getTotalFiles()
	{
		return totalFiles;
	}

	/**
	 * @param totalFiles the totalFiles to set
	 */
	public void setTotalFiles(long totalFiles)
	{
		this.totalFiles = totalFiles;
	}

	/**
	 * @return the totalSkippedFiles
	 */
	@Column(name = "total_skipped", nullable=false)
	public long getTotalSkippedFiles()
	{
		return totalSkippedFiles;
	}

	/**
	 * @param totalSkippedFiles the skippedFiles to set
	 */
	public void setTotalSkippedFiles(long totalSkippedFiles)
	{
		this.totalSkippedFiles = totalSkippedFiles;
	}
	
	/**
	 * @return the version
	 */
	@Version
    @Column(name="OPTLOCK")
    public Integer getVersion() {
		return version;
	}
	
	/**
	 * @param version the current version
	 */
	public void setVersion(Integer version) {
		this.version = version;
	}

	/**
	 * Formats bytes into human readable string value
	 * @param memoryLimit in bytes
	 * @return
	 */
	public static String formatMaxMemory(Long memoryLimit) {
	    return FileUtils.byteCountToDisplaySize(memoryLimit);
	}
	
//	/**
//     * Returns a list of TransferEvent in the history of this transfer task.
//     * 
//     * @return
//     */
//    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "transferTask", fetch=FetchType.LAZY, orphanRemoval=true)
//    public List<TransferEvent> getEvents() {
//        return events;
//    }
//    
//    /**
//     * @param events
//     */
//    public void setEvents(List<TransferEvent> events) {
//        this.events = events;
//    }
//    
//    /**
//     * Adds an event to the history of this transfer task. This will automatically
//     * be saved with the transfer task when the transfer task is persisted.
//     * 
//     * @param event
//     */
//    public void addEvent(TransferStatusType status, String message)
//    {
//        addEvent(new TransferEvent(this, status, message, owner));
//    }   
    
    /**
     * Adds an event to the history of this transfer task. This will automatically
     * be saved with the transfer task when the transfer task is persisted.
     * 
     * @param event
     */
    @Transient
    public void addEvent(TransferTaskEvent event) {
        event.setEntity(getUuid());
        TransferTaskEventProcessor eventProcessor = 
    			new TransferTaskEventProcessor();
    	eventProcessor.processTransferTaskEvent(this, event);
    	
//        try {
//        	
//			
//		} catch (EntityEventPersistenceException e) {
//			log.error("Failed to save " + event.getStatus() 
//					+ " event for transfer " + event.getEntity(), e);
//		}
//        
//        NotificationManager.process(getUuid(), event.getStatus(), event.getCreatedBy());     
    }
    
    public String toJSON() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        try
        {   
            json.put("id", getUuid())
                .put("attempts", getAttempts())
                .put("bytesTransferred", getBytesTransferred())
                .put("created", getCreated().toString())
                .put("dest", getDest())
                .put("endTime", getEndTime() == null ? null : getEndTime().toString())
                .put("lastUpdated", getLastUpdated() == null ? getCreated().toString() : getLastUpdated().toString())
                .put("owner", getOwner())
                .put("parentTask", getParentTask() == null ? null : getParentTask().getUuid())
                .put("rootTask", getRootTask() == null ? null : getRootTask().getUuid())
                .put("source", getSource())
                .put("startTime", getStartTime() == null ? null : getStartTime().toString())
                .put("status", getStatus().name())
                .put("totalFiles", getTotalFiles())
                .put("totalSize", getTotalSize())
                .put("totalSkippedFiles", getTotalSkippedFiles());
                
                ObjectNode linksObject = mapper.createObjectNode();
                
                linksObject.put("self", (ObjectNode)mapper.createObjectNode()
                    .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid()));
                
                linksObject.put("source", (ObjectNode)mapper.createObjectNode()
                        .put("href", resolveEndpointToUrl(getSource())));
                
                linksObject.put("dest", (ObjectNode)mapper.createObjectNode()
                        .put("href", resolveEndpointToUrl(getDest())));
                
                if (getParentTask() != null) {
                    linksObject.put("parentTask", mapper.createObjectNode()
                        .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getParentTask().getUuid()));
                }
                
                if (getRootTask() != null) {
                    linksObject.put("rootTask", mapper.createObjectNode()
                        .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getRootTask().getUuid()));
                }
                
//                if (isParent()) {
                    linksObject.put("childTasks", (ObjectNode)mapper.createObjectNode()
                        .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid() + "/subtasks"));
//                }
                
                linksObject.put("notifications", mapper.createObjectNode()
                        .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE) + "?associatedUuid=" + getUuid()));
                
                linksObject.put("owner", mapper.createObjectNode()
                        .put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + getOwner()));
                
                json.put("_links", linksObject);
        }
        catch (Exception e) {
        	log.error("Error producing JSON output for transfer task " + getUuid());
        }

        return json.toString();
        
    }
    
    /**
     * Converts an Agave URL to standard HTTP URL for reference in the hypermedia links 
     * included in the JSON response. If the link is not an Agave url, it is returned as is.
     * 
     * @param endpoint to resolve. Can be of the form agave:// or http(s)://
     * @return String resolved url
     */
    public String resolveEndpointToUrl(String endpoint) 
    {
        URI endpointUri = URI.create(endpoint);
        try {
            if (AgaveUriUtil.isInternalURI(endpointUri)) {
                if (StringUtils.equalsIgnoreCase(endpointUri.getScheme(), "agave")) {
                    return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE) 
                            + "/media/system/" + endpointUri.getHost() + endpointUri.getPath();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        
        return endpoint;
    }

    public String toString() 
    {
        String status = getStatus().name();
        if (!getStatus().isCancelled()) {
            if (getTotalSize() == 0) {
                status += " - ?%";
            } else {
                status += " - " + Math.floor(getBytesTransferred() / getTotalSize());
            }
        }
         
        return String.format("[%s] %s -> %s - %s", status, getSource(), getDest(), getUuid());
    }
	
	/**
	 * Parses numbers from abbreviated human readable form into human readable
	 * integers in gb.
	 * <ul>
	 * <li>1XB => 1000000000</li>
	 * <li>1EB => 1000000000</li>
	 * <li>1PB => 1000000</li>
	 * <li>1TB => 1000</li>
	 * <li>1GB => 1</li>
	 * </ul>
	 * @param memoryLimit
	 * @return
	 * @throws NumberFormatException
	 */
	public static long parseMaxMemory(String memoryLimit) throws NumberFormatException
	{
		if (memoryLimit == null) {
			throw new NumberFormatException("Cannot parse a null value.");
		} 
		
		memoryLimit = memoryLimit.toUpperCase()
				.replaceAll(",", "")
				.replaceAll(" ", "");
		
		long returnValue = -1;
	    Pattern patt = Pattern.compile("([\\d.-]+)([EPTGM]B)", Pattern.CASE_INSENSITIVE);
	    Matcher matcher = patt.matcher(memoryLimit);
	    Map<String, Integer> powerMap = new HashMap<String, Integer>();
	    powerMap.put("XB", 3);
	    powerMap.put("PB", 2);
	    powerMap.put("TB", 1);
	    powerMap.put("GB", 0);
	    if (matcher.find()) {
			String number = matcher.group(1);
			int pow = powerMap.get(matcher.group(2).toUpperCase());
			BigDecimal bytes = new BigDecimal(number);
			bytes = bytes.multiply(BigDecimal.valueOf(1024).pow(pow));
			returnValue = bytes.longValue();
	    } else {
	    	throw new NumberFormatException("Invalid number format.");
	    }
	    return returnValue;
	}
	
	@Transient
	public void updateSummaryStats(TransferTaskImpl transferTask)
	{
		if (transferTask != null) {
			this.bytesTransferred += transferTask.getBytesTransferred();
			this.totalFiles += transferTask.getTotalFiles();
			this.totalSize += transferTask.getTotalSize();
			this.totalSkippedFiles += transferTask.getTotalSkippedFiles();
			this.lastUpdated = Instant.now();
			this.updateTransferRate();
		}
	}
}

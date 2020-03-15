package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.apache.log4j.Logger;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;

import javax.persistence.*;
import java.time.Instant;

import static javax.persistence.GenerationType.IDENTITY;


/**
 * Container class to hold records of current and scheduled transfers.
 * Transfers are fine-grained entities and can be reused throughout
 * the api by different services needing to move data. Ideally they will
 * be executed by a pool of transfer worker processes, but it is conceivable
 * that a syncronous transfer may occur, in which case the parent process
 * should upInstant the task themself.
 * 
 * @author dooley
 *
 */
public class TransferTask {
    
    private static final Logger log = Logger.getLogger(TransferTask.class);
    
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
	private String parentTaskId;
	private String rootTaskId;
	private Instant startTime;
	private Instant endTime;
	private Instant created = Instant.now();
	private Instant lastUpdated = Instant.now();
	private String uuid;
	
	public TransferTask() {
		tenantId = TenancyHelper.getCurrentTenantId();
		setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
	}

	public TransferTask(JsonObject json) {
		this();
		this.setAttempts(json.getInteger("attempts"));
		this.setBytesTransferred(json.getInteger("bytesTransferred"));
		this.setCreated(json.getInstant("created"));
		this.setDest(json.getString("dest"));
		this.setEndTime(json.getInstant("endTime"));
		this.setEventId(json.getString("eventId"));
		this.setLastUpdated(json.getInstant("lastUpdated"));
		this.setOwner(json.getString("owner"));
		this.setSource(json.getString("source"));
		this.setStartTime(json.getInstant("startTime"));
		this.setStatus(TransferStatusType.valueOf(json.getString("status")));
		this.setTenantId(json.getString("tenantId"));
		this.setTotalSize(json.getLong("totalSize", 0L));
		this.setTransferRate(json.getDouble("transferRate", 0D));
		this.setParentTaskId(json.getString("parentTask"));
		this.setRootTaskId(json.getString("rootTask"));
		this.setUuid(json.getString("uuid"));
		this.setTotalFiles(json.getInteger("totalFiles", 0));
		this.setTotalSkippedFiles(json.getInteger("totalSkippedFiles", 0));
	}

	public TransferTask(String source, String dest)
	{
		this();
		this.source = source;
		this.dest = dest;
	}
	
	public TransferTask(String source, String dest, String owner, String parentTaskId, String rootTaskId)
	{
		this(source, dest);
		this.parentTaskId = parentTaskId;
		this.rootTaskId = rootTaskId;
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
	@Column(name = "parent_task", nullable=false, length=64)
	public String getParentTaskId()
	{
		return parentTaskId;
	}

	/**
	 * @param parentTaskId the parentTaskId to set
	 */
	public void setParentTaskId(String parentTaskId)
	{
		this.parentTaskId = parentTaskId;
	}

	/**
	 * @return the rootTask
	 */
	@Column(name = "root_task", nullable=false, length=64)
	public String getRootTaskId()
	{
		return rootTaskId;
	}

	/**
	 * @param rootTaskId the rootTaskId to set
	 */
	public void setRootTaskId(String rootTaskId)
	{
		this.rootTaskId = rootTaskId;
	}

	/**
	 * @return the source
	 */
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
	@Enumerated(EnumType.STRING)
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
	@Temporal(TemporalType.TIMESTAMP)
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
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "last_Updated", nullable = false, length = 19)
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


	public String toJSON() {
        JsonObject json = new JsonObject();
        try
        {   
            json.put("id", getUuid())
                .put("attempts", getAttempts())
                .put("bytesTransferred", getBytesTransferred())
                .put("created", getCreated())
                .put("dest", getDest())
                .put("endTime", getEndTime())
                .put("lastUpdated", getLastUpdated())
                .put("owner", getOwner())
                .put("parentTask", getParentTaskId())
                .put("rootTask", getRootTaskId())
                .put("source", getSource())
                .put("startTime", getStartTime())
                .put("status", getStatus().name())
                .put("totalFiles", getTotalFiles())
                .put("totalSize", getTotalSize())
                .put("totalSkippedFiles", getTotalSkippedFiles())
				.put("_links", new JsonObject()
						.put("self", new JsonObject()
                    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid()))
                		.put("source", new JsonObject()
                        	.put("href", TransferRateHelper.resolveEndpointToUrl(getSource())))
						.put("dest", new JsonObject()
							.put("href", TransferRateHelper.resolveEndpointToUrl(getDest())))
                    	.put("parentTask", new JsonObject()
                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getParentTaskId()))
                		.put("rootTask", new JsonObject()
                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getRootTaskId()))
                    	.put("childTasks", new JsonObject()
                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid() + "/subtasks"))
						.put("notifications", new JsonObject()
                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE) + "?associatedUuid=" + getUuid()))
                		.put("owner", new JsonObject()
                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + getOwner())));
        }
        catch (Exception e) {
        	log.error("Error producing JSON output for transfer task " + getUuid());
        }

        return json.toString();
        
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
}

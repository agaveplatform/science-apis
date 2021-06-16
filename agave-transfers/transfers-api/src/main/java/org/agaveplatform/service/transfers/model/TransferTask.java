package org.agaveplatform.service.transfers.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;


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
@JsonSerialize(using = AgaveResourceSerializer.class)
@DataObject
public class TransferTask implements org.iplantc.service.transfer.model.TransferTask {
    
    private static final Logger log = LoggerFactory.getLogger(TransferTask.class);

	private Long id;
	private String source;
	private String dest;
	private String owner;
	private String eventId;
	private int attempts = 0;
	private Instant lastAttempt = null;
	private Instant nextAttempt = null;
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
		setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
	}

	public TransferTask(JsonObject json) {
		this();

		this.setId(json.getLong("id"));

		this.setAttempts(json.getInteger("attempts", 0));

		if (json.containsKey("bytes_transferred")) {
			this.setBytesTransferred(json.getLong("bytes_transferred", 0L));
		} else {
			this.setBytesTransferred(json.getLong("bytesTransferred", 0L));
		}

		this.setCreated(json.getInstant("created", Instant.now()));

		this.setDest(json.getString("dest"));

		if (json.containsKey("end_time")) {
			this.setEndTime(json.getInstant("end_time", null));
		} else {
			this.setEndTime(json.getInstant("endTime", null));
		}

		if (json.containsKey("event_id")) {
			this.setEventId(json.getString("event_id", null));
		} else {
			this.setEventId(json.getString("eventId", null));
		}

		if (json.containsKey("last_updated")) {
			this.setLastUpdated(json.getInstant("last_updated"));
		} else {
			this.setLastUpdated(json.getInstant("lastUpdated"));
		}

		if (json.containsKey("last_attempt")) {
			this.setLastAttempt(json.getInstant("last_attempt"));
		} else {
			this.setLastAttempt(json.getInstant("lastAttempt"));
		}

		if (json.containsKey("next_attempt")) {
			this.setNextAttempt(json.getInstant("next_attempt"));
		} else {
			this.setNextAttempt(json.getInstant("nextAttempt"));
		}

		this.setOwner(json.getString("owner", null));

		this.setSource(json.getString("source"));

		if (json.containsKey("start_time")) {
			this.setStartTime(json.getInstant("start_time"));
		} else {
			this.setStartTime(json.getInstant("startTime"));
		}

		this.setStatus(TransferStatusType.valueOf(json.getString("status")));

		if (json.containsKey("tenant_id")) {
			this.setTenantId(json.getString("tenant_id"));
		}

		if (json.containsKey("total_size")) {
			this.setTotalSize(json.getLong("total_size", 0L));
		} else {
			this.setTotalSize(json.getLong("totalSize", 0L));
		}

		if (json.containsKey("transfer_rate")) {
			this.setTransferRate(json.getDouble("transfer_rate", 0D));
		} else {
			this.setTransferRate(json.getDouble("transferRate", 0D));
		}

		if (json.containsKey("parent_task")) {
			this.setParentTaskId(json.getString("parent_task", null));
		} else {
			this.setParentTaskId(json.getString("parentTask", null));
		}

		if (json.containsKey("root_task")) {
			this.setRootTaskId(json.getString("root_task", null));
		} else {
			this.setRootTaskId(json.getString("rootTask", null));
		}

		this.setUuid(json.getString("uuid"));

		if (json.containsKey("total_files")) {
			this.setTotalFiles(json.getLong("total_files", 0L));
		} else {
			this.setTotalFiles(json.getLong("totalFiles", 0L));
		}

		if (json.containsKey("total_skipped_files")) {
			this.setTotalSkippedFiles(json.getLong("total_skipped_files", 0L));
		} else {
			this.setTotalSkippedFiles(json.getLong("totalSkippedFiles", 0L));
		}
	}

	public TransferTask(String source, String dest, String tenantId)
	{
		this();
		this.source = source;
		this.dest = dest;
		setTenantId(tenantId);
	}
	
	public TransferTask(String source, String dest, String owner, String parentTaskId, String rootTaskId)
	{
		this(source, dest);
		this.parentTaskId = parentTaskId;
		this.rootTaskId = rootTaskId;
		this.owner = owner;
		setTenantId(tenantId);
	}

	public TransferTask(String source, String dest){
		this.source = source;
		this.dest = dest;
	}

	public TransferTask(String tenantId, String source, String dest, String owner, String parentTaskID, String rootTaskID )
	{
		this(source, dest, tenantId);
		this.parentTaskId = parentTaskID;
		this.rootTaskId = rootTaskId;
		this.owner = owner;
	}

	/**
	 * @return the id
	 */
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

	public void setStatusString(String status) {
		this.setStatus(TransferStatusType.valueOf(status));
	}

	/**
	 * @return the startTime
	 */
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
		return toJson().toString();
	}

	public JsonObject toJson() {
        JsonObject json = new JsonObject();
        try
        {   
            json.put("id", getId())
                .put("attempts", getAttempts())
                .put("created", getCreated())
                .put("dest", getDest())
				.put("source", getSource())
				.put("endTime", getEndTime())
                .put("lastUpdated", getLastUpdated())
				.put("lastAttempt", getLastAttempt())
				.put("nextAttempt", getNextAttempt())
                .put("owner", getOwner())
                .put("parentTask", getParentTaskId())
                .put("rootTask", getRootTaskId())
				.put("status", getStatus().name())
				.put("uuid", getUuid())
				.put("bytesTransferred", getBytesTransferred())
				.put("startTime", getStartTime())
				.put("totalFiles", getTotalFiles())
				.put("transferRate", getTransferRate())
				.put("totalSize", getTotalSize())
				.put("tenant_id", getTenantId())
				.put("totalSkippedFiles", getTotalSkippedFiles());
//				.put("_links", new JsonObject()
//						.put("self", new JsonObject()
//                    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid()))
//                		.put("source", new JsonObject()
//                        	.put("href", TransferRateHelper.resolveEndpointToUrl(getSource())))
//						.put("dest", new JsonObject()
//							.put("href", TransferRateHelper.resolveEndpointToUrl(getDest())))
//                    	.put("parentTask", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getParentTaskId()))
//                		.put("rootTask", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getRootTaskId()))
//                    	.put("childTasks", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid() + "/subtasks"))
//						.put("notifications", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_NOTIFICATION_SERVICE) + "?associatedUuid=" + getUuid()))
//                		.put("owner", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + getOwner())));
        }
        catch (Exception e) {
        	log.error("Error producing JSON output for transfer task " + getUuid());
        }

        return json;
        
    }

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TransferTask that = (TransferTask) o;
		return Objects.equals(getSource(), that.getSource()) &&
				Objects.equals(getDest(), that.getDest()) &&
				Objects.equals(getOwner(), that.getOwner()) &&
				getStatus() == that.getStatus() &&
				Objects.equals(getTenantId(), that.getTenantId()) &&
				Objects.equals(getUuid(), that.getUuid());
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, source, dest, owner, status, tenantId, uuid);
	}

	public String toString()
    {
    	return toJSON();
    }

	public Instant getLastAttempt() {
		return lastAttempt;
	}

	public void setLastAttempt(Instant lastAttempt) {
		this.lastAttempt = lastAttempt;
	}

	public Instant getNextAttempt() {
		return nextAttempt;
	}

	public void setNextAttempt(Instant nextAttempt) {
		this.nextAttempt = nextAttempt;
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
			return 0.0;
		}
	}
}

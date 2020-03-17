package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import java.time.Instant;

import static javax.persistence.GenerationType.IDENTITY;

public class TransferUpdate {

	private static final Logger log = LoggerFactory.getLogger(TransferUpdate.class);

	private Long id;
	private String owner;
	private int attempts = 0;
	private TransferStatusType status = TransferStatusType.QUEUED;
	private long totalSize = 0;
	private long totalFiles = 0;
	private long totalSkippedFiles = 0;
	private long bytesTransferred = 0;
	private String tenantId;
	private String transferTaskId;
	private Instant startTime;
	private Instant endTime;
	private Instant created = Instant.now();

	public TransferUpdate() {
		tenantId = TenancyHelper.getCurrentTenantId();
	}

	public TransferUpdate(JsonObject json) {
		this();
		this.setAttempts(json.getInteger("attempts"));
		this.setBytesTransferred(json.getInteger("bytes_transferred"));
		this.setCreated(json.getInstant("created"));
		this.setEndTime(json.getInstant("end_time"));
		this.setOwner(json.getString("owner"));
		this.setStartTime(json.getInstant("start_time"));
		this.setStatus(TransferStatusType.valueOf(json.getString("status")));
		this.setTenantId(json.getString("tenant_id"));
		this.setTotalSize(json.getLong("total_size", 0L));
		this.setTransferTaskId(json.getString("transfer_task"));
		this.setTotalFiles(json.getInteger("total_files", 0));
		this.setTotalSkippedFiles(json.getInteger("total_skipped", 0));
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
	@Column(name = "transfer_task", nullable=false, length=64)
	public String getTransferTaskId()
	{
		return transferTaskId;
	}

	/**
	 * @param transferTaskId the parentTaskId to set
	 */
	public void setTransferTaskId(String transferTaskId)
	{
		this.transferTaskId = transferTaskId;
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
			json.put("id", getId())
					.put("attempts", getAttempts())
					.put("bytesTransferred", getBytesTransferred())
					.put("created", getCreated())
					.put("endTime", getEndTime())
					.put("owner", getOwner())
					.put("startTime", getStartTime())
					.put("status", getStatus().name())
					.put("totalFiles", getTotalFiles())
					.put("totalSize", getTotalSize())
					.put("totalSkippedFiles", getTotalSkippedFiles())
					.put("transferTaskId", getTransferTaskId());
//					.put("_links", new JsonObject()
//						.put("self", new JsonObject()
//                    		.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid()))
//                		.put("transferTask", new JsonObject()
//							.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getTransferTaskId()))
//                    	.put("childTasks", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_TRANSFER_SERVICE) + getUuid() + "/subtasks"))
//                		.put("owner", new JsonObject()
//                        	.put("href", TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + getOwner())));
		}
		catch (Exception e) {
			log.error("Error producing JSON output for transfer update " + getTransferTaskId());
		}

		return json.toString();

	}

	public String toString()
	{
//		String status = getStatus().name();
//		if (!getStatus().isCancelled()) {
//			if (getTotalSize() == 0) {
//				status += " - ?%";
//			} else {
//				status += " - " + Math.floor(getBytesTransferred() / getTotalSize()) + "%";
//			}
//		}
		return String.format("%s [%s]:\n  bytes: %d\n  total: %d\n  files: %d\n  skipped: %d",
				getStatus().name(), getTransferTaskId(), getBytesTransferred(), getTotalSize(),
				getTotalFiles(), getTotalSkippedFiles());
	}
}

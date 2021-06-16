package org.iplantc.service.io.model;

import org.iplantc.service.transfer.model.TransferTask;

import java.util.Date;

/**
 * @deprecated
 * @see TransferTask instead
 */
@Deprecated
public interface QueueTask {

	/**
	 * @return
	 */
	Long getId();

	/**
	 * @param id
	 */
	void setId(Long id);

	/**
	 * @return the logicalFile
	 */
	LogicalFile getLogicalFile();

	/**
	 * @param logicalFile the logicalFile to set
	 */
	void setLogicalFile(LogicalFile logicalFile);

	/**
	 * Get username of the user who creatd the task
	 * @return
	 */
	String getOwner();

	/**
	 * Set username of the user who created this task
	 * @param owner
	 */
	void setOwner(String owner);

//	/**
//	 * @return the eventId
//	 */
//	public abstract String getEventId();
//
//	/**
//	 * @param eventId the eventId to set
//	 */
//	public abstract void setEventId(String eventId);

	/**
	 * @return the created
	 */
	Date getCreated();

	/**
	 * @param created the created to set
	 */
	void setCreated(Date created);

	/**
	 * @return the lastUpdated
	 */
	Date getLastUpdated();

	/**
	 * @param lastUpdated the lastUpdated to set
	 */
	void setLastUpdated(Date lastUpdated);


	/**
	 * Returns string value of enumerated status object.
	 * @return
	 */
	String getStatusAsString();
}
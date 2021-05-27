/**
 * 
 */
package org.iplantc.service.io.model.enumerations;

/**
 * @author dooley
 *
 */
public enum StagingTaskStatus
{
	STAGING_QUEUED("File/folder queued for staging"), 
	STAGING("Staging file/folder"), 
	STAGING_FAILED("Staging failed"), 
	STAGING_COMPLETED("Staging completed successfully");
	
	private final String description;
	
	StagingTaskStatus(String description) {
		this.description = description;
	}
	
	public String getDescription() {
		return description;
	}
}

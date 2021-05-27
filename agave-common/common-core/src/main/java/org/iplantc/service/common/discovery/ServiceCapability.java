package org.iplantc.service.common.discovery;

public interface ServiceCapability
{
	/**
	 * @return the id
	 */
    Long getId();

	/**
	 * @param id the id to set
	 */
    void setId(Long id);

	/**
	 * @return the tenantCode
	 */
    String getTenantCode();

	/**
	 * @param tenantCode the tenantCode to set
	 */
    void setTenantCode(String tenantCode);

	/**
	 * @return the apiName
	 */
    String getApiName();

	/**
	 * @param apiName the apiName to set
	 */
    void setApiName(String apiName);

	/**
	 * @return the activityType
	 */
    String getActivityType();

	/**
	 * @param activityType the activityType to set
	 */
    void setActivityType(String activityType);

	/**
	 * @return the username
	 */
    String getUsername();

	/**
	 * @param username the username to set
	 */
    void setUsername(String username);

	/**
	 * @return the groupName
	 */
    String getGroupName();

	/**
	 * @param groupName the groupName to set
	 */
    void setGroupName(String groupName);

	/**
	 * @return the definition
	 */
    String getDefinition();

	/**
	 * @param definition the definition to set
	 */
    void setDefinition(String definition);

	/**
	 * Whether the given {@link ServiceCapabilityImpl} allows the
	 * given behavior either through wildcard or exact matching.
	 * 
	 * @param {@link ServiceCapabilityImpl} 
	 * @return true if the this {@link ServiceCapabilityImpl} in any 
	 * way matches the provided {@link ServiceCapabilityImpl}
	 */
    boolean allows(ServiceCapability o);

	/**
	 * Whether the given {@link ServiceCapabilityImpl} is invalidated
	 * by this {@link ServiceCapabilityImpl}. This differs from the 
	 * {@link ServiceCapabilityImpl#allows(ServiceCapabilityImpl)} method in
	 * that this only returns true if an explicit invalidation 
	 * field is found. 
	 * 
	 * a.invalidates(b) does not imply a.allows(b)
	 * 
	 * @param {@link ServiceCapabilityImpl} 
	 * @return true if the this {@link ServiceCapabilityImpl} in any 
	 * way matches the provided {@link ServiceCapabilityImpl}
	 */
    boolean invalidates(ServiceCapability o);

	/**
     * @return the executionSystemId
     */
    String getExecutionSystemId();

    /**
     * @param executionSystemId the executionSystemId to set
     */
    void setExecutionSystemId(String executionSystemId);
    
    /**
     * @return the sourceSystemId
     */
    String getSourceSystemId();

    /**
     * @param sourceSystemId the sourceSystemId to set
     */
    void setSourceSystemId(String sourceSystemId);
    
    /**
     * @return the destSystemId
     */
    String getDestSystemId();

    /**
     * @param sourceSystemId the destSystemId to set
     */
    void setDestSystemId(String sourceSystemId);

    /**
     * @return the batchQueueName
     */
    String getBatchQueueName();

    /**
     * @param batchQueueName the batchQueueName to set
     */
    void setBatchQueueName(String batchQueueName);

}
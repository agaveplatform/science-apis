package org.iplantc.service.common.model;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Timestamp;
import java.util.Date;

public interface AgaveEntityEvent {

	/**
	 * @return the id
	 */
    Long getId();

	/**
	 * @param id
	 *            the id to set
	 */
    void setId(Long id);

	/**
	 * @return the softwareUuid
	 */
    String getEntity();

	/**
	 * @param entity
	 *            the uuid of the entity to set
	 */
    void setEntity(String entityUuid);

	/**
	 * @return the status
	 */
    String getStatus();

	/**
	 * @param status
	 *            the status to set
	 */
    void setStatus(String status);

	/**
	 * @return the username
	 */
    String getCreatedBy();

	/**
	 * @param username
	 *            the creator to set
	 */
    void setCreatedBy(String createdBy);

	/**
	 * @return the message
	 */
    String getDescription();

	/**
	 * @param description
	 *            the description to set
	 */
    void setDescription(String description);

	/**
	 * @return the ipAddress
	 */
    String getIpAddress();

	/**
	 * @param ipAddress
	 *            the ipAddress to set
	 */
    void setIpAddress(String ipAddress);

	/**
	 * @return the tenantId
	 */
    String getTenantId();

	/**
	 * @param tenantId
	 *            the tenantId to set
	 */
    void setTenantId(String tenantId);

	/**
	 * @return the created
	 */
    Date getCreated();

	/**
	 * @return the uuid
	 */
    String getUuid();

	/**
	 * @param uuid
	 *            the uuid to set
	 */
    void setUuid(String uuid);

	/**
	 * @param created
	 *            the created to set
	 */
    void setCreated(Timestamp created);

	ObjectNode getLinks();

}
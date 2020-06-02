package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import java.time.Instant;

import static javax.persistence.GenerationType.IDENTITY;

public class TransferTaskRequest {

	private static final Logger log = LoggerFactory.getLogger(TransferTaskRequest.class);

	private String source;
	private String dest;
	private String owner;
	private String tenantId;

	public TransferTaskRequest() {}

	public TransferTaskRequest(String source, String dest, String owner, String tenantId)
	{
		this();
		setDest(dest);
		setOwner(owner);
		setSource(source);
		setTenantId(tenantId);
	}

	public TransferTaskRequest(JsonObject json) {
		this();
		this.setDest(json.getString("dest"));
		this.setOwner(json.getString("owner"));
		this.setSource(json.getString("source"));
		this.setTenantId(json.getString("tenantId"));
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


	public String toString()
	{
		return String.format("[%s] %s -> %s - %s", getTenantId(), getSource(), getDest(), getOwner());
	}
}

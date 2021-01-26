/**
 *
 */
package org.iplantc.service.metadata.model;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONWriter;

import javax.persistence.*;
import java.util.Date;

/**
 * Class to represent individual shared permissions for jobs
 *
 * @author dooley
 *
 */
public class MetadataPermission {

	private String				username;
	private PermissionType		permission;
	private Date				lastUpdated = new Date();
	private String 				tenantId;
	private String				group;

	public MetadataPermission() {
		this.setTenantId(TenancyHelper.getCurrentTenantId());
	}

	public MetadataPermission(String username, PermissionType permissionType) throws MetadataException
	{
		this();
		setUsername(username);
		setPermission(permissionType);
	}

	/**
	 * @return the username
	 */
	@Column(name = "username", nullable = false, length = 32)
	public String getUsername()
	{
		return username;
	}

	/**
	 * @param username
	 *            the username to set
	 * @throws MetadataException if username is too long
	 */
	public void setUsername(String username) throws MetadataException
	{
		if (!StringUtils.isEmpty(username) && username.length() > 32) {
			throw new MetadataException("'permission.username' must be less than 32 characters");
		}

		this.username = username;
	}

	/**
	 * @return the permission
	 */
	@Enumerated(EnumType.STRING)
	@Column(name = "permission", nullable = false, length = 16)
	public PermissionType getPermission()
	{
		return permission;
	}

	/**
	 * @param permission
	 *            the permission to set
	 */
	public void setPermission(PermissionType permission)
	{
		this.permission = permission;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	/**
	 * @param lastUpdated
	 *            the lastUpdated to set
	 */
	public void setLastUpdated(Date lastUpdated)
	{
		this.lastUpdated = lastUpdated;
	}

	/**
	 * @return the lastUpdated
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "lastUpdated", nullable = false, length = 19)
	public Date getLastUpdated()
	{
		return lastUpdated;
	}

	public boolean canRead()
	{
		return permission.canRead();
	}

	public boolean canWrite()
	{
		return permission.canWrite();
	}

	public boolean canExecute()
	{
		return permission.canExecute();
	}

	public String getGroup() {return this.group;}

	public void setGroup(String group) {this.group = group;}

	public String toJSON(String uuid) throws JSONException
	{
		JSONWriter writer = new JSONStringer();
		writer.object()
				.key("username").value(username)
				.key("permission").object()
				.key("read").value(canRead())
				.key("write").value(canWrite())
				.endObject()
				.key("_links").object()
				.key("self").object()
				.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + uuid + "/pems/" + username)
				.endObject()
				.key("parent").object()
				.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + uuid)
				.endObject()
				.key("profile").object()
				.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + username)
				.endObject()
				.endObject()
				.endObject();

		return writer.toString();
	}

	public String toString()
	{
		return username + " " + permission;
	}

	public boolean equals(Object o)
	{
		if (o instanceof MetadataPermission) {
			return (
					( (MetadataPermission) o ).username.equals(username) &&
							( (MetadataPermission) o ).permission.equals(permission) );
		}
		return false;
	}

}

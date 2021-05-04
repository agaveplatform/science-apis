/**
 * 
 */
package org.iplantc.service.metadata.model;

import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONWriter;

import java.util.Date;

/**
 * Class to represent individual shared permissions for {@link MetadataSchemaItem}
 * @author dooley
 */
public class MetadataSchemaPermission {

//	@BsonProperty ("_id")
//	@BsonId
	private ObjectId id;
//	@BsonProperty ("schemaId")
	private String				schemaId;
//	@BsonProperty ("username")
	private String				username;
//	@BsonProperty ("permission")
	private PermissionType		permission;
//	@BsonProperty ("lastUpdated")
	private Date				lastUpdated = new Date();
//	@BsonProperty ("tenantId")
	private String 				tenantId;

	public MetadataSchemaPermission() {
		this.setTenantId(TenancyHelper.getCurrentTenantId());
	}

	public MetadataSchemaPermission(String schemaId, String username, PermissionType permissionType) throws MetadataException
	{
		this();
		setSchemaId(schemaId);
		setUsername(username);
		setPermission(permissionType);
	}

	/**
	 * @return the id
	 */
	public ObjectId getId()
	{
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(ObjectId id)
	{
		this.id = id;
	}

	/**
	 * @return the schemaId
	 */
	public String getSchemaId()
	{
		return schemaId;
	}

	/**
	 * @param schemaId
	 *            the jobId to set
	 */
	public void setSchemaId(String schemaId)
	{
		this.schemaId = schemaId;
	}

	/**
	 * @return the username
	 */
	public String getUsername()
	{
		return username;
	}

	/**
	 * @param username
	 *            the username to set
     * @throws MetadataException when username is empty or greater than 32 characters
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

	public String toJSON() throws JSONException 
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
	        		.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "schema/" + schemaId + "/pems/" + username)
	        	.endObject()
	        	.key("parent").object()
	        		.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "schema/" + schemaId)
	        	.endObject()
	        	.key("profile").object()
	        		.key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + username)
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
		if (o instanceof MetadataSchemaPermission) {
			return (
				( (MetadataSchemaPermission) o ).username.equals(username) &&
				( (MetadataSchemaPermission) o ).permission.equals(permission) );
		}
		return false;
	}

}

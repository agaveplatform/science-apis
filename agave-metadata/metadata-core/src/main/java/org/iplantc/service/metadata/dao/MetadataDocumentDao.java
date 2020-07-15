package org.iplantc.service.metadata.dao;

import org.iplantc.service.metadata.model.MetadataPermission;
import org.json.JSONObject;

import java.util.Date;
import java.util.List;

public class MetadataDocumentDao {
    private String uuid;
    private String tenantId;
    private String schemaId;
    private String internalUsername;
    private Date lastUpdated;
    private String name;
    private JSONObject value;
    private Date created;
    private String owner;
    private List<MetadataPermission> permissions;

    public MetadataDocumentDao(){ }


    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    public String getInternalUsername() {
        return internalUsername;
    }

    public void setInternalUsername(String internalUsername) {
        this.internalUsername = internalUsername;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JSONObject getValue() {
        return value;
    }

    public void setValue(JSONObject value) {
        this.value = value;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<MetadataPermission> getPermissions() {
        return permissions;
    }

    public void setPermissions(List<MetadataPermission> permissions) {
        this.permissions = permissions;
    }


}

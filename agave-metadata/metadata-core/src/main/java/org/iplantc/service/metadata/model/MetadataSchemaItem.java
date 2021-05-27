/**
 * 
 */
package org.iplantc.service.metadata.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.model.validation.constraints.ValidJsonSchema;
import org.iplantc.service.notification.model.Notification;

import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author dooley
 *
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class MetadataSchemaItem {
    
    @Id
    private String _id;
    
    @Length(max=16384)
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    @ValidJsonSchema
    private JsonNode schema;

    @NotNull(message = "Null tenantId attribute specified. Please provide a valid tenantId for this metadata item.")
    @Length(min = 1, max = 64, message = "Metadata tenant must be non-empty and less than 64 characters.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String tenantId;
    
    @NotNull(message = "Null internalUsername attribute specified. Please provide a valid internalUsername for this metadata item.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String internalUsername;

    @NotNull(message = "Null owner attribute specified. Please provide a valid owner for this metadata item.")
    @Length(min = 1, max = 32, message = "Metadata owner must be non-empty and less than 33 characters.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String owner;

    @Length(max = 64, message = "Metadata uuid must be a valid uuid.")
    @NotNull(message = "No uuid attribute specified. Please provide a valid uuid for this metadata item.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String uuid;

    @NotNull(message = "Null created attribute specified. Please provide a valid created for this metadata item.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private Date created;

    @NotNull(message = "Null lastUpdated attribute specified. Please provide a valid lastUpdated for this metadata item.")
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private Date lastUpdated;

    @JsonInclude(Include.NON_NULL)
    private String error;

    @JsonView({MetadataViews.Resource.Summary.class})
    private String _links;

    @JsonIgnore
    @JsonView({MetadataViews.Resource.Notifications.class, MetadataViews.Request.class})
    private final List<Notification> notifications = new ArrayList<Notification>();

    //KL
    @JsonIgnore
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private List<MetadataPermission> permissions = new ArrayList<MetadataPermission>();
    
    public MetadataSchemaItem() {
        this.uuid = new AgaveUUID(UUIDType.METADATA).toString();
        this.tenantId = TenancyHelper.getCurrentTenantId();
        this.created = new Date();
        this.lastUpdated = new Date();
    }

    /**
     * @return the schema
     */
    public synchronized JsonNode getSchema() {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public synchronized void setSchema(JsonNode schema) {
        this.schema = schema;
    }

    /**
     * @return the tenantId
     */
    public synchronized String getTenantId() {
        return tenantId;
    }

    /**
     * @param tenantId the tenantId to set
     */
    public synchronized void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /**
     * @return the owner
     */
    public synchronized String getOwner() {
        return owner;
    }

    /**
     * @param owner the owner to set
     */
    public synchronized void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * @return the internalUsername
     */
    public String getInternalUsername() {
        return internalUsername;
    }

    /**
     * @param internalUsername the internalUsername to set
     */
    public void setInternalUsername(String internalUsername) {
        this.internalUsername = internalUsername;
    }

    /**
     * @return the uuid
     */
    public synchronized String getUuid() {
        return uuid;
    }

    /**
     * @param uuid the uuid to set
     */
    public synchronized void setUuid(String uuid) {
        this.uuid = uuid;
    }

    /**
     * @return the created
     */
    public synchronized Date getCreated() {
        return created;
    }

    /**
     * @param created the created to set
     */
    public synchronized void setCreated(Date created) {
        this.created = created;
    }

    /**
     * @return the lastUpdated
     */
    public synchronized Date getLastUpdated() {
        return lastUpdated;
    }

    /**
     * @param lastUpdated the lastUpdated to set
     */
    public synchronized void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    /**
     * @return the permissions
     */
    public synchronized List<MetadataPermission> getPermissions() {
        return permissions;
    }

    //KL

    /**
     * @param pem the permissions to set
     */
    public synchronized void setPermissions(List<MetadataPermission> pem) {
        this.permissions = pem;
    }

    public synchronized void updatePermissions(MetadataPermission pem) throws MetadataException {
        MetadataPermission currentUserPermission = this.getPermissionForUsername(pem.getUsername());

        if (currentUserPermission != null) {
            int indx = this.permissions.indexOf(currentUserPermission);
            this.permissions.set(indx, pem);
        } else {
            this.permissions.add(pem);
        }
    }

    /**
     * @param pem The permission to remove
     */
    public synchronized void removePermission(MetadataPermission pem) {
        this.permissions.remove(pem);
    }

    /**
     * Returns permission for the user with the given username
     * @param username the username of the person whose permissino will be returned.
     * @return the permission of the user, or null of not present
     * @throws MetadataException when unable to instantiate a metadata permission item
     */
    public synchronized MetadataPermission getPermissionForUsername(String username) throws MetadataException {
        for (MetadataPermission pem : this.permissions) {
            if (pem.getUsername().equals(username)) {
                return pem;
            }
        }
        if (StringUtils.equals(username, this.getOwner())) {
            return new MetadataPermission(username, PermissionType.ALL);
        }
        return null;
    }

    @JsonIgnore
    public ObjectNode toObjectNode() {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode()
                .put("owner", getOwner())
                .put("uuid", getUuid())
                .put("created", getCreated().toInstant().toString())
                .put("lastUpdated", getLastUpdated().toInstant().toString());

        json.set("schema", getSchema());

        ObjectNode hal = mapper.createObjectNode();
        hal.set("self", mapper.createObjectNode().put("href",
                TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE + "schemas/" + getUuid())));

        json.set("_links", hal);

        return json;
    }

    
}
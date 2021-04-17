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
import org.hibernate.validator.constraints.Length;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.model.validation.constraints.ValidJsonSchema;

import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Past;
import java.util.Date;

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
    
    @NotNull
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String tenantId;
    
    @NotNull
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String internalUsername;
    
    @NotNull
    @Length(min=1,max=128)
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String owner;
    
    @Length(max=64)
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private String uuid;
    
    @JsonInclude(Include.NON_NULL)
    private String error;
    
    @Past
    @NotNull
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    private Date created;
    
    @JsonView({MetadataViews.Resource.Summary.class, MetadataViews.Request.class})
    @NotNull
    private Date lastUpdated;
    
    @JsonView({MetadataViews.Resource.Summary.class})
    private String _links;
    
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
     * @return the error
     */
    public synchronized String getError() {
        return error;
    }

    /**
     * @param error the error to set
     */
    public synchronized void setError(String error) {
        this.error = error;
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
    
//    @JsonValue
//    public String toString() {
//        return toObjectNode().toString();
//    }

//    public DBObject toDBObject() {
//        DBObject json = new BasicDBObject()
//                .append("name", getName())
//                .append("value", getValue())
//                .append("schemaId", getSchemaId())
//                .append("owner", getOwner())
//                .append("uuid", getUuid())
//                .append("tenantId", getTenantId())
//                .append("created", new DateTime(getCreated()).toString())
//                .append("lastUpdated", new DateTime(getLastUpdated()).toString())
//                .append("associatedUuids", new BasicDBList().addAll(getAssociatedUuids().getRawUuid()));
//        return json;
//    }
    
}
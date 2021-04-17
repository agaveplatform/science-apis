package org.iplantc.service.metadata.model.serialization;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.json.JsonWriter;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONWriter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.DeflaterOutputStream;

/**
 * Serialize the MetadataItem class
 *
 * @author kliang
 */
public class MetadataItemSerializer {
    private static final Logger log = Logger.getLogger(MetadataItemSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private MetadataItem metadataItem;
    String resourceURL;

    public MetadataItemSerializer() {
        this.resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);
    }

    public MetadataItemSerializer(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
        this.resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);
    }

    /**
     * Format MetadataItem to BasicDBObject
     *
     * @return BasicDBObject equivalent of the MetadataItem
     */
    public BasicDBObject formatMetadataItem(MetadataItem metadataItem) {
        BasicDBObject result;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:SS'Z'-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        result = new BasicDBObject("uuid", metadataItem.getUuid())
                .append("schemaId", metadataItem.getSchemaId())
                .append("internalUsername", metadataItem.getInternalUsername())
                .append("associationIds", metadataItem.getAssociations().getAssociatedIds().keySet().toString())
                .append("lastUpdated", metadataItem.getLastUpdated().toInstant().toString())
                .append("name", metadataItem.getName())
                .append("value", BasicDBObject.parse(String.valueOf(metadataItem.getValue())))
                .append("created", metadataItem.getCreated().toInstant().toString())
                .append("owner", metadataItem.getOwner());

        return result;
    }


    /**
     * Format {@link MetadataItem} to an {@link ObjectNode}
     * @param metadataItem {@link MetadataItem} to format
     * @return {@link ObjectNode} equivalent of the MetadataItem
     */
    public ObjectNode formatMetadataItemToNode(MetadataItem metadataItem) {
        ObjectNode result = mapper.createObjectNode();
        result.put("uuid", metadataItem.getUuid())
                .put("schemaId", metadataItem.getSchemaId())
                .put("internalUsername", metadataItem.getInternalUsername())
                .put("associationIds", metadataItem.getAssociations().getAssociatedIds().keySet().toString())
                .put("lastUpdated", metadataItem.getLastUpdated().toInstant().toString())
                .put("name", metadataItem.getName())
                .put("created", metadataItem.getCreated().toInstant().toString())
                .put("owner", metadataItem.getOwner())
                .replace("value", metadataItem.getValue());

        return result;
    }


    /**
     * Format {@link MetadataItem} to an {@link JsonNode} with resolved links
     * @param metadataItem {@link MetadataItem} to format
     * @return {@link JsonNode} equivalent of the MetadataItem
     */
    public JsonNode formatMetadataItemJsonResult(MetadataItem metadataItem) throws UUIDException {
        ObjectNode metadataObject = formatMetadataItemToNode(metadataItem);

        ObjectNode hal = mapper.createObjectNode();

        if (resourceURL == null)
            this.resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);

        hal.putObject("self")
                .put("href",
                        resourceURL + "data/" +
                                metadataItem.getUuid());
        hal.putObject("permissions")
                .put("href",
                        resourceURL + "data/" +
                                metadataItem.getUuid() + "/pems");
        hal.putObject("owner")
                .put("href",
                        resourceURL + metadataItem.getOwner());
        if (metadataItem.getAssociations() != null) {
            MetadataAssociationList associationList = metadataItem.getAssociations();

            List<JsonNode> halAssociationIds = formatAssociationIdsToNodeList(associationList);
            hal.putArray("associationIds")
                    .addAll(halAssociationIds);
        }
        if (metadataItem.getSchemaId() != null && !StringUtils.isEmpty(metadataItem.getSchemaId())) {
            AgaveUUID agaveUUID = new AgaveUUID(metadataItem.getSchemaId());
            hal.putObject(agaveUUID.getResourceType().name())
                    .put("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference()));

        }

        metadataObject.replace("_links", hal);

        return metadataObject;

    }


    /**
     * Format {@link Document} without the permissions, tenantId, or _id field
     * @param document {@link Document} to format
     * @return {@link Document} with removed fields
     */
    public Document formatDocumentResult(Document document){
        if (document.containsKey("permissions")) {
            document.remove("permissions");
        }

        if (document.containsKey("_id")) {
            document.remove("_id");
        }

        if (document.containsKey("tenantId")){
            document.remove("tenantId");
        }
        return document;
    }

    /**
     * Format {@link MetadataItem} to a {@link Document} with resolved links
     * @param document {@link Document} to format
     * @return {@link Document} equivalent of the MetadataItem
     */
    public Document formatMetadataItemDocumentResult(Document document) throws UUIDException, PermissionException, MetadataAssociationException {
        Document hal = new Document();

        if (resourceURL == null)
            this.resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);

        String uuid = document.getString("uuid");
        hal.put("self",
                new Document("href",
                        resourceURL + "data/" +
                                uuid));
        hal.put("permissions",
                new Document("href",
                        resourceURL + "data/" +
                                uuid + "/pems"));
        hal.put("owner",
                new Document("href",
                        resourceURL + document.getString("owner")));


        if (document.containsKey("associationIds")) {
            BasicDBList halAssociationIds = new BasicDBList();
            MetadataAssociationList associationList = new MetadataAssociationList();

            List<String> docAssociationList = document.getList("associationIds", String.class);
            if (docAssociationList.size() > 0)
                associationList.addAll(docAssociationList);

            halAssociationIds = formatAssociationIds(associationList);
            hal.put("associationIds", halAssociationIds);
        }

        if (document.containsKey("schemaId")) {
            String schemaId = document.getString("schemaId");
            if (schemaId != null && !StringUtils.isEmpty(schemaId)) {
                AgaveUUID agaveUUID = new AgaveUUID(schemaId);
                hal.append(agaveUUID.getResourceType().name(),
                        new Document("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference())));
            }
        } else {
            document.put("schemaId", null); 
        }
        document.put("created", document.getDate("created").toInstant().toString());
        document.put("lastUpdated", document.getDate("lastUpdated").toInstant().toString());
        document.put("_links", hal);
        return document;
    }


    /**
     * Format MetadataItem to BasicDBObject
     *
     * @return BasicDBObject equivalent of the MetadataItem
     * @deprecated
     */
    public BasicDBObject formatMetadataItem() {
        return new BasicDBObject("uuid", metadataItem.getUuid())
                .append("schemaId", metadataItem.getSchemaId())
                .append("internalUsername", metadataItem.getInternalUsername())
                .append("associationIds", metadataItem.getAssociations().getAssociatedIds().keySet().toString())
                .append("lastUpdated", metadataItem.getLastUpdated().toInstant().toString())
                .append("name", metadataItem.getName())
                .append("value", BasicDBObject.parse(String.valueOf(metadataItem.getValue())))
                .append("created", metadataItem.getCreated().toInstant().toString())
                .append("owner", metadataItem.getOwner());
    }

    /**
     * Format MetadataItem to DBObject with the resolved URLS
     *
     * @return DBObject equivalent of the MetadataItems with the resolved URLS
     * @throws UUIDException
     * @deprecated
     */
    public DBObject formatMetadataItemResult(MetadataItem metadataItem) throws UUIDException {
        BasicDBObject metadataObject = formatMetadataItem(metadataItem);
        BasicDBObject hal = new BasicDBObject();

        if (resourceURL == null)
            this.resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);

        hal.put("self",
                new BasicDBObject("href",
                        resourceURL + "data/" +
                                metadataItem.getUuid()));
        hal.put("permissions",
                new BasicDBObject("href",
                        resourceURL + "data/" +
                                metadataItem.getUuid() + "/pems"));
        hal.put("owner",
                new BasicDBObject("href",
                        resourceURL + metadataItem.getOwner()));

        if (metadataItem.getAssociations() != null) {
            BasicDBList halAssociationIds = new BasicDBList();
            MetadataAssociationList associationList = metadataItem.getAssociations();

            halAssociationIds = formatAssociationIds(associationList);
            hal.put("associationIds", halAssociationIds);
        }

        if (metadataItem.getSchemaId() != null && !StringUtils.isEmpty(metadataItem.getSchemaId())) {
            AgaveUUID agaveUUID = new AgaveUUID(metadataItem.getSchemaId());
            hal.append(agaveUUID.getResourceType().name(),
                    new BasicDBObject("href", TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference())));

        }
        metadataObject.put("_links", hal);
        return metadataObject;

    }

    public BasicDBList getResolvedUrlsForAssociationIds(MetadataAssociationList associationList) throws UUIDException {
        // TODO: break this into a list of object under the associationIds attribute so
        // we dont' overwrite the objects in the event there are multiple of the same type.

        BasicDBList halAssociationIds = new BasicDBList();
        for (String associatedId : associationList.getAssociatedIds().keySet()) {
            AgaveUUID agaveUUID = new AgaveUUID(associatedId);

            try {
                String resourceUrl = agaveUUID.getObjectReference();
                BasicDBObject assocResource = new BasicDBObject();
                assocResource.put("rel", associatedId);
                assocResource.put("href", TenancyHelper.resolveURLToCurrentTenant(resourceUrl));
                assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                halAssociationIds.add(assocResource);
            } catch (UUIDException e) {
                BasicDBObject assocResource = new BasicDBObject();
                assocResource.put("rel", associatedId);
                assocResource.put("href", null);
                if (agaveUUID != null) {
                    assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                }
                halAssociationIds.add(assocResource);
            }
        }
        return halAssociationIds;
    }

    public List<JsonNode> formatAssociationIdsToNodeList(MetadataAssociationList associationList) throws UUIDException {
        List<JsonNode> halAssociationIds = new ArrayList<>();
        for (Map.Entry<String, AssociatedReference> entry : associationList.getAssociatedIds().entrySet()) {
            AgaveUUID agaveUUID = new AgaveUUID(entry.getKey());
            ObjectNode node = mapper.createObjectNode()
                    .put("rel", entry.getKey())
                    .put("href", entry.getValue().getUrl())
                    .put("title", agaveUUID.getResourceType().name().toLowerCase());
            halAssociationIds.add(node);
        }
        return halAssociationIds;
    }

    public BasicDBList formatAssociationIds(MetadataAssociationList associationList) throws UUIDException {
        BasicDBList halAssociationIds = new BasicDBList();

        for (Map.Entry<String, AssociatedReference> entry : associationList.getAssociatedIds().entrySet()) {
            AgaveUUID agaveUUID = new AgaveUUID(entry.getKey());
            BasicDBObject assocResource = new BasicDBObject();

            assocResource.put("rel", entry.getKey());
            assocResource.put("href", entry.getValue().getUrl());
            assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
            halAssociationIds.add(assocResource);
        }
        return halAssociationIds;
    }

    public void getResolvedUrlsForAssociationIdsJSON(MetadataAssociationList associationList, JSONWriter writer) throws UUIDException, JSONException {
        writer.key("associationId").array();
        for (String associatedId : associationList.getAssociatedIds().keySet()) {
            AgaveUUID agaveUUID = new AgaveUUID(associatedId);
            writer.object();
            try {
                String resourceUrl = agaveUUID.getObjectReference();
                writer.key("rel")
                        .value((String) associatedId)
                        .key("href").value(TenancyHelper.resolveURLToCurrentTenant(resourceUrl))
                        .key("title").value(agaveUUID.getResourceType().name().toLowerCase());

            } catch (UUIDException e) {
                writer.key("rel")
                        .value((String) associatedId)
                        .key("href").value(null);
                if (agaveUUID != null) {
                    writer.key("title").value(agaveUUID.getResourceType().name().toLowerCase());
                }
            }
            writer.endObject();
        }
        writer.endArray();
    }


    public void formatMetadataItemResultJson(JSONWriter writer) throws JSONException, UUIDException {

        writer.key("_links").object();

        writer.key("self").object()
                .key("href")
                .value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                        metadataItem.getUuid())
                .endObject()
                .key("permissions").object()
                .key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                metadataItem.getUuid() + "/pems")
                .endObject()
                .key("owner").object()
                .key("href").value(TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + metadataItem.getOwner())
                .endObject()
                .endObject();

        if (metadataItem.getAssociations() != null) {
            getResolvedUrlsForAssociationIdsJSON(metadataItem.getAssociations(), writer);
        }

        if (metadataItem.getSchemaId() != null && !StringUtils.isEmpty(metadataItem.getSchemaId())) {
            AgaveUUID agaveUUID = new AgaveUUID(metadataItem.getSchemaId());
            writer.key(agaveUUID.getResourceType().name()).object()
                    .key("href").value(TenancyHelper.resolveURLToCurrentTenant(agaveUUID.getObjectReference()))
                    .endObject();
        }
        writer.endObject();
    }


    public String toJson() throws JSONException, UUIDException {
        JSONWriter writer = new JSONStringer();
        //metadata item
        writer.object()
                .key("uuid").value(metadataItem.getUuid())
                .key("schemaId").value(metadataItem.getSchemaId())
                .key("associationIds").array();
        for (String associatedId : metadataItem.getAssociations().getAssociatedIds().keySet()) {
            writer.value(associatedId);
        }
        writer.endArray()
                .key("lastUpdated").value(metadataItem.getLastUpdated().toInstant().toString())
                .key("name").value(metadataItem.getName())
                .key("value").value(metadataItem.getValue())
                .key("created").value(metadataItem.getCreated().toInstant().toString())
                .key("owner").value(metadataItem.getOwner());

        //links
        formatMetadataItemResultJson(writer);
        writer.endObject();
        return writer.toString();
    }

}

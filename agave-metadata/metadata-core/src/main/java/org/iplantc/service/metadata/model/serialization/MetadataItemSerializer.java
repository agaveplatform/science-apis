package org.iplantc.service.metadata.model.serialization;


import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.lang.StringUtils;
import org.bson.json.JsonWriter;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONWriter;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

/**
 * Serialize the MetadataItem class
 *
 * @author kliang
 */
public class MetadataItemSerializer {

    private MetadataItem metadataItem;

    public MetadataItemSerializer(MetadataItem metadataItem){
        this.metadataItem = metadataItem;
    }

    /**
     * Format MetadataItem to BasicDBObject
     * @return BasicDBObject equivalent of the MetadataItem
     */
    public BasicDBObject formatMetadataItem() {
        BasicDBObject result;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:SSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        result = new BasicDBObject("uuid", metadataItem.getUuid())
                .append("schemaId", metadataItem.getSchemaId())
                .append("internalUsername", metadataItem.getInternalUsername())
                .append("associationIds", metadataItem.getAssociations().getAssociatedIds().keySet().toString())
                .append("lastUpdated", formatter.format(metadataItem.getLastUpdated()))
                .append("name", metadataItem.getName())
                .append("value", BasicDBObject.parse(String.valueOf(metadataItem.getValue())))
                .append("created", formatter.format(metadataItem.getCreated()))
                .append("owner", metadataItem.getOwner());
        return result ;
    }

    /**
     * Format MetadataItem to DBObject with the resolved URLS
     * @return DBObject equivalent of the MetadataItems with the resolved URLS
     * @throws UUIDException
     */
    public DBObject formatMetadataItemResult() throws UUIDException {
        BasicDBObject metadataObject = formatMetadataItem();
        BasicDBObject hal = new BasicDBObject();
        String resourceURL = TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE);
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
                        resourceURL+ metadataItem.getOwner()));

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
        for (String associatedId : associationList.getAssociatedIds().keySet()){
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
        for (String associatedId : associationList.getAssociatedIds().keySet()){
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
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        JSONWriter writer = new JSONStringer();
        //metadata item
        writer.object()
                .key("uuid").value(metadataItem.getUuid())
                .key("schemaId").value(metadataItem.getSchemaId())
                .key("associationIds").array();
                for (String associatedId : metadataItem.getAssociations().getAssociatedIds().keySet()){
                    writer.value(associatedId);
                }
                writer.endArray()
                .key("lastUpdated").value(formatter.format(metadataItem.getLastUpdated()))
                .key("name").value(metadataItem.getName())
                .key("value").value(metadataItem.getValue())
                .key("created").value(formatter.format(metadataItem.getCreated()))
                .key("owner").value(metadataItem.getOwner());

        //links
        formatMetadataItemResultJson(writer);
        writer.endObject();
        return writer.toString();
    }

}

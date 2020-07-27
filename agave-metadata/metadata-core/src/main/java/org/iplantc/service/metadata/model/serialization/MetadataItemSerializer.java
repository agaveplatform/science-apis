package org.iplantc.service.metadata.model.serialization;


import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONWriter;

import java.text.SimpleDateFormat;
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
    public BasicDBObject formatMetadataItem(){
        BasicDBObject result;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:SSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        result = new BasicDBObject("uuid", metadataItem.getUuid())
                .append("schemaId", metadataItem.getSchemaId())
                .append("internalUsername", metadataItem.getInternalUsername())
                .append("associationIds", String.valueOf(metadataItem.getAssociations()))
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
        hal.put("self",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                                metadataItem.getUuid()));
        hal.put("permissions",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data/" +
                                metadataItem.getUuid() + "/pems"));
        hal.put("owner",
                new BasicDBObject("href",
                        TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_PROFILE_SERVICE) + metadataItem.getOwner()));

        if (metadataItem.getAssociations() != null) {
            // TODO: break this into a list of object under the associationIds attribute so
            // we dont' overwrite the objects in the event there are multiple of the same type.
            BasicDBList halAssociationIds = new BasicDBList();

            MetadataAssociationList associationList = metadataItem.getAssociations();

            for (String associatedId : associationList.getAssociatedIds().keySet()){
                AgaveUUID agaveUUID = new AgaveUUID((String) associatedId);

                try {
                    String resourceUrl = agaveUUID.getObjectReference();
                    BasicDBObject assocResource = new BasicDBObject();
                    assocResource.put("rel", (String) associatedId);
                    assocResource.put("href", TenancyHelper.resolveURLToCurrentTenant(resourceUrl));
                    assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                    halAssociationIds.add(assocResource);
                } catch (UUIDException e) {
                    BasicDBObject assocResource = new BasicDBObject();
                    assocResource.put("rel", (String) associatedId);
                    assocResource.put("href", null);
                    if (agaveUUID != null) {
                        assocResource.put("title", agaveUUID.getResourceType().name().toLowerCase());
                    }
                    halAssociationIds.add(assocResource);
                }
            }

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

}

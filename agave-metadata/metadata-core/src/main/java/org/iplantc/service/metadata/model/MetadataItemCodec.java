package org.iplantc.service.metadata.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Custom Codec for processing MetadataItem to Bson
 *
 * @author kliang
 */

public class MetadataItemCodec implements Codec<MetadataItem> {
    private static final Logger log = Logger.getLogger(MetadataItemCodec.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Codec<Document> documentCodec;

    public MetadataItemCodec() {
        this.documentCodec = new DocumentCodec();
    }

    public MetadataItemCodec(Codec<Document> codec) {
        this.documentCodec = codec;
    }

    @Override
    public MetadataItem decode(BsonReader reader, DecoderContext decoderContext) {
        MetadataItem metadataItem = new MetadataItem();
        Document document = documentCodec.decode(reader, decoderContext);

        String uuid = document.getString("uuid");
        if (StringUtils.isNotEmpty(uuid)) {
            metadataItem.setUuid(uuid);
        }

        try {
            Date createdDate = document.getDate("created");
            if (createdDate != null) {
                metadataItem.setCreated(createdDate);
            }
        } catch (Exception e) {
            log.error("Failed to decode created dates for metadata item " + uuid);
        }

        try {
            Date lastUpdatedDate = document.getDate("lastUpdated");
            if (lastUpdatedDate != null) {
                metadataItem.setLastUpdated(lastUpdatedDate);
            }
        } catch (Exception e) {
            log.error("Failed to decode lastUpdated date for metadata item " + uuid);
        }

        String owner = document.getString("owner");
        if (StringUtils.isNotEmpty(owner)) {
            metadataItem.setOwner(owner);
        }

        String tenantId = document.getString("tenantId");
        if (StringUtils.isNotEmpty(tenantId)) {
            metadataItem.setTenantId(tenantId);
        }

        String schemaId = document.getString("schemaId");
        if (StringUtils.isNotEmpty(schemaId)) {
            metadataItem.setSchemaId(schemaId);
        }

        String internalUsername = document.getString("internalUsername");
        if (StringUtils.isNotEmpty(internalUsername)) {
            metadataItem.setInternalUsername(internalUsername);
        }

        List<String> associationList = document.getList("associationIds", String.class);

        MetadataAssociationList metadataAssociationList = metadataItem.getAssociations();

        if (associationList != null) {
            // Allow invalid uuids to pass to preserve the reference and let the appropriate
            // service to handle access issues
            for (String associationId : associationList) {
                try {
                    metadataAssociationList.add(associationId);
                } catch (MetadataAssociationException | PermissionException ignored) {}
            }
        }
        metadataItem.setAssociations(metadataAssociationList);

        String name = document.getString("name");
        if (StringUtils.isNotEmpty(name))
            metadataItem.setName(name);


        // Metadata value can be anything, so we do a generic pojo deserialization. Primary types should be retained
        JsonNode json = null;
        try {
            json = objectMapper.valueToTree(document.get("value"));
        }
        catch (Exception e) {
            log.error("Failed to decode lastUpdated date for metadata item " + uuid);
        }

        metadataItem.setValue(json);

        //permissions
        metadataItem.setPermissions(new ArrayList<>());
        List<MetadataPermission> permissionList = new ArrayList<>();
        List<Document> permList = document.getList("permissions", Document.class);

        if (permList != null) {
            for (Document doc : permList) {
                MetadataPermission newPem = new MetadataPermission();
                try {
                    newPem.setUsername((String) doc.get("username"));
                    newPem.setGroup((String) doc.get("group"));
                    newPem.setPermission(PermissionType.getIfPresent(doc.get("permission").toString().toUpperCase()));
                    permissionList.add(newPem);
                } catch (MetadataException e) {
                    e.printStackTrace();
                }
            }
        }
        metadataItem.setPermissions(permissionList);
        return metadataItem;
    }

    @Override
    public void encode(BsonWriter writer, MetadataItem metadataItem, EncoderContext encoderContext) {

        writer.writeStartDocument();
        writer.writeDateTime("created", metadataItem.getCreated().getTime());
        writer.writeDateTime("lastUpdated", metadataItem.getLastUpdated().getTime());
        writer.writeName("uuid");
        writer.writeString(metadataItem.getUuid());
        writer.writeName("owner");
        writer.writeString(metadataItem.getOwner());
        writer.writeName("tenantId");
        writer.writeString(metadataItem.getTenantId());
        writer.writeName("associationIds");
        writer.writeStartArray();
        for (String key : metadataItem.getAssociations().getAssociatedIds().keySet()) {
            writer.writeString(key);
        }
//        writer.writeString(value.getAssociations().toString());
        writer.writeEndArray();
        writer.writeName("schemaId");
        try {
            writer.writeString(metadataItem.getSchemaId());
        } catch (Exception e) {
            writer.writeNull();
        }
        writer.writeName("name");
        writer.writeString(metadataItem.getName());

        writer.writeName("value");
        encodeJsonNode(writer, metadataItem.getValue());

        writer.writeStartArray("permissions");
        for (MetadataPermission pem : metadataItem.getPermissions()) {
            writer.writeStartDocument();
            writer.writeName("username");
            writer.writeString(pem.getUsername());
            writer.writeNull("group");
//            writer.writeName("group");
//            writer.writeString(pem.getGroup());
            writer.writeName("permission");
            writer.writeString(pem.getPermission().toString());
            writer.writeEndDocument();
        }
        writer.writeEndArray();
        writer.writeEndDocument();
    }

    /**
     * Writes the given jsonNode to the BsonWriter. This method allows for recursion to
     * support both primary types, ObjectNodes, and ArrayNodes.
     *
     * @param writer the active writer for the metadata item codec encoding.
     * @param jsonNode the jsonNode to process
     */
    public void encodeJsonNode(BsonWriter writer, JsonNode jsonNode) {
        if (jsonNode == null) {
            writer.writeNull();
        } else if (jsonNode.isValueNode()) {
            if (jsonNode.isIntegralNumber()) {
                writer.writeInt32(jsonNode.intValue());
            } else if (jsonNode.isFloatingPointNumber()) {
                writer.writeDouble(jsonNode.asDouble());
            } else if (jsonNode.isBoolean()) {
                writer.writeBoolean(jsonNode.asBoolean());
            } else {
                writer.writeString(jsonNode.textValue());
            }
        } else if (jsonNode.isArray()) {
            writer.writeStartArray();
            jsonNode.forEach(item -> encodeJsonNode(writer, item));
            writer.writeEndArray();
        } else {
            try {
                String jsonValueString = objectMapper.writeValueAsString(jsonNode);
                BsonDocument bsonDocument = BsonDocument.parse(jsonValueString);
                BsonReader bsonReader = new BsonDocumentReader(bsonDocument);
                writer.pipe(bsonReader);
            }
            catch (JsonProcessingException e) {
                log.error("Failed to serialize value for metadata value " + jsonNode.asText(), e);
                writer.writeString(jsonNode.asText());
            }
        }
    }

    @Override
    public Class<MetadataItem> getEncoderClass() {
        return MetadataItem.class;
    }


    /**
     * Write to BsonWriter recursively for nested JsonNodes
     *
     * @param doc    JsonNode to parse
     * @param writer BsonWriter to write to
     * @return BsonWriter
     */
    private BsonWriter recursiveParseDocument(JsonNode doc, BsonWriter writer) {
        for (Iterator<String> it = doc.fieldNames(); it.hasNext(); ) {
            String key = it.next();

            if (doc.get(key).isObject()) {
                JsonNode nestedNode = doc.get(key);
                writer.writeStartDocument(key);
                recursiveParseDocument(nestedNode, writer);
            } else {
                writer.writeName(key);
                writer.writeString(String.valueOf(doc.get(key).textValue()));
            }
        }
        writer.writeEndDocument();
        return writer;
    }
}

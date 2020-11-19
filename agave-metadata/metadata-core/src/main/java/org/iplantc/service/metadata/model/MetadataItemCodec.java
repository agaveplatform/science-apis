package org.iplantc.service.metadata.model;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Date;

/**
 * Custom Codec for processing MetadataItem to Bson
 *
 * @author kliang
 */

public class MetadataItemCodec implements Codec<MetadataItem> {

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

        try {
            Date createdDate = document.getDate("created");
            if (createdDate != null)
                metadataItem.setCreated(createdDate);

            Date lastUpdatedDate = document.getDate("lastUpdated");
            if (lastUpdatedDate != null)
                metadataItem.setLastUpdated(lastUpdatedDate);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String uuid = document.getString("uuid");
        if (StringUtils.isNotEmpty(uuid))
            metadataItem.setUuid(uuid);

        String owner = document.getString("owner");
        if (StringUtils.isNotEmpty(owner))
            metadataItem.setOwner(owner);

        String tenantId = document.getString("tenantId");
        if (StringUtils.isNotEmpty(tenantId))
            metadataItem.setTenantId(tenantId);

        String schemaId = document.getString("schemaId");
        if (StringUtils.isNotEmpty(schemaId))
            metadataItem.setSchemaId(schemaId);

        String internalUsername = document.getString("internalUsername");
        if (StringUtils.isNotEmpty(internalUsername))
            metadataItem.setInternalUsername(internalUsername);

        List<String> associationList = document.getList("associationIds", String.class);


        MetadataAssociationList metadataAssociationList = metadataItem.getAssociations();

        if (associationList != null) {
            // Allow invalid uuids to pass to preserve the reference and let the appropriate
            // service to handle access issues
            for (String associationId : associationList) {
                try {
                    metadataAssociationList.add(associationId);
                } catch (MetadataAssociationException e) {
                } catch (PermissionException e) {
                }
            }
        }
        metadataItem.setAssociations(metadataAssociationList);

        String name = document.getString("name");
        if (StringUtils.isNotEmpty(name))
            metadataItem.setName(name);

        //value
        ObjectMapper mapper = new ObjectMapper();
        Document value;
        try {
            value = (Document) document.get("value");
        } catch (Exception e) {
            value = new Document();
        }
        ObjectNode json = mapper.createObjectNode();

        if (value != null) {
            for (String key : value.keySet()) {
                Document doc = null;
                try {
                    doc = (Document) value.get(key);
                    JsonFactory factory = new ObjectMapper().getFactory();
                    JsonNode jsonMetadataNode = factory.createParser(doc.toJson()).readValueAsTree();
                    json.replace(key, jsonMetadataNode);

                } catch (Exception e) {
                    json.put(key, String.valueOf(value.get(key)));
                }
            }
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
    public void encode(BsonWriter writer, MetadataItem value, EncoderContext encoderContext) {

        writer.writeStartDocument();
        writer.writeDateTime("created", value.getCreated().getTime());
        writer.writeDateTime("lastUpdated", value.getLastUpdated().getTime());
        writer.writeName("uuid");
        writer.writeString(value.getUuid());
        writer.writeName("owner");
        writer.writeString(value.getOwner());
        writer.writeName("tenantId");
        writer.writeString(value.getTenantId());
        writer.writeName("associationIds");
        writer.writeStartArray();
        for (String key : value.getAssociations().getAssociatedIds().keySet()) {
            writer.writeString(key);
        }
//        writer.writeString(value.getAssociations().toString());
        writer.writeEndArray();
        writer.writeName("schemaId");
        try {
            writer.writeString(value.getSchemaId());
        } catch (Exception e) {
            writer.writeNull();
        }
        writer.writeName("name");
        writer.writeString(value.getName());
        writer.writeName("value");
        writer.writeStartDocument();
        recursiveParseDocument(value.getValue(), writer);
        writer.writeStartArray("permissions");
        for (MetadataPermission pem : value.getPermissions()) {
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

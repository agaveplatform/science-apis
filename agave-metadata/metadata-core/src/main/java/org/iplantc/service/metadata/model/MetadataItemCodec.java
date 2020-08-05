package org.iplantc.service.metadata.model;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonReader;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonParseException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.joda.time.DateTime;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Custom Codec for processing MetadataItem to Bson
 *
 * @author kliang
 */

public class MetadataItemCodec implements Codec<MetadataItem> {

    private Codec<Document> documentCodec;

    public MetadataItemCodec(){
        this.documentCodec = new DocumentCodec();
    }

    public MetadataItemCodec(Codec<Document> codec){
        this.documentCodec = codec;
    }

    @Override
    public MetadataItem decode(BsonReader reader, DecoderContext decoderContext) {
        MetadataItem metadataItem = new MetadataItem();
        Document document = documentCodec.decode(reader, decoderContext);
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            metadataItem.setCreated(formatter.parse(document.getString("created")));
            metadataItem.setLastUpdated(formatter.parse(document.getString("lastUpdated")));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String uuid = document.getString("uuid");
        metadataItem.setUuid(uuid);
        metadataItem.setOwner(document.getString("owner"));
        metadataItem.setTenantId(document.getString("tenantId"));
        metadataItem.setSchemaId(document.getString("schemaId"));
        metadataItem.setInternalUsername(document.getString("internalUsername"));

        List<String> associationList = document.getList("associationIds", String.class);
        for (String associationId : associationList) {
            try {
                metadataItem.getAssociations().add(associationId);
            } catch (MetadataAssociationException e) {
                e.printStackTrace();
            } catch (PermissionException e) {
                e.printStackTrace();
            }
        }

        metadataItem.setName(document.getString("name"));

        //value
        ObjectMapper mapper = new ObjectMapper();
        Document value;
        try {
             value = (Document) document.get("value");
        } catch (Exception e) {
            value = new Document();
        }
        ObjectNode json = mapper.createObjectNode();

        for (String key : value.keySet()){
            Document doc = null;
            try {
                doc = (Document) value.get(key);
                JsonFactory factory = new ObjectMapper().getFactory();
                JsonNode jsonMetadataNode = factory.createParser(doc.toJson()).readValueAsTree();
                json.put(key, jsonMetadataNode);

            } catch (Exception e) {
                json.put(key, String.valueOf(value.get(key)));
            }
        }
        metadataItem.setValue(json);

        //permissions
        metadataItem.setPermissions(new ArrayList<>());
        List<MetadataPermission> permissionList = new ArrayList<>();
        List<Document> permList= document.getList("permissions", Document.class);
        for (Document doc : permList) {
            MetadataPermission newPem = new MetadataPermission();
            try {
                newPem.setUsername((String)doc.get("username"));
                newPem.setUuid(uuid);
                newPem.setGroup((String)doc.get("group"));
                newPem.setPermission(PermissionType.valueOf(doc.get("permission").toString()));
                permissionList.add(newPem);
            } catch (MetadataException e) {
                e.printStackTrace();
            }
        }
        metadataItem.setPermissions(permissionList);
        return metadataItem;
    }

    @Override
    public void encode(BsonWriter writer, MetadataItem value, EncoderContext encoderContext) {

        writer.writeStartDocument();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ-05:00");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        writer.writeName("created");
        writer.writeString(formatter.format(value.getCreated()));
        writer.writeName("lastUpdated");
        writer.writeString(formatter.format(value.getLastUpdated()));
        writer.writeName("uuid");
        writer.writeString(value.getUuid());
        writer.writeName("owner");
        writer.writeString(value.getOwner());
        writer.writeName("tenantId");
        writer.writeString(value.getTenantId());
        writer.writeName("associationIds");
        writer.writeStartArray();
        for (String key : value.getAssociations().getAssociatedIds().keySet()){
            writer.writeString(key);
        }
//        writer.writeString(value.getAssociations().toString());
        writer.writeEndArray();
        writer.writeName("name");
        writer.writeString(value.getName());
        writer.writeName("value");
        writer.writeStartDocument();
        recursiveParseDocument(value.getValue(), writer);
        writer.writeStartArray("permissions");

        for (MetadataPermission pem : value.getPermissions()){
            writer.writeStartDocument();
                writer.writeName("username");
                writer.writeString(pem.getUsername());
                writer.writeName("group");
                writer.writeString(pem.getGroup());
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
     * @param doc JsonNode to parse
     * @param writer BsonWriter to write to
     * @return BsonWriter
     */
    private BsonWriter recursiveParseDocument(JsonNode doc, BsonWriter writer){
        for (Iterator<String> it = doc.fieldNames(); it.hasNext(); ) {
            String key = it.next();

            if (doc.get(key).isObject()){
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

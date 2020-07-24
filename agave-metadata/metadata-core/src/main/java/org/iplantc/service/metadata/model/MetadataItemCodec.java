package org.iplantc.service.metadata.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

        String uuid = document.getString("uuid");

        metadataItem.setUuid(uuid);
        metadataItem.setOwner(document.getString("owner"));
        metadataItem.setTenantId(document.getString("tenantId"));
        metadataItem.setSchemaId(document.getString("schemaId"));
        metadataItem.setInternalUsername(document.getString("internalUsername"));
        //metadataItem.setLastUpdated(document.getDate("lastUpdated"));
        metadataItem.setName(document.getString("name"));

        ObjectMapper mapper = new ObjectMapper();

        Document value = new Document();

        try {
             value = (Document) document.get("value");
        } catch (Exception e) {
            //catch
        }
        ObjectNode json = mapper.createObjectNode();

        for (String key : value.keySet()){
            json.put(key, String.valueOf(value.get(key)));
        }

        metadataItem.setValue(json);

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
        writer.writeName("uuid");
        writer.writeString(value.getUuid());
        writer.writeName("owner");
        writer.writeString(value.getOwner());
        writer.writeName("tenantId");
        writer.writeString(value.getTenantId());
        writer.writeName("associationIds");
        writer.writeString(value.getAssociations().toString());
        writer.writeName("name");
        writer.writeString(value.getName());
        writer.writeName("value");
        writer.writeStartDocument();

        //write values to Document
        for (Iterator<String> it = value.getValue().fieldNames(); it.hasNext(); ) {
            String key = it.next();
            writer.writeName(key);
            writer.writeString(String.valueOf(value.getValue().get(key).textValue()));
        }

        writer.writeEndDocument();
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
}

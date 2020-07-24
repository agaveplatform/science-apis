package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.AsExistingPropertyTypeSerializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.Permission;
import java.util.Arrays;
import java.util.List;

public class MetadataSearchIT {

    public MetadataItem createMetadataItem() throws MetadataException, MetadataStoreException, IOException, MetadataQueryException {
        String userQuery = "{\"value\": {\"title\": \"Example Metadata\", \"properties\": {\"species\": \"arabidopsis\", \"description\": \"A model organism...\"}}, \"name\": \"test metadata\"}\"";
        String uuid = new AgaveUUID(UUIDType.METADATA).toString();
        String username = "testuser";
        MetadataSearch search = new MetadataSearch(false, username, uuid);
        search.setOwner(username);
        JsonFactory factory = new ObjectMapper().getFactory();
        JsonNode jsonMetadataNode = factory.createParser(userQuery).readValueAsTree();
        search.parseJsonMetadata(jsonMetadataNode);
        search.setOwner(username);
        search.updateMetadataItem();
        MetadataItem metadataItem = search.getMetadataItem();
        return metadataItem;
    }

    @Test
    public void collectionTest() throws IOException, MetadataException, UUIDException, MetadataQueryException, MetadataStoreException {
        String userQuery = "{\"name\": \"some metadata\"}";
        JsonFactory factory = new ObjectMapper().getFactory();

        String uuid = new AgaveUUID(UUIDType.METADATA).toString();
        String username = "testuser";
        String sharedUser = "testSharedUser";
        MetadataSearch search = new MetadataSearch(false, username, uuid);
        search.clearCollection();

        //add metadata for associatedID
        MetadataItem associatedItem = createMetadataItem();

        //for create/updating
            String strJson =
                    "  {" +
                    "    \"uuid\": \""+uuid+"\"," +
                    "    \"schemaId\": null," +
                    "    \"associationIds\": [" +
                    "      \""+associatedItem.getUuid()+"\"" +
                    "    ]," +
                    "    \"name\": \"some metadata\"," +
                    "    \"value\": {" +
                    "      \"title\": \"Example Metadata\"," +
                    "      \"properties\": {" +
                    "        \"species\": \"arabidopsis\"," +
                    "        \"description\": \"A model plant organism...\"" +
                    "      }" +
                    "    }," +
                    "    \"owner\": \""+username+"\"" +
                    "   }";

            JsonNode jsonMetadataNode = factory.createParser(strJson).readValueAsTree();

            //setup metadata item to be inserted
            search.parseJsonMetadata(jsonMetadataNode);

            if (search.find(userQuery).size() == 0) {
                search.setOwner(username);
            }

            //insert metdata item
            search.updateMetadataItem();
            Assert.assertEquals(search.findAll().size(), 2);

            DBObject result = search.formatMetadataItemResult(search.getMetadataItem());
            System.out.println(result);

    }

}

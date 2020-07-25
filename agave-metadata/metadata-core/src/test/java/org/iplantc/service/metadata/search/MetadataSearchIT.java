package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.AsExistingPropertyTypeSerializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.bson.Document;
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

    String uuid = new AgaveUUID(UUIDType.METADATA).toString();
    String username = "testuser";
    String sharedUser = "testSharedUser";


    public MetadataItem createMetadataItem() throws MetadataException, MetadataStoreException, IOException, MetadataQueryException, PermissionException {
        String userQuery = "{\"value\": {\"title\": \"Example Metadata\", \"properties\": {\"species\": \"arabidopsis\", \"description\": \"A model organism...\"}}, \"name\": \"test metadata\"}\"";
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
    public void collectionTest() throws IOException, MetadataException, UUIDException, MetadataQueryException, MetadataStoreException, PermissionException {
        String userQuery = "{\"name\": \"some metadata\"}";
        JsonFactory factory = new ObjectMapper().getFactory();

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

    @Test
    public void updateExistingItem() throws MetadataException, MetadataQueryException, MetadataStoreException, IOException, PermissionException {
        String strUpdate =
                "  {" +
                        "    \"name\": \"New Metadata\"," +
                        "    \"value\": {" +
                        "      \"title\": \"Changed Metadata Title\"," +
                        "      \"properties\": {" +
                        "        \"species\": \"wisteria\"," +
                        "        \"description\": \"A model flower organism...\"" +
                        "       }" +
                        "       }" +
                        "   }";

        Document doc = Document.parse(strUpdate);


        //update item as user without permission
        MetadataSearch search = new MetadataSearch(false, sharedUser, uuid);

        search.clearCollection();

        //insert item
        MetadataItem metadataItem = createMetadataItem();
//        Assert.assertEquals(search.findAll().size(),1, "Collection size should be 1 after inserting the metadata item.");

        List<MetadataItem> resultAll = search.findAll();


        String query = "{\"name\": \"New Metadata\"}";

        JsonFactory factory = new ObjectMapper().getFactory();
        JsonNode jsonMetadataNode = factory.createParser(strUpdate).readValueAsTree();

        //give user the correct permissions
        search.parseJsonMetadata(jsonMetadataNode);
        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());

//        Assert.assertEquals(search.find(strUpdate).size(), 0, "User does not have read permissions, there should be no results.");
//        Assert.assertThrows(PermissionException.class, () -> search.find(strUpdate));

        //give user read permission
        search.setUsername(sharedUser);
        search.updatePermissions(sharedUser, "", PermissionType.READ);
        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());
//        Assert.assertEquals(search.findAll().size(), 1, "Updating metadata item should not add a new item.");
//        Assert.assertEquals(search.find(strUpdate).size(), 1, "User has read permissions, there should be 1 result for the metadata.");
//        Assert.assertThrows(PermissionException.class, () -> search.find(strUpdate));

//        MetadataItem metadataItemResult = search.findAll().get(0);


        //update item as user with permission
        search.setUsername(username);
        search.updatePermissions(sharedUser, "", PermissionType.READ_WRITE);
        search.updateMetadataItem();
        List<MetadataItem> result = search.find(query);
//        List<MetadataItem> resultAll = search.findAll();
        Assert.assertEquals(resultAll.size(), 1, "There should only be 1 metadata item");
        Assert.assertEquals(result.size(), 1, "User has read/write permissions, there should be 1 result for the metadata.");
        Assert.assertEquals(result.get(0).getName(), "New Metadata");
        MetadataItem currentMetadataItem = search.getMetadataItem();

        Assert.assertEquals(result.get(0).getValue(), currentMetadataItem.getValue());

    }

//    @Test
//    public void findAllMetadataForUserTest(){
//
//    }
//
//    @Test
//    public void findPermissionForUuidTest(){
//
//    }
//
//    @Test
//    public void findAllPermissionsForUserTest(){
//
//    }

}

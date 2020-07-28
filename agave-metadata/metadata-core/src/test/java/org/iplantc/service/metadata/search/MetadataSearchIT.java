package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Test(groups={"integration"})
public class MetadataSearchIT {

    String uuid;
    String username = "testuser";
    String sharedUser = "testSharedUser";
    String metadataQuery = "{\"value\": {\"title\": \"Example Metadata\", \"properties\": {\"species\": \"arabidopsis\", \"description\": \"A model organism...\"}}, \"name\": \"test metadata\"}\"";

    public MetadataItem createMetadataItem(String query) throws MetadataException, MetadataStoreException, IOException, MetadataQueryException, PermissionException {
        MetadataSearch search = new MetadataSearch(false, username);
        search.setOwner(username);
        JsonFactory factory = new ObjectMapper().getFactory();
        JsonNode jsonMetadataNode = factory.createParser(query).readValueAsTree();
        search.parseJsonMetadata(jsonMetadataNode);
        Document doc = new Document();
        doc.parse(metadataQuery);
        search.parseDocument(doc);
        search.setOwner(username);
        uuid = new AgaveUUID(UUIDType.METADATA).toString();
        search.setUuid(uuid);
        search.updateMetadataItem();
        return search.getMetadataItem();
    }

    @Test
    public void insertTest() throws IOException, MetadataException, UUIDException, MetadataQueryException, MetadataStoreException, PermissionException {
        String userQuery = "{\"name\": \"some metadata\"}";
        JsonFactory factory = new ObjectMapper().getFactory();

        MetadataSearch search = new MetadataSearch(false, username);
        search.clearCollection();

        //add metadata for associatedID
        MetadataItem associatedItem = createMetadataItem(metadataQuery);

        //for create/updating
        uuid = new AgaveUUID(UUIDType.METADATA).toString();
        search.setUuid(uuid);
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

        MetadataItemSerializer serializer = new MetadataItemSerializer(search.getMetadataItem());

        DBObject result = serializer.formatMetadataItemResult();
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

//        Document doc = Document.parse(strUpdate);

        //update item as user without permission
        MetadataSearch search = new MetadataSearch(false, sharedUser);

        search.clearCollection();

        //insert item
        MetadataItem metadataItem = createMetadataItem(metadataQuery);
        Assert.assertEquals(search.findAll().size(),1, "Collection size should be 1 after inserting the metadata item.");

        String queryBeforeUpdate = "{\"name\": \"test metadata\"}";
        String queryAfterUpdate = "{\"name\": \"New Metadata\"}";

        JsonFactory factory = new ObjectMapper().getFactory();
        JsonNode jsonMetadataNode = factory.createParser(strUpdate).readValueAsTree();

        //give user the correct permissions
        search.parseJsonMetadata(jsonMetadataNode);
        search.setUuid(metadataItem.getUuid());

        List<MetadataItem> testResult = search.findAll();

        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());

        Assert.assertEquals(search.find(strUpdate).size(), 0, "User does not have read permissions, there should be no results.");

        //try to change permission as user without read/write permission - exception should be thrown
        search.setUsername(sharedUser);
        search.updatePermissions(sharedUser, "", PermissionType.READ);
        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());

        //change permission as the owner
        search.setUsername(username);
        search.updatePermissions(sharedUser, "", PermissionType.READ);
        List<MetadataItem> resultAll = search.findAll();
        Assert.assertEquals(search.findAll().size(), 1, "Updating metadata item should not add a new item.");

        //user should now be able to search
        search.setUsername(sharedUser);
        Assert.assertEquals(search.find(queryBeforeUpdate).size(), 1, "User has read permissions, there should be 1 result for the metadata.");
        MetadataItem metadataItemResult = search.findAll().get(0);

        //grant user with write permissions and update
        search.setUsername(username);
        search.updatePermissions(sharedUser, "", PermissionType.READ_WRITE);

        search.setUsername(sharedUser);
        search.updateMetadataItem();

        Assert.assertEquals(search.find(queryBeforeUpdate).size(), 0, "Metadata name should be changed, the query should not return anything");
        List<MetadataItem> result = search.find(queryAfterUpdate);
        resultAll = search.findAll();
        Assert.assertEquals(resultAll.size(), 1, "There should only be 1 metadata item");
        Assert.assertEquals(result.size(), 1, "User has read/write permissions, there should be 1 result for the metadata.");
        Assert.assertEquals(result.get(0).getName(), "New Metadata");

        MetadataItem currentMetadataItem = search.getMetadataItem();

        Assert.assertEquals(result.get(0).getValue(), currentMetadataItem.getValue());

    }

    @Test
    public void searchTest() throws IOException, MetadataException, MetadataQueryException, MetadataStoreException, PermissionException {
        MetadataSearch search = new MetadataSearch(true, username);
        search.clearCollection();

        String metadataQueryMustard =
                "  {" +
                        "    \"name\": \"mustard plant\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a plant\"," +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"The seed of the mustard plant is used as a spice...\"" +
                        "       }" +
                        "   }";

        String metadataQueryCactus =
                "  {" +
                        "    \"name\": \"cactus (cactaeceae)\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a plant\"," +
                        "      \"order\": \"Caryophyllales\", " +
                        "        \"profile\": {" +
                        "        \"status\": \"inactive\"" +
                        "           }," +
                        "        \"description\": \"It could take a century for a cactus to produce its first arm. /n" +
                        "                           A type of succulent and monocots. .\"" +
                        "       }" +
                        "   }";

        String metadataQueryAgavoideae =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Asparagales\", " +
                        "        \"profile\": {" +
                        "        \"status\": \"paused\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "   }";

        String metadataQueryWisteria =
                "  {" +
                        "    \"name\": \"wisteria\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Fabales\", " +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"native to China, Korea, Japan, and the Eastern United States.\"" +
                        "       }" +
                        "   }";

        List<String> queryList = Arrays.asList(metadataQueryMustard, metadataQueryCactus, metadataQueryAgavoideae, metadataQueryWisteria);

        for (String query : queryList) {
            try {
                uuid = new AgaveUUID(UUIDType.METADATA).toString();
                createMetadataItem(query);
            } catch (Exception e ) {
                System.out.println("Exception reached: " + e);
            }
        }


        //verify that all items are added
        List<MetadataItem> resultAll = search.findAll();
        Assert.assertEquals(resultAll.size(), 4, "4 Metadata items should have been added.");

        String queryByName = "{\"name\":\"mustard plant\"}";
        String queryByValue = "{\"value.type\":\"a plant\"}";
        String queryByValueInArray = "{ \"value.profile.status\": { \"$in\": [ \"active\", \"paused\" ] } }";
        String queryByNameRegex = "{ \"name\": { \"$regex\": \"^Cactus.*\", \"$options\": \"i\"}}";
        String queryByValueRegex = "{ \"value.description\": { \"$regex\": \".*monocots.*\", \"$options\": \"m\"}}";
        String queryByValueConditional ="{" +
                "   \"$or\":[" +
                "      {" +
                "         \"value.description\":{" +
                "            \"$regex\": " +
                "               \".*century.*\"" +
                "            \"$options\":\"i\"" +
                "         }" +
                "      }," +
                "      {" +
                "         \"value.type\":{" +
                "            \"$regex\":\".*plant.*\"" +
                "         }," +
                "         \"value.order\":{" +
                "            \"$regex\":\"Asparagales\"" +
                "         }" +
                "      }" +
                "   ]" +
                "}";

        List<MetadataItem> resultList;

        resultList = search.find(queryByName);
        Assert.assertEquals(resultList.size(),1, "There should be 1 metadata item found: mustard plant");

        resultList = search.find(queryByValue);
        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: mustard plant and cactus");

        resultList = search.find(queryByValueInArray);
        Assert.assertEquals(resultList.size(), 3, "There should be 3 metadata items found: mustard plant, Agavoideae, wisteria");

        resultList = search.find(queryByNameRegex);
        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: cactus");

        resultList = search.find(queryByValueRegex);
        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: cactus");

        resultList = search.find(queryByValueConditional);
        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: cactus and Agavoideae");
    }

//    @Test
//    public void schemaTest(){
//
//    }

    @Test
    public void findAllMetadataForUserTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException {
        MetadataSearch implicitSearch = new MetadataSearch(true, username);
        MetadataSearch explicitSearch = new MetadataSearch(false, username);
        implicitSearch.clearCollection();

        String metadataQueryAgavoideae =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Asparagales\", " +
                        "      \"properties\": {" +
                        "        \"profile\": {" +
                        "        \"status\": \"paused\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "       }" +
                        "   }";

        String metadataQueryWisteria =
                "  {" +
                        "    \"name\": \"wisteria\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Fabales\", " +
                        "      \"properties\": {" +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"native to China, Korea, Japan, and the Eastern United States.\"" +
                        "       }" +
                        "       }" +
                        "   }";

        //insert items;
        List<String> queryList = Arrays.asList(metadataQueryAgavoideae, metadataQueryWisteria);
        for (String query : queryList) {
            uuid = new AgaveUUID(UUIDType.METADATA).toString();
            createMetadataItem(query);
        }

        //verify items were added
        List<MetadataItem> resultAll = implicitSearch.findAll();
        Assert.assertEquals(resultAll.size(), 2);

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = implicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 2, "Implicit Search should find 2 metadata items ");

        //only ones the user has been given explicit permission for
        searchResult = explicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 0, "Search should find 0 metadata items because user has not been given explicit permissions for the items.");

    }

    @Test
    public void findPermissionForUuidTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException {
        MetadataSearch search = new MetadataSearch(false, username);
        search.clearCollection();

        String metadataQueryAgavoideae =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Asparagales\", " +
                        "      \"properties\": {" +
                        "        \"profile\": {" +
                        "        \"status\": \"paused\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "       }" +
                        "   }";

//        uuid = new AgaveUUID(UUIDType.METADATA).toString();
        MetadataItem metadataItem = createMetadataItem(metadataQueryAgavoideae);
        search.setUuid(metadataItem.getUuid());

        List<MetadataItem> resultAll = search.findAll();
        Assert.assertEquals(resultAll.size(), 1);

        List<MetadataItem>result = search.findPermission_User(username, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 1, "There is only 1 metadataitem with the specified permission");

        result = search.findPermission_User(sharedUser, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 0, "The user doesn't have any permissions specified for the uuid");

        //adding permissions
//        search.setMetadataPermission(sharedUser, PermissionType.READ, "");
        search.updatePermissions(sharedUser, "", PermissionType.READ);
        result = search.findPermission_User(sharedUser, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 1, "The user has specified permission for the uuid");

    }

//    @Test
//    public void findAllPermissionsForUserTest(){
//
//    }

}

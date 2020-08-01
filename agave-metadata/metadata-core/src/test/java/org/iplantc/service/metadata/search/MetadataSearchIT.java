package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;
import com.mongodb.TaggableReadPreference;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.json.JSONException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups={"integration"})
public class MetadataSearchIT {

    String uuid;
    String username = "testuser";
    String sharedUser = "testSharedUser";
    List<String> queryList = new ArrayList<>();
    AgaveUUID jobUuid = new AgaveUUID(UUIDType.JOB);
    AgaveUUID schemaUuid = new AgaveUUID(UUIDType.SCHEMA);

    public List<String> setQueryList(List<String> stringList){
        if (stringList.isEmpty()) {
            this.queryList.add("  {" +
                    "    \"name\": \"mustard plant\"," +
                    "    \"value\": {" +
                    "      \"type\": \"a plant\"," +
                    "        \"profile\": {" +
                    "        \"status\": \"active\"" +
                    "           }," +
                    "        \"description\": \"The seed of the mustard plant is used as a spice...\"" +
                    "       }," +
                    "       \"associationIds\": [\"" + jobUuid.toString() + "\"]" +
                    "   }");

            this.queryList.add("  {" +
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
                    "   }");

            this.queryList.add(
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
                            "   }");
            this.queryList.add("  {" +
                    "    \"name\": \"wisteria\"," +
                    "    \"value\": {" +
                    "      \"type\": \"a flowering plant\"," +
                    "      \"order\": \" Fabales\", " +
                    "        \"profile\": {" +
                    "        \"status\": \"active\"" +
                    "           }," +
                    "        \"description\": \"native to China, Korea, Japan, and the Eastern United States.\"" +
                    "       }," +
                    "       \"associationIds\": [\"" + jobUuid.toString() + "\", " +
                    "       \"" + schemaUuid.toString() + "\"]" +
                    "   }");
        } else {
            this.queryList.addAll(stringList);
        }
        return this.queryList;
    }

    public MetadataItem createSingleMetadataItem(String query) throws MetadataException, MetadataStoreException, IOException, MetadataQueryException, PermissionException, UUIDException {
        if (StringUtils.isEmpty(query)) {
            query = "{\"value\": {\"title\": \"Example Metadata\", \"properties\": {\"species\": \"arabidopsis\", " +
                    "\"description\": \"A model organism...\"}}, \"name\": \"test metadata\"}\"";
        }
        MetadataSearch search = new MetadataSearch(false, username);
        MetadataSearch spySearch = Mockito.spy(search);

        Mockito.doReturn(createResponseString(jobUuid)).when(spySearch).getValidationResponse(jobUuid.toString());
        Mockito.doReturn(createResponseString(schemaUuid)).when(spySearch).getValidationResponse(schemaUuid.toString());

        JsonFactory factory = new ObjectMapper().getFactory();
        JsonNode jsonMetadataNode = factory.createParser(query).readValueAsTree();
        spySearch.parseJsonMetadata(jsonMetadataNode);
//        Document doc = new Document();
//        doc.parse(query);
//        search.parseDocument(doc);
        spySearch.setOwner(username);
        uuid = new AgaveUUID(UUIDType.METADATA).toString();
        spySearch.setUuid(uuid);
        spySearch.updateMetadataItem();
        return spySearch.getMetadataItem();
    }

    public List<MetadataItem> createMultipleMetadataItems(List<String> queryList) throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        List<MetadataItem> resultList = new ArrayList<>();
        for (String query : queryList){
            resultList.add(createSingleMetadataItem(query));
        }
        return resultList;
    }

    public String createResponseString(AgaveUUID uuid) throws UUIDException {
        return  "  {" +
                "    \"uuid\": \""+ uuid.toString() +"\"," +
                "    \"type\": \""+ uuid.getResourceType().toString() + "\"," +
                "    \"_links\": {" +
                "      \"self\": {" +
                "        \"href\": \""+ TenancyHelper.resolveURLToCurrentTenant(uuid.getObjectReference()) + "\"" +
                "      }" +
                "    }" +
                "  }";
    }

    @Test
    public void insertTest() throws IOException, MetadataException, UUIDException, MetadataQueryException, MetadataStoreException, PermissionException {
        String userQuery = "{\"name\": \"some metadata\"}";
        JsonFactory factory = new ObjectMapper().getFactory();

        MetadataSearch search = new MetadataSearch(true, this.username);
        MetadataSearch spySearch = Mockito.spy(search);
        spySearch.clearCollection();

        //add metadata for associatedID
        MetadataItem associatedItem = createSingleMetadataItem("");
        AgaveUUID associatedUuid = new AgaveUUID(associatedItem.getUuid());

        Mockito.doReturn(createResponseString(associatedUuid)).when(spySearch).getValidationResponse(associatedItem.getUuid());

        //for create/updating
        uuid = new AgaveUUID(UUIDType.METADATA).toString();
        spySearch.setUuid(uuid);
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
        spySearch.parseJsonMetadata(jsonMetadataNode);

        if (spySearch.find(userQuery).size() == 0) {
            spySearch.setOwner(username);
        }

        //insert metadata item
        spySearch.updateMetadataItem();
        Assert.assertEquals(spySearch.findAll().size(), 2);

        List<MetadataItem> findResult = spySearch.find(userQuery);
        Assert.assertEquals(findResult.size(), 1, "Should find 1 result with the inserted metadata item");

    }

    @Test
    public void updateExistingItem() throws MetadataException, MetadataQueryException, MetadataStoreException, IOException, PermissionException, UUIDException {
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
        MetadataItem metadataItem = createSingleMetadataItem("");
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
        Assert.assertEquals(resultAll.size(), 1, "Updating metadata item should not add a new item.");

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
    public void searchTest() throws IOException, MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, UUIDException {
        MetadataSearch search = new MetadataSearch(true, username);
        search.clearCollection();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());
        createMultipleMetadataItems(this.queryList);

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

    @Test
    public void findAllMetadataForUserTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        MetadataSearch implicitSearch = new MetadataSearch(true, this.username);
        MetadataSearch explicitSearch = new MetadataSearch(false, this.username);

        MetadataSearch spyImplicitSearch = Mockito.spy(implicitSearch);
        MetadataSearch spyExplicitSearch = Mockito.spy(explicitSearch);

        Mockito.doReturn(createResponseString(jobUuid)).when(spyImplicitSearch).getValidationResponse(jobUuid.toString());
        Mockito.doReturn(createResponseString(schemaUuid)).when(spyImplicitSearch).getValidationResponse(schemaUuid.toString());

        implicitSearch.clearCollection();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        createMultipleMetadataItems(this.queryList);

        //verify items were added
        List<MetadataItem> resultAll = implicitSearch.findAll();
        Assert.assertEquals(resultAll.size(), 4);

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = implicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 4, "Implicit Search should find 2 metadata items ");

        //only ones the user has been given explicit permission for
        searchResult = explicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 0, "Search should find 0 metadata items because user has not been given explicit permissions for the items.");

    }

    @Test
    public void findPermissionForUuidTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        MetadataSearch search = new MetadataSearch(false, this.username);
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

        MetadataItem metadataItem = createSingleMetadataItem(metadataQueryAgavoideae);
        search.setUuid(metadataItem.getUuid());

        List<MetadataItem> resultAll = search.findAll();
        Assert.assertEquals(resultAll.size(), 1);

        List<MetadataItem>result = search.findPermission_User(username, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 1, "There is only 1 metadataitem with the specified permission");

        result = search.findPermission_User(sharedUser, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 0, "The user doesn't have any permissions specified for the uuid");

        //adding permissions
        search.updatePermissions(sharedUser, "", PermissionType.READ);
        result = search.findPermission_User(sharedUser, metadataItem.getUuid());
        Assert.assertEquals(result.size(), 1, "The user has specified permission for the uuid");

    }


    @Test
    public void validateUuid() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        MetadataSearch search = new MetadataSearch(true, this.username);
        MetadataSearch spySearch = Mockito.spy(search);

        Mockito.doReturn(createResponseString(jobUuid)).when(spySearch).getValidationResponse(jobUuid.toString());
        Mockito.doReturn(createResponseString(schemaUuid)).when(spySearch).getValidationResponse(schemaUuid.toString());

        String invalidUuid = new AgaveUUID(UUIDType.FILE).toString();
        Mockito.doReturn(null).when(spySearch).getValidationResponse(invalidUuid);

        spySearch.clearCollection();
        setQueryList(new ArrayList<>());

        for (String query : this.queryList) {
            uuid = new AgaveUUID(UUIDType.METADATA).toString();
            JsonFactory factory = new ObjectMapper().getFactory();
            JsonNode jsonMetadataNode = factory.createParser(query).readValueAsTree();
            spySearch.parseJsonMetadata(jsonMetadataNode);
            spySearch.setOwner(username);
            spySearch.setUuid(uuid);
            spySearch.updateMetadataItem();
        }

        List<MetadataItem> metadataItemList = spySearch.find("{\"name\": \"mustard plant\"}");
        Assert.assertTrue(metadataItemList.get(0).getAssociations().getAssociatedIds().containsKey(jobUuid.toString()));

        metadataItemList = spySearch.find("{\"name\": \"wisteria\"}");
        Assert.assertTrue(metadataItemList.get(0).getAssociations().getAssociatedIds().containsKey(schemaUuid.toString()));
    }

    @Test
    public void StringToMetadataItemTest() throws IOException, UUIDException, PermissionException, MetadataAssociationException, MetadataException, MetadataStoreException, MetadataQueryException {
        String metadataQueryAloe =
                "  {" +
                        "    \"name\": \"Aloe\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a plant\"" +
                        "}" +
                        "   }";


        MetadataSearch search  = new MetadataSearch(false, this.username);
        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
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
                        "       }," +
                        "       \"associationIds\": [" +
                        "        \"" + aloeMetadataItem.getUuid() + "\"]" +
                        "   }";

        ObjectMapper mapper = new ObjectMapper();
        MetadataItem bean = mapper.readValue(metadataQueryAgavoideae, MetadataItem.class);

        MetadataAssociationList associationList= bean.getAssociations();
        associationList.add(aloeMetadataItem.getUuid());
        bean.setAssociations(associationList);

        try {
            MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(bean);
            System.out.println(metadataItemSerializer.formatMetadataItemResult().toString());
        } catch (Exception e){
            Assert.fail("Serializing MetadataItem to Json String should not throw exception");
        }
    }



}

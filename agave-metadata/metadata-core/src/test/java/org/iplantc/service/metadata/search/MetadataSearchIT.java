package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataAssociationException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.mock;

@Test(groups = {"integration"})
public class MetadataSearchIT {
    String uuid;
    String username = "testuser";
    String sharedUser = "testSharedUser";
    List<String> queryList = new ArrayList<>();
    AgaveUUID jobUuid = new AgaveUUID(UUIDType.JOB);
    AgaveUUID schemaUuid = new AgaveUUID(UUIDType.SCHEMA);

    public List<String> setQueryList(List<String> stringList) {
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
                    "   \"associationIds\": [\"" + jobUuid.toString() + "\"]" +
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

    //model validation

    //exception handling w uuid generation

    //notification

    //schema validation for new metadata items

    //basic permission check

    //basic retrieval operations

    //no data leak - combination of permission + retrieval

    @AfterTest
    public void cleanUpCollection() throws MetadataQueryException {
        try {
            MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
                    org.iplantc.service.common.Settings.METADATA_DB_USER, org.iplantc.service.common.Settings.METADATA_DB_SCHEME, org.iplantc.service.common.Settings.METADATA_DB_PWD.toCharArray());

            MongoClient client = new com.mongodb.MongoClient(
                    new ServerAddress(org.iplantc.service.common.Settings.METADATA_DB_HOST, org.iplantc.service.common.Settings.METADATA_DB_PORT),
                    mongoCredential,
                    MongoClientOptions.builder().build());

            MongoDatabase mongoDatabase = client.getDatabase(org.iplantc.service.common.Settings.METADATA_DB_SCHEME);
            MongoCollection mongoCollection = mongoDatabase.getCollection(Settings.METADATA_DB_COLLECTION, MetadataItem.class);

            mongoCollection.deleteMany(new Document());

        } catch (Exception ex) {
            throw new MetadataQueryException(ex);
        }
    }

//    public MetadataItem createSingleMetadataItem(String query) throws MetadataException, MetadataStoreException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        if (StringUtils.isEmpty(query)) {
//            query = "{\"value\": {\"title\": \"Example Metadata\", \"properties\": {\"species\": \"arabidopsis\", " +
//                    "\"description\": \"A model organism...\"}}, \"name\": \"test metadata\"}\"";
//        }
//        ObjectMapper mapper = new ObjectMapper();
//
//        MetadataSearch search = new MetadataSearch(username);
//        MetadataSearch spySearch = Mockito.spy(search);
//        spySearch.setAccessibleOwnersExplicit();
//
//        MetadataValidation metadataValidation = new MetadataValidation();
//        MetadataValidation spyMetadataValidation = Mockito.spy(metadataValidation);
//
//        MetadataValidation mockMetadataValidation = mock(MetadataValidation.class);
//
////        Mockito.doReturn(createResponseString(jobUuid)).when(spySearch).getValidationResponse(jobUuid.toString());
////        Mockito.doReturn(createResponseString(schemaUuid)).when(spySearch).getValidationResponse(schemaUuid.toString());
//        Mockito.doReturn(createResponseString(jobUuid)).when(spyMetadataValidation).getValidationResponse(jobUuid.toString());
//        Mockito.doReturn(createResponseString(schemaUuid)).when(spyMetadataValidation).getValidationResponse(schemaUuid.toString());
//
//        MetadataValidation mockValidation = mock(MetadataValidation.class);
//
//        AssociatedReference associatedJobReference = new AssociatedReference(jobUuid, createResponseString(jobUuid));
//        AssociatedReference associatedSchemaReference = new AssociatedReference(schemaUuid, createResponseString(schemaUuid));
//        MetadataAssociationList associationList = new MetadataAssociationList();
//        associationList.add(associatedJobReference);
//        associationList.add(associatedSchemaReference);
//
//
//        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIds_uuidApi(mapper.createArrayNode().add(jobUuid.toString()));
//        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIds_uuidApi(mapper.createArrayNode().add(schemaUuid.toString()));
//
//
//        JsonFactory factory = new ObjectMapper().getFactory();
//        JsonNode jsonMetadataNode = factory.createParser(query).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        jsonHandler.setMetadataValidation(mockMetadataValidation);
//        jsonHandler.parseJsonMetadata(jsonMetadataNode);
//        spySearch.setMetadataItem(jsonHandler.getMetadataItem());
//
////        spySearch.parseJsonMetadata(jsonMetadataNode);
//        spySearch.setOwner(username);
//        spySearch.updateMetadataItem();
//        return spySearch.getMetadataItem();
//    }

    //    public List<MetadataItem> createMultipleMetadataItems(List<String> queryList) throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
//        List<MetadataItem> resultList = new ArrayList<>();
//        for (String query : queryList) {
//            resultList.add(createSingleMetadataItem(query));
//        }
//        return resultList;
//    }
//
    public String createResponseString(AgaveUUID uuid) throws UUIDException {
        return "  {" +
                "    \"uuid\": \"" + uuid.toString() + "\"," +
                "    \"type\": \"" + uuid.getResourceType().toString() + "\"," +
                "    \"_links\": {" +
                "      \"self\": {" +
                "        \"href\": \"" + TenancyHelper.resolveURLToCurrentTenant(uuid.getObjectReference()) + "\"" +
                "      }" +
                "    }" +
                "  }";
    }

    public MetadataItem createMetadataItemFromString(String strMetadataItem, String owner) throws MetadataQueryException {
        JsonHandler jsonHandler = new JsonHandler();
        JsonNode node = jsonHandler.parseStringToJson(strMetadataItem);
        jsonHandler.parseJsonMetadata(node);

        MetadataItem metadataItem = jsonHandler.getMetadataItem();
        metadataItem.setOwner(owner);

        return metadataItem;
    }

    @Test
    public void createNewItemTest() throws IOException, MetadataException, UUIDException, MetadataQueryException, MetadataStoreException, PermissionException {
        String strItemToAdd = "  {" +
                "    \"name\": \"mustard plant\"," +
                "    \"value\": {" +
                "      \"type\": \"a plant\"," +
                "        \"profile\": {" +
                "        \"status\": \"active\"" +
                "           }," +
                "        \"description\": \"The seed of the mustard plant is used as a spice...\"" +
                "       }," +
                "   \"associationIds\": [\"" + jobUuid.toString() + "\"]," +
                "   \"schemaId\": \"" + schemaUuid.toString() + "\""+
                "   }";

//        JsonHandler handler = new JsonHandler();
//        JsonNode node = handler.parseStringToJson(strItemToAdd);
//        handler.parseJsonMetadata(node);
//
//        MetadataItem toAddItem = handler.getMetadataItem();
//        toAddItem.setOwner(this.username);

        MetadataItem toAddItem = createMetadataItemFromString(strItemToAdd, this.username);

        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(toAddItem);
        MetadataItem addedItem = search.updateMetadataItem();

        Assert.assertEquals(addedItem, toAddItem, "updateMetadataItem should return the MetadataItem successfully added/updated.");

        List<MetadataItem> foundItems = search.find("{\"uuid\": \"" + toAddItem.getUuid() + "\"}");
        Assert.assertEquals(foundItems.get(0), toAddItem, "MetadataItem found with matching uuid should match the MetadataItem added.");

    }

    @Test
    public void updateExistingItemAsOwner() throws MetadataException, MetadataQueryException, MetadataStoreException, IOException, PermissionException, UUIDException {
        MetadataSearch createItem = new MetadataSearch(this.username);
        createItem.clearCollection();
        createItem.setAccessibleOwnersImplicit();

        String toAdd =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"title\": \" Asparagales\", " +
                        "        \"properties\": {" +
                        "        \"species\": \"wisteria\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "   }";


//
//        JsonHandler jsonHandler = new JsonHandler();
//        JsonNode jsonNode = jsonHandler.parseStringToJson(toAdd);
//        jsonHandler.parseJsonMetadata(jsonNode);


        MetadataItem toAddItem = createMetadataItemFromString(toAdd, this.username);
//        toAddItem.setOwner(this.username);
        createItem.setMetadataItem(toAddItem);
        createItem.updateMetadataItem();

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

//        JsonHandler updatedJsonHandler = new JsonHandler();
//        JsonNode updatedJsonNode = updatedJsonHandler.parseStringToJson(strUpdate);
//        updatedJsonHandler.parseJsonMetadata(updatedJsonNode);

        MetadataItem toUpdateItem = createMetadataItemFromString(strUpdate, this.username);

        MetadataSearch updateItem = new MetadataSearch(this.username);
        updateItem.setAccessibleOwnersImplicit();
        updateItem.setMetadataItem(toUpdateItem);

        updateItem.setUuid(createItem.getUuid());
        MetadataItem existingItem = updateItem.findOne();
        updateItem.setOwner(existingItem.getOwner());
        updateItem.updateMetadataItem();

        String queryAfterUpdate = "{\"name\": \"New Metadata\", \"value.properties.species\": \"wisteria\"}";
        List<MetadataItem> result = updateItem.find(queryAfterUpdate);
        Assert.assertEquals(result.get(0).getValue().get("properties").get("description").textValue(),
                "A model flower organism...");
    }

//    @Test
//    public void updateExistingItemAsSharedUserWithRead() throws IOException, MetadataStoreException, MetadataException, MetadataQueryException, PermissionException, UUIDException {
//        MetadataSearch searchAsOwner = new MetadataSearch(username);
//        searchAsOwner.clearCollection();
//        searchAsOwner.setAccessibleOwnersExplicit();
////        MetadataItem metadataItem = createSingleMetadataItem("");
//        MetadataItem metadataItem = createMetadataItemFromString("");
//        searchAsOwner.updatePermissions(sharedUser, "", PermissionType.READ);
//
//        MetadataSearch search = new MetadataSearch(sharedUser);
//        search.setAccessibleOwnersExplicit();
//
//        String strUpdate =
//                "  {" +
//                        "    \"name\": \"New Metadata\"," +
//                        "    \"value\": {" +
//                        "      \"title\": \"Changed Metadata Title\"," +
//                        "      \"properties\": {" +
//                        "        \"species\": \"wisteria\"," +
//                        "        \"description\": \"A model flower organism...\"" +
//                        "       }" +
//                        "       }" +
//                        "   }";
//
//        JsonFactory factory = new ObjectMapper().getFactory();
//        JsonNode jsonMetadataNode = factory.createParser(strUpdate).readValueAsTree();
//        search.parseJsonMetadata(jsonMetadataNode);
//        search.setUuid(metadataItem.getUuid());
//
//        try {
//            MetadataItem updatedMetadataItem = search.updateMetadataItem();
//        } catch (PermissionException p) {
//            Assert.assertEquals(p.getMessage(), "User does not have sufficient access to edit public metadata item.");
//        }
//    }
//
//    @Test
//    public void updateExistingItemAsSharedUserWithReadWrite() throws IOException, MetadataStoreException, MetadataException, MetadataQueryException, PermissionException, UUIDException {
//        MetadataSearch searchAsOwner = new MetadataSearch(username);
//        searchAsOwner.clearCollection();
//        searchAsOwner.setAccessibleOwnersImplicit();
//
//        MetadataItem metadataItem = createSingleMetadataItem("");
//        searchAsOwner.setMetadataItem(metadataItem);
//        searchAsOwner.updatePermissions(sharedUser, "", PermissionType.READ_WRITE);
//
//        MetadataSearch search = new MetadataSearch(sharedUser);
//        search.setAccessibleOwnersImplicit();
//
//        search.setMetadataItem(metadataItem);
//        String strUpdate =
//                "  {" +
//                        "    \"name\": \"New Metadata\"," +
//                        "    \"value\": {" +
//                        "      \"title\": \"Changed Metadata Title\"," +
//                        "      \"properties\": {" +
//                        "        \"species\": \"wisteria\"," +
//                        "        \"description\": \"A model flower organism...\"" +
//                        "       }" +
//                        "       }" +
//                        "   }";
//
//        JsonFactory factory = new ObjectMapper().getFactory();
//        JsonNode jsonMetadataNode = factory.createParser(strUpdate).readValueAsTree();
//        search.parseJsonMetadata(jsonMetadataNode);
//        search.setUuid(metadataItem.getUuid());
//        search.setOwner(username);
//
//        List<MetadataItem> allResult = search.findAll();
//        MetadataItem updatedMetadataItem = search.updateMetadataItem();
//
//        String queryAfterUpdate = "{\"name\": \"New Metadata\", \"value.title\": \"Changed Metadata Title\"}";
//        queryAfterUpdate = "{\"name\": \"New Metadata\"}";
//        List<MetadataItem> result = search.find(queryAfterUpdate);
//        allResult = search.findAll();
//
//        Assert.assertEquals(result.get(0), updatedMetadataItem);
//    }
//
//    public void updateExistingItemAsSharedUserWithNoPermission() throws IOException, MetadataStoreException, MetadataException, MetadataQueryException, PermissionException, UUIDException {
//        MetadataSearch searchAsOwner = new MetadataSearch(username);
//        searchAsOwner.clearCollection();
//        searchAsOwner.setAccessibleOwnersImplicit();
//
//        MetadataItem metadataItem = createSingleMetadataItem("");
//
//        MetadataSearch search = new MetadataSearch(sharedUser);
//        search.setAccessibleOwnersImplicit();
//
//        String strUpdate =
//                "  {" +
//                        "    \"name\": \"New Metadata\"," +
//                        "    \"value\": {" +
//                        "      \"title\": \"Changed Metadata Title\"," +
//                        "      \"properties\": {" +
//                        "        \"species\": \"wisteria\"," +
//                        "        \"description\": \"A model flower organism...\"" +
//                        "       }" +
//                        "       }" +
//                        "   }";
//
//        JsonFactory factory = new ObjectMapper().getFactory();
//        JsonNode jsonMetadataNode = factory.createParser(strUpdate).readValueAsTree();
//        search.parseJsonMetadata(jsonMetadataNode);
//        search.setUuid(metadataItem.getUuid());
//
//        try {
//            MetadataItem updatedMetadataItem = search.updateMetadataItem();
//        } catch (PermissionException p) {
//            Assert.assertEquals(p.getMessage(), "User does not have sufficient access to edit public metadata item.");
//        }
//    }

    @Mock
    AuthorizationHelper mockAuthorizationHelper;

    @InjectMocks
    MetadataSearch mockSearch;

    @Test
    public void findAllMetadataForUserImplicitSearchTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        MetadataSearch implicitSearch = new MetadataSearch(this.username);

        MetadataSearch spyImplicitSearch = Mockito.spy(implicitSearch);

        spyImplicitSearch.clearCollection();
        spyImplicitSearch.setAccessibleOwnersImplicit();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }

//        createMultipleMetadataItems(this.queryList);

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = spyImplicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 4, "Implicit Search should find 4 metadata items ");
    }

    @Test
    public void findAllMetadataForUserExplicitSearchTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, UUIDException {
        MetadataSearch explicitSearch = new MetadataSearch(this.sharedUser);

        MetadataSearch spyExplicitSearch = Mockito.spy(explicitSearch);
        spyExplicitSearch.clearCollection();
        spyExplicitSearch.setAccessibleOwnersExplicit();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }
//        createMultipleMetadataItems(this.queryList);

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = spyExplicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 0, "Search should find 0 metadata items because user has not been given explicit permissions for the items.");

    }

    @Test
    public void regexSearchTest() throws IOException, MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, UUIDException {
        MetadataSearch search = new MetadataSearch(username);
        search.setAccessibleOwnersImplicit();

        search.clearCollection();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }
//        createMultipleMetadataItems(this.queryList);

        String queryByValueRegex = "{ \"value.description\": { \"$regex\": \".*monocots.*\", \"$options\": \"m\"}}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByValueRegex);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: cactus");
    }

    @Test
    public void nameSearchTest() throws MetadataQueryException, MetadataException, MetadataStoreException, UUIDException, PermissionException, IOException {
        MetadataSearch search = new MetadataSearch(username);
        search.clearCollection();
        search.setAccessibleOwnersImplicit();


        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }
//        createMultipleMetadataItems(this.queryList);

        String queryByName = "{\"name\":\"mustard plant\"}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByName);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: mustard plant");
    }

    @Test
    public void nestedValueSearchTest() throws MetadataQueryException, MetadataException, MetadataStoreException, UUIDException, PermissionException, IOException {
        MetadataSearch search = new MetadataSearch(username);
        search.clearCollection();
        search.setAccessibleOwnersImplicit();


        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }
//        createMultipleMetadataItems(this.queryList);

        String queryByValue = "{\"value.type\":\"a plant\"}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByValue);

        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: mustard plant and cactus");
    }

    @Test
    public void conditionalSearchTest() throws MetadataQueryException, MetadataException, MetadataStoreException, UUIDException, PermissionException, IOException {
        MetadataSearch search = new MetadataSearch(username);
        search.clearCollection();
        search.setAccessibleOwnersImplicit();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.updateMetadataItem();
        }
//        createMultipleMetadataItems(this.queryList);

        String queryByValueConditional = "{" +
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
        resultList = search.find(queryByValueConditional);

        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: cactus and Agavoideae");
    }

    @Test
    public void deleteMetadataItemAsOwnerTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
        String metadataQueryAloe =
                "  {" +
                        "    \"name\": \"Aloe\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a plant\"" +
                        "}" +
                        "   }";


        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();


        MetadataItem aloeMetadataItem = createMetadataItemFromString(metadataQueryAloe, this.username);
//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);

        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setAccessibleOwnersExplicit();

        ownerSearch.setUuid(aloeMetadataItem.getUuid());
        MetadataItem itemToDelete = ownerSearch.findOne();
        ownerSearch.setMetadataItem(itemToDelete);
        MetadataItem deletedItem = ownerSearch.deleteMetadataItem();

        Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");
        Assert.assertEquals(deletedItem.getName(), "Aloe", "Deleted metadata item returned should have name \"Aloe\"");

    }

//    @Test
//    public void deleteMetadataItemAsReadUserTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        String metadataQueryAloe =
//                "  {" +
//                        "    \"name\": \"Aloe\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a plant\"" +
//                        "}" +
//                        "   }";
//
//
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersExplicit();
//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
//        search.updatePermissions(this.sharedUser, "", PermissionType.READ);
//
//        MetadataSearch readSearch = new MetadataSearch(this.sharedUser);
//        readSearch.setAccessibleOwnersExplicit();
//
//        readSearch.setUuid(aloeMetadataItem.getUuid());
//        MetadataItem itemToDelete = readSearch.findOne();
//        readSearch.setMetadataItem(itemToDelete);
//
//        Assert.assertThrows(PermissionException.class, () -> readSearch.deleteMetadataItem());
//    }
//
//    @Test
//    public void deleteMetadataItemAsReadWriteUserTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        String metadataQueryAloe =
//                "  {" +
//                        "    \"name\": \"Aloe\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a plant\"" +
//                        "}" +
//                        "   }";
//
//
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersExplicit();
//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
//        search.updatePermissions(this.sharedUser, "", PermissionType.READ_WRITE);
//
//        MetadataSearch readWriteSearch = new MetadataSearch(this.username);
//        readWriteSearch.setAccessibleOwnersExplicit();
//
//        readWriteSearch.setUuid(aloeMetadataItem.getUuid());
//        MetadataItem itemToDelete = readWriteSearch.findOne();
//        readWriteSearch.setMetadataItem(itemToDelete);
//        MetadataItem deletedItem = readWriteSearch.deleteMetadataItem();
//
//        Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");
//        Assert.assertEquals(deletedItem.getName(), "Aloe", "Deleted metadata item returned should have name \"Aloe\"");
//
//    }
//
//    @Test
//    public void deleteMetadataItemAsWriteUserTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        String metadataQueryAloe =
//                "  {" +
//                        "    \"name\": \"Aloe\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a plant\"" +
//                        "}" +
//                        "   }";
//
//
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersExplicit();
//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
//        search.updatePermissions(this.sharedUser, "", PermissionType.WRITE);
//
//        MetadataSearch writeSearch = new MetadataSearch(this.username);
//        writeSearch.setAccessibleOwnersExplicit();
//
//        writeSearch.setUuid(aloeMetadataItem.getUuid());
//        MetadataItem itemToDelete = writeSearch.findOne();
//        writeSearch.setMetadataItem(itemToDelete);
//        MetadataItem deletedItem = writeSearch.deleteMetadataItem();
//
//        Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");
//        Assert.assertEquals(deletedItem.getName(), "Aloe", "Deleted metadata item returned should have name \"Aloe\"");
//
//    }
//
//    @Test
//    public void deleteMetadataItemWithNoPermissionTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        String metadataQueryAloe =
//                "  {" +
//                        "    \"name\": \"Aloe\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a plant\"" +
//                        "}" +
//                        "   }";
//
//
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersExplicit();
//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
//        search.updatePermissions(this.sharedUser, "", PermissionType.NONE);
//
//        MetadataSearch noneSearch = new MetadataSearch(this.sharedUser);
//        noneSearch.setAccessibleOwnersExplicit();
//        noneSearch.setUuid(aloeMetadataItem.getUuid());
//        MetadataItem itemToDelete = noneSearch.findOne();
//        noneSearch.setMetadataItem(itemToDelete);
//
//        Assert.assertThrows(PermissionException.class, () -> noneSearch.deleteMetadataItem());
//    }

    @Test
    public void StringToMetadataItemTest() throws IOException, UUIDException, PermissionException, MetadataException, MetadataStoreException, MetadataQueryException, MetadataAssociationException {
        String metadataQueryAloe =
                "  {" +
                        "    \"name\": \"Aloe\"," +
                        "    \"value\": {" +
                        "      \"type\": \"a plant\"" +
                        "}" +
                        "   }";


        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();

//        MetadataItem aloeMetadataItem = createSingleMetadataItem(metadataQueryAloe);
        MetadataItem aloeMetadataItem = createMetadataItemFromString(metadataQueryAloe, this.username);
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

        MetadataAssociationList associationList = bean.getAssociations();
        associationList.add(aloeMetadataItem.getUuid());
        bean.setAssociations(associationList);

        try {
            MetadataItemSerializer metadataItemSerializer = new MetadataItemSerializer(bean);
//            System.out.println(metadataItemSerializer.formatMetadataItemResult().toString());
        } catch (Exception e) {
            Assert.fail("Serializing MetadataItem to Json String should not throw exception");
        }
    }

//    @Test
//    public void findMetadataWithoutPermissionTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.clearCollection();
//        search.setAccessibleOwnersExplicit();
//
//        String metadataQueryAgavoideae =
//                "  {" +
//                        "    \"name\": \"Agavoideae\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a flowering plant\"," +
//                        "      \"order\": \" Asparagales\", " +
//                        "      \"properties\": {" +
//                        "        \"profile\": {" +
//                        "        \"status\": \"paused\"" +
//                        "           }," +
//                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
//                        "       }" +
//                        "       }" +
//                        "   }";
//        //add item
//        MetadataItem metadataItem = createSingleMetadataItem(metadataQueryAgavoideae);
//        search.setUuid(metadataItem.getUuid());
//        search.updatePermissions(sharedUser, "", PermissionType.READ);
//        search.updatePermissions("readWriteUser", "", PermissionType.READ_WRITE);
//        search.updatePermissions("writeUser", "", PermissionType.WRITE);
//
//        String invalidUser = "invalidUser";
//        MetadataSearch searchAsUserWithoutPermission = new MetadataSearch(invalidUser);
//        searchAsUserWithoutPermission.setUsername(invalidUser);
//        searchAsUserWithoutPermission.setAccessibleOwnersExplicit();
//        List<MetadataItem> newResult = searchAsUserWithoutPermission.findPermission_User(invalidUser, metadataItem.getUuid());
//        Assert.assertNull(newResult, "User without permissions to view metadata item should return null value.");
//
//    }

    @Test
    public void findMetadataWithFiltersTest() throws MetadataStoreException, MetadataException, IOException, MetadataQueryException, PermissionException, UUIDException {
        ObjectMapper mapper = new ObjectMapper();

        MetadataItem testEntity = new MetadataItem();
        testEntity.setName("wisteria");
        String value =
                "    {" +
                        "      \"type\": \"a flowering plant\"," +
                        "      \"order\": \" Fabales\", " +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"native to China, Korea, Japan, and the Eastern United States.\"" +
                        "       }}";
        testEntity.setValue(mapper.getFactory().createParser(value).readValueAsTree());
        testEntity.setOwner(username);

        MetadataSearch toAdd = new MetadataSearch(username);
        toAdd.setMetadataItem(testEntity);
        toAdd.setAccessibleOwnersExplicit();
        toAdd.updateMetadataItem();

        String strQuery = "{ \"value.type\": { \"$regex\": \".*flowering.*\"}}";
        String[] filters = {"name", "value.type"};

        MetadataSearch search = new MetadataSearch(username);
        List<Document> foundItems = search.filterFind(strQuery, filters);
        Assert.assertEquals(foundItems.size(), 1);
        Assert.assertTrue(Arrays.asList("wisteria", "Agavoideae").contains(foundItems.get(0).get("name")));
        Assert.assertTrue(foundItems.get(0).getEmbedded(List.of("value", "type"), String.class).contains("flowering"));
    }
}

package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.apache.commons.lang3.StringUtils;
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
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.iplantc.service.notification.managers.NotificationManager;
import org.joda.time.DateTime;
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

    @Mock
    AuthorizationHelper mockAuthorizationHelper;

    @InjectMocks
    MetadataSearch mockSearch;

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
                    "       \"associationIds\": [\"" + jobUuid.toString() + "\"], " +
                    "       \"schemaId\": \"" + schemaUuid.toString() + "\"" +
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

    //no data leak - combination of permission + retrieval

    @BeforeClass
    public void setup(){
        createSchema();
    }

    @AfterMethod
    public void cleanUpCollection() throws MetadataQueryException {
        try {
            MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
                    org.iplantc.service.common.Settings.METADATA_DB_USER, org.iplantc.service.common.Settings.METADATA_DB_SCHEME, org.iplantc.service.common.Settings.METADATA_DB_PWD.toCharArray());

            MongoClient client = new com.mongodb.MongoClient(
                    new ServerAddress(org.iplantc.service.common.Settings.METADATA_DB_HOST, org.iplantc.service.common.Settings.METADATA_DB_PORT),
                    mongoCredential,
                    MongoClientOptions.builder().build());

            MongoDatabase mongoDatabase = client.getDatabase(Settings.METADATA_DB_SCHEME);
            MongoCollection mongoCollection = mongoDatabase.getCollection(Settings.METADATA_DB_COLLECTION, MetadataItem.class);
            MongoCollection mongoSchemaCollection = mongoDatabase.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);

            mongoCollection.deleteMany(new Document());
            mongoSchemaCollection.deleteMany(new Document());

        } catch (Exception ex) {
            throw new MetadataQueryException(ex);
        }
    }

    public String createSchema() {
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());
        MongoClient mongoClient = new com.mongodb.MongoClient(
                new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT),
                mongoCredential,
                MongoClientOptions.builder().build());
        MongoDatabase mongoDatabase = mongoClient.getDatabase(org.iplantc.service.metadata.Settings.METADATA_DB_SCHEME);
        MongoCollection mongoCollection = mongoDatabase.getCollection(org.iplantc.service.metadata.Settings.METADATA_DB_SCHEMATA_COLLECTION);


        String strItemToAdd = "  {" +
                "    \"name\": \"Sample Name\"," +
                "    \"value\": {" +
                "      \"type\": \"Sample type\"," +
                "        \"profile\": {" +
                "        \"status\": \"Sample Status\"" +
                "           }," +
                "        \"description\": \"Sample description...\"" +
                "       }," +
                "   }";

        Document doc;
        String timestamp = new DateTime().toString();
            doc = new Document("internalUsername", this.username)
                    .append("lastUpdated", timestamp)
                    .append("schema", ServiceUtils.escapeSchemaRefFieldNames(strItemToAdd));

        doc.put("uuid", schemaUuid.toString());
        doc.append("created", timestamp);
        doc.append("owner", this.username);
        doc.append("tenantId", TenancyHelper.getCurrentTenantId());

        mongoCollection.insertOne(doc);

        return schemaUuid.toString();
    }

    public MetadataItem createMetadataItemFromString(String strMetadataItem, String owner) throws MetadataQueryException, UUIDException, MetadataException, PermissionException, MetadataAssociationException {
        ObjectMapper mapper = new ObjectMapper();
        MetadataValidation mockValidation = mock(MetadataValidation.class);

        MetadataAssociationList associationList = new MetadataAssociationList();
        associationList.add(jobUuid.toString());

        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIds_uuidApi(mapper.createArrayNode().add(jobUuid.toString()));
        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.setMetadataValidation(mockValidation);

        JsonNode node = jsonHandler.parseStringToJson(strMetadataItem);
        jsonHandler.parseJsonMetadata(node);

        MetadataItem metadataItem = jsonHandler.getMetadataItem();
        metadataItem.setOwner(owner);

        return metadataItem;
    }

    //basic operations
    @Test
    public void createNewItemTest() throws MetadataException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException {
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
                "   \"schemaId\": \"" + schemaUuid.toString() + "\"" +
                "   }";

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
    public void updateExistingItemAsOwner() throws MetadataException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException {
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

        MetadataItem toAddItem = createMetadataItemFromString(toAdd, this.username);
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

    @Test
    public void findMetadataWithFiltersTest() throws MetadataException, IOException, MetadataQueryException, PermissionException {
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
        List<MetadataItem> all = search.findAll(new String[0]);


        List<Document> foundItems = search.filterFind(strQuery, filters);
        Assert.assertEquals(foundItems.size(), 1);
        Assert.assertTrue(Arrays.asList("wisteria", "Agavoideae").contains(foundItems.get(0).get("name")));
        Assert.assertTrue(foundItems.get(0).getEmbedded(List.of("value", "type"), String.class).contains("flowering"));
    }

    @Test
    public void findAllMetadataForUserImplicitSearchTest() throws MetadataException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException {
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

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = spyImplicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 4, "Implicit Search should find 4 metadata items ");
    }

    @Test
    public void findAllMetadataForUserExplicitSearchTest() throws MetadataException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException {
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

        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = spyExplicitSearch.find(userQuery);
        Assert.assertEquals(searchResult.size(), 0, "Search should find 0 metadata items because user has not been given explicit permissions for the items.");

    }

    @Test
    public void findRegexSearchTest() throws MetadataException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException {
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

        String queryByValueRegex = "{ \"value.description\": { \"$regex\": \".*monocots.*\", \"$options\": \"m\"}}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByValueRegex);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: cactus");
    }

    @Test
    public void findNameSearchTest() throws MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException {
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

        String queryByName = "{\"name\":\"mustard plant\"}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByName);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: mustard plant");
    }

    @Test
    public void findNestedValueSearchTest() throws MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException {
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

        String queryByValue = "{\"value.type\":\"a plant\"}";
        List<MetadataItem> resultList;
        resultList = search.find(queryByValue);

        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: mustard plant and cactus");
    }

    @Test
    public void findConditionalSearchTest() throws MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException {
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
    public void deleteMetadataItemAsOwnerTest() throws MetadataQueryException, PermissionException, MetadataException, UUIDException, MetadataAssociationException {
        //create metadata item to delete
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

        MetadataSearch addItem = new MetadataSearch(this.username);
        addItem.setAccessibleOwnersImplicit();
        addItem.setMetadataItem(aloeMetadataItem);
        addItem.updateMetadataItem();

        //find item to delete
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setAccessibleOwnersExplicit();
        ownerSearch.setUuid(aloeMetadataItem.getUuid());
        MetadataItem itemToDelete = ownerSearch.findOne();
        ownerSearch.setMetadataItem(itemToDelete);

        MetadataItem deletedItem = ownerSearch.deleteMetadataItem();

        Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");
        Assert.assertEquals(deletedItem.getName(), "Aloe", "Deleted metadata item returned should have name \"Aloe\"");

        MetadataItem itemAfterDelete = ownerSearch.findOne();
        Assert.assertNull(itemAfterDelete, "Item should not be found after deleting.");
    }

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

}

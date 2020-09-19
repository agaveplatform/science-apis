package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.auth.AuthorizationHelper;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.*;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.joda.time.DateTime;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(groups = {"integration"})
public class MetadataSearchIT {
    String uuid;
    String username = "TEST_USER";
    String sharedUser = "SHARED_USER";
    List<String> queryList = new ArrayList<>();
    AgaveUUID jobUuid = new AgaveUUID(UUIDType.JOB);
    AgaveUUID schemaUuid = new AgaveUUID(UUIDType.SCHEMA);
    ObjectMapper mapper = new ObjectMapper();


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
    public void setup() {
        createSchema();
    }

    @AfterMethod
    public void cleanUpCollection()  {
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
            fail("Unable to clean up collection after tests, " + ex.getMessage());
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

        String strItemToAdd = "{" +
                "\"order\": \"sample order\"," +
                "\"type\": \"object\"," +
                "\"properties\" : {" +
                "\"profile\" : {\"type\": \"string\"}," +
                "\"status\": {\"enum\" : [\"active\", \"retired\", \"disabled\", \"banned\"]} " +
                "}, " +
                "\"description\": {\"type\" : \"string\"}" +
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

    public MetadataItem createMetadataItem() throws MetadataException, PermissionException, MetadataAssociationException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setOwner(username);
        metadataItem.setInternalUsername(username);
        metadataItem.setName("Agavoideae");
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        MetadataAssociationList associationList = new MetadataAssociationList();
        associationList.add(jobUuid.toString());
        metadataItem.setAssociations(associationList);
        metadataItem.setPermissions(Arrays.asList(new MetadataPermission(this.sharedUser, PermissionType.READ)));

        return metadataItem;
    }

    public MetadataItem createMetadataItemFromString(String strMetadataItem, String owner) throws MetadataValidationException, MetadataQueryException, UUIDException, MetadataException, PermissionException, MetadataAssociationException {
        ObjectMapper mapper = new ObjectMapper();
        MetadataValidation mockValidation = mock(MetadataValidation.class);

        MetadataAssociationList associationList = new MetadataAssociationList();
        associationList.add(jobUuid.toString());

        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIdsUuidApi(mapper.createArrayNode().add(jobUuid.toString()));
        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.setMetadataValidation(mockValidation);

        JsonNode node = jsonHandler.parseStringToJson(strMetadataItem);
        jsonHandler.parseJsonMetadata(node);

        MetadataItem metadataItem = jsonHandler.getMetadataItem();
        metadataItem.setOwner(owner);
        metadataItem.setInternalUsername(owner);
        metadataItem.setPermissions(Arrays.asList(new MetadataPermission(this.sharedUser, PermissionType.READ)));

        return metadataItem;
    }

    //basic operations

    @DataProvider(name="initMetadataItemDataProvider")
    public Object[][] initMetadataItemDataProvider() throws MetadataValidationException, MetadataAssociationException, UUIDException, PermissionException, MetadataQueryException, MetadataException {
        return new Object[][]{
                {createMetadataItem(), false, "Valid metadata item should be returned and not throw Exceptions."},
                {new MetadataItem(), true, "Missing fields should throw Exception when interacting with database."},
                {null, true, "Null MetadataItem should throw MetadataException when interacting with database."}
        };
    }

    @Test (dataProvider = "initMetadataItemDataProvider")
    public void createNewItemTest(MetadataItem metadataItem, boolean bolThrowException, String message)  {
        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);

        try {
            MetadataItem addedItem = search.insertCurrentMetadataItem();
            if (bolThrowException)
                fail("Exception should be thrown: " + message);

            Assert.assertEquals(addedItem, metadataItem, "Item to be added should be returned on successful insert.");
            List<MetadataItem> foundItems = search.find(eq("uuid", metadataItem.getUuid()));
            for (MetadataItem foundItem : foundItems)
                Assert.assertEquals(foundItem, metadataItem, "MetadataItem found matching the added MetadataItem should be equal.");
        } catch (Exception e) {
            if (!bolThrowException)
                fail(message);

            if (e.getClass().equals(MetadataException.class))
                assertEquals(e.getMessage(), "Cannot insert a null MetadataItem", message);
            else
                assertEquals(e.getMessage(), "value can not be null", message);

        }

    }

    @Test
    public void updateExistingItemAsOwner() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        MetadataItem toAdd = createMetadataItem();
        MetadataSearch createItem = new MetadataSearch(this.username);
        createItem.setMetadataItem(toAdd);
        MetadataItem addedItem = createItem.insertCurrentMetadataItem();

//        MetadataSearch createItem = new MetadataSearch(this.username);
////        createItem.clearCollection();
//        createItem.setAccessibleOwnersImplicit();
//
//        String toAdd =
//                "  {" +
//                        "    \"name\": \"Agavoideae\"," +
//                        "    \"value\": {" +
//                        "      \"title\": \" Asparagales\", " +
//                        "        \"properties\": {" +
//                        "        \"species\": \"wisteria\"" +
//                        "           }," +
//                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
//                        "       }" +
//                        "   }";
//
//        MetadataItem toAddItem = createMetadataItemFromString(toAdd, this.username);
//        createItem.setMetadataItem(toAddItem);
//        createItem.insertCurrentMetadataItem();

        addedItem.setName("New Metadata");
        addedItem.setValue(mapper.createObjectNode()
            .put("title", "Update Metadata Title")
            .putObject("properties")
            .put("species", "wisteria")
            .put("description", "A model flower organism..."));


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
//        JsonHandler updateJsonHandler = new JsonHandler();
//        JsonNode updateJsonNode = updateJsonHandler.parseStringToJson(strUpdate);
//        Document toUpdateItem = updateJsonHandler.parseJsonMetadataToDocument(updateJsonNode);
//        JsonNode updatedValueNode = updateJsonHandler.parseValueToJsonNode(updateJsonNode);
//
//        MetadataSearch updateItem = new MetadataSearch(this.username);
//        updateItem.setAccessibleOwnersImplicit();
//        updateItem.setUuid(createItem.getUuid());
//        updateItem.updateMetadataItem(toUpdateItem);
//
//        String queryAfterUpdate = "{\"name\": \"New Metadata\", \"value.properties.species\": \"wisteria\"}";
//        List<MetadataItem> result = updateItem.find(queryAfterUpdate);
//        MetadataItem updatedItem = result.get(0);
//
//        Assert.assertEquals(updatedItem.getValue(), updatedValueNode, "MetadataItem value should be updated.");
//        Assert.assertEquals(updatedItem.getValue().get("properties").get("description").textValue(),
//                "A model flower organism...");
//        Assert.assertEquals(updatedItem.getName(), "New Metadata",
//                "MetadataItem name should be updated to \"New Metadata\".");
//        Assert.assertEquals(updatedItem.getSchemaId(), toAddItem.getSchemaId(),
//                "MetadataItem schemaId should not be changed since it was not included in the update.");
//        Assert.assertEquals(updatedItem.getOwner(), toAddItem.getOwner(),
//                "MetadataItem owner should not be changed.");
//        Assert.assertEquals(updatedItem.getUuid(), toAddItem.getUuid(),
//                "MetadataItem uuid should not be changed.");
//        Assert.assertEquals(updatedItem.getTenantId(), toAddItem.getTenantId(), "" +
//                "MetadataItem tenantId should not be changed.");
//        Assert.assertEquals(updatedItem.getPermissions(), toAddItem.getPermissions(),
//                "MetadataItem permissions should not be changed since it was not included in the update.");
//        Assert.assertTrue(updatedItem.getAssociations().getAssociatedIds().keySet().equals(toAddItem.getAssociations().getAssociatedIds().keySet()), "MetadataItem associatedIds should not be changed since it was not included in the update.");
//        Assert.assertEquals(updatedItem.getNotifications(), toAddItem.getNotifications(),
//                "MetadataItem notifications should not be changed since it was not included in the update.");
//        Assert.assertTrue((updatedItem.getCreated().compareTo(toAddItem.getCreated()) == 0),
//                "MetadataItem created date should not be changed.");
//        Assert.assertTrue((updatedItem.getLastUpdated().compareTo(toAddItem.getLastUpdated()) >= 1),
//                "MetadataItem lastUpdated should be updated to after the initial item's lastUpdated date.");
    }

    @Test
    public void findMetadataWithFiltersTest() throws MetadataException, IOException, MetadataQueryException, MetadataStoreException {
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
        toAdd.insertCurrentMetadataItem();

        String strQuery = "{ \"value.type\": { \"$regex\": \".*flowering.*\"}}";
        String[] filters = {"name", "value.type", "lastUpdated"};

        MetadataSearch search = new MetadataSearch(username);
//        List<MetadataItem> all = search.findAll();

        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument(strQuery);
        List<Document> foundItems = search.filterFind(docQuery, filters);
        Assert.assertEquals(foundItems.size(), 1);
        Assert.assertTrue(Arrays.asList("wisteria", "Agavoideae").contains(foundItems.get(0).get("name")));
        Assert.assertTrue(foundItems.get(0).getEmbedded(List.of("value", "type"), String.class).contains("flowering"));

    }

    @Test
    public void findAllMetadataForUserImplicitSearchTest() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, Settings.PUBLIC_USER_USERNAME);
            MetadataSearch addSearch = new MetadataSearch(Settings.PUBLIC_USER_USERNAME);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
        }
//        String userQuery = "";
        List<MetadataItem> searchResult;
        MetadataSearch implicitSearch = new MetadataSearch(this.username);
        implicitSearch.setAccessibleOwnersImplicit();

        searchResult = implicitSearch.find(new Document());
        Assert.assertEquals(searchResult.size(), 0, "Implicit Search should find 0 metadata items ");
    }

    @Test
    public void findAllMetadataForUserExplicitSearchTest() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, Settings.PUBLIC_USER_USERNAME);
            MetadataSearch addSearch = new MetadataSearch(Settings.PUBLIC_USER_USERNAME);
            addSearch.setAccessibleOwnersExplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
        }

        MetadataSearch explicitSearch = new MetadataSearch(this.username);
        explicitSearch.setAccessibleOwnersExplicit();

//        String userQuery = "";
        List<MetadataItem> searchResult;

        searchResult = explicitSearch.find(new Document());
        Assert.assertEquals(searchResult.size(), 4, "Explicit Search should find 4 metadata items ");

        for (MetadataItem foundItem : searchResult) {
            MetadataPermissionManager pemManager = new MetadataPermissionManager(foundItem, this.username);
            if (pemManager.canRead(username)) {
                //pass
            } else {
                fail("Users should be able to read metadata items.");
            }
        }
    }

    @Test
    public void findMetadataItemAsSharedUser() throws MetadataValidationException, MetadataQueryException, MetadataException, PermissionException, MetadataStoreException {
        //parse json
        String metadataQueryAgavoideae =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"order\": \" Asparagales\", " +
                        "      \"properties\": {" +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "       }," +
                        "       \"schemaId\": \"" + uuid + "\"" +
                        "   }";

        //validate metadata item from json handler
        JsonHandler agavoideaeJsonHandler = new JsonHandler();
        JsonNode agaveJsonNode = agavoideaeJsonHandler.parseStringToJson(metadataQueryAgavoideae);
        agavoideaeJsonHandler.parseJsonMetadata(agaveJsonNode);
        MetadataItem metadataItem = agavoideaeJsonHandler.getMetadataItem();

        //add Metadata Item and permissions
        MetadataSearch search = new MetadataSearch(this.username);
        search.setMetadataItem(metadataItem);
        search.setOwner(this.username);
        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, this.username);

//        MetadataPermission readUserPermission = new MetadataPermission("READ_USER", PermissionType.READ);
//        MetadataPermission readWriteUserPermission = new MetadataPermission("READWRITE_USER", PermissionType.READ_WRITE);
//        MetadataPermission invalidUserPermission = new MetadataPermission("UNKNOWN_USER", PermissionType.UNKNOWN);
        pemManager.setPermission("READ_USER", PermissionType.READ.name());
        pemManager.setPermission("READWRITE_USER", PermissionType.READ_WRITE.name());

        MetadataItem addedItem = search.findOne();

        //search
        if (pemManager.canRead("READ_USER")) {
            MetadataSearch getSearch = new MetadataSearch("READ_USER");
            getSearch.setUuid(metadataItem.getUuid());

            MetadataItem foundItem = getSearch.findOne();
            Assert.assertEquals(foundItem, addedItem);
        } else {
            fail("Shared user with READ permission should have read access to the Metadata item");
        }

        if (pemManager.canRead("READWRITE_USER")) {
            MetadataSearch getSearch = new MetadataSearch("READWRITE_USER");
            getSearch.setUuid(addedMetadataItem.getUuid());

            MetadataItem foundItem = getSearch.findOne();
            Assert.assertEquals(foundItem, addedItem);
        } else {
            fail("Shared user with READ WRITE permission should have read access to the Metadata item");
        }

        if (pemManager.canRead(addedMetadataItem.getUuid())) {
            fail("User with no permissions should not have read access to the Metadata item");
        }
    }

    @Test
    public void findRegexSearchTest() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(username);
        search.setAccessibleOwnersImplicit();

//        search.clearCollection();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
        }

        String queryByValueRegex = "{ \"value.description\": { \"$regex\": \".*monocots.*\", \"$options\": \"m\"}}";
        JsonHandler jsonHandler = new JsonHandler();
        Document docValueRegexQuery = jsonHandler.parseUserQueryToDocument(queryByValueRegex);
        List<MetadataItem> resultList;
        resultList = search.find(docValueRegexQuery);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: cactus");
    }

    @Test
    public void findNameSearchTest() throws MetadataValidationException, MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(username);
//        search.clearCollection();
        search.setAccessibleOwnersImplicit();


        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
        }

        String queryByName = "{\"name\":\"mustard plant\"}";
        List<MetadataItem> resultList;

        JsonHandler jsonHandler = new JsonHandler();
        Document docQueryByName = jsonHandler.parseUserQueryToDocument(queryByName);

        resultList = search.find(docQueryByName);

        Assert.assertEquals(resultList.size(), 1, "There should be 1 metadata item found: mustard plant");
    }

    @Test
    public void findNestedValueSearchTest() throws MetadataValidationException, MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(username);
//        search.clearCollection();
        search.setAccessibleOwnersImplicit();


        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
        }

        String queryByValue = "{\"value.type\":\"a plant\"}";
        List<MetadataItem> resultList;

        JsonHandler jsonHandler = new JsonHandler();
        Document docQueryByValue = jsonHandler.parseUserQueryToDocument(queryByValue);

        resultList = search.find(docQueryByValue);

        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: mustard plant and cactus");
    }

    @Test
    public void findConditionalSearchTest() throws MetadataValidationException, MetadataQueryException, MetadataException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(username);
//        search.clearCollection();
        search.setAccessibleOwnersImplicit();

        if (this.queryList.isEmpty())
            setQueryList(new ArrayList<String>());

        for (String query : this.queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.setAccessibleOwnersImplicit();
            addSearch.setMetadataItem(toAdd);
            addSearch.insertCurrentMetadataItem();
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
        JsonHandler jsonHandler = new JsonHandler();
        Document docQueryByValueConditional = jsonHandler.parseUserQueryToDocument(queryByValueConditional);

        resultList = search.find(docQueryByValueConditional);

        Assert.assertEquals(resultList.size(), 2, "There should be 2 metadata items found: cactus and Agavoideae");
    }

    @Test
    public void deleteMetadataItemTest() throws MetadataValidationException, MetadataQueryException, PermissionException, MetadataException, UUIDException, MetadataAssociationException, MetadataStoreException {
        //create metadata item to delete
//        String metadataQueryAloe =
//                "  {" +
//                        "    \"name\": \"Aloe\"," +
//                        "    \"value\": {" +
//                        "      \"type\": \"a plant\"" +
//                        "}" +
//                        "   }";

        MetadataItem toAdd = createMetadataItem();
        MetadataItem addedItem = MetadataDao.getInstance().insert(toAdd);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedItem, addedItem.getOwner());
        if (pemManager.canWrite(this.username)) {
            MetadataSearch deleteSearch = new MetadataSearch(this.username);
            deleteSearch.setAccessibleOwnersImplicit();
            deleteSearch.setUuid(addedItem.getUuid());

            MetadataItem deletedItem = deleteSearch.deleteCurrentMetadataItem();

            Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");

            MetadataItem itemAfterDelete = deleteSearch.findOne();
            Assert.assertNull(itemAfterDelete, "Item should not be found after deleting.");

        } else {
            fail("Owner should have permission to delete item.");
        }
    }

    @Test
    public void StringToMetadataItemTest() throws IOException, UUIDException, PermissionException, MetadataException,  MetadataValidationException, MetadataQueryException, MetadataAssociationException {
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
            fail("Serializing MetadataItem to Json String should not throw exception");
        }
    }


    //Metadata Collection
//    @Test
//    public void createMetadataItemWithSchema() throws MetadataException, MetadataValidationException,  MetadataQueryException, MetadataAssociationException, UUIDException, PermissionException, UnknownHostException, MetadataStoreException {
//        //parse json
//
//        String strItemToAdd = "  {" +
//                "    \"name\": \"Sample Name\"," +
//                "    \"value\": {" +
//                "      \"type\": \"Sample type\"," +
//                "        \"profile\": {" +
//                "        \"status\": \"Sample Status\"" +
//                "           }," +
//                "        \"description\": \"Sample description...\"" +
//                "       }," +
//                "   }";
//
//        //AssociatedMetadataItem
//        String metadataAgave = "{" +
//                "    \"name\": \"Agave\"," +
//                "    \"value\": {" +
//                "      \"type\": \"a desert/dry-zone type plant\"" +
//                "       }" +
//                "   }";
//
////
////        //Schema Setup
////        String strAgavoideaeSchema = "{" +
////                "\"order\": \"sample order\"," +
////                "\"type\": \"object\"," +
////                "\"properties\" : {" +
////                "\"profile\" : {\"type\": \"string\"}," +
////                "\"status\": {\"enum\" : [\"active\", \"retired\", \"disabled\", \"banned\"]} " +
////                "}, " +
////                "\"description\": {\"type\" : \"string\"}" +
////                "   }";
////
////
////        String timestamp = new DateTime().toString();
////        Document doc = new Document("internalUsername", this.username)
////                .append("lastUpdated", timestamp)
////                .append("schema", JSON.parse(ServiceUtils.escapeSchemaRefFieldNames(strAgavoideaeSchema)));
////
////        MetadataSchemaPermissionManager pm = null;
////
////        String uuid = new AgaveUUID(UUIDType.SCHEMA).toString();
////        doc.put("uuid", uuid);
////        doc.append("created", timestamp);
////        doc.append("owner", username);
////        doc.append("tenantId", TenancyHelper.getCurrentTenantId());
////        MetadataSchemaDao.getInstance().getDefaultCollection().insertOne(doc);
////
////        pm = new MetadataSchemaPermissionManager(uuid, username);
////        pm.setPermission(username, "ALL");
//
//        String metadataQueryAgavoideae =
//                "  {" +
//                        "    \"name\": \"Agavoideae\"," +
//                        "    \"value\": {" +
//                        "      \"order\": \" Asparagales\", " +
//                        "      \"properties\": {" +
//                        "        \"profile\": {" +
//                        "        \"status\": \"active\"" +
//                        "           }," +
//                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
//                        "       }" +
//                        "       }," +
//                        "       \"schemaId\": \"" + schemaUuid + "\"" +
//                        "   }";
//
//        //validate metadata item from json handler
//        JsonHandler agavoideaeJsonHandler = new JsonHandler();
//        JsonNode agavoideaeJsonNode = agavoideaeJsonHandler.parseStringToJson(metadataQueryAgavoideae);
//        agavoideaeJsonHandler.parseJsonMetadata(agavoideaeJsonNode);
//        MetadataItem agavoideaeMetadataItem = agavoideaeJsonHandler.getMetadataItem();
//
//        //add metadata item to collection
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.username, agavoideaeMetadataItem.getUuid());
//
//        if (pemManager.canWrite()) {
//            MetadataSearch search = new MetadataSearch(this.username);
//            search.setMetadataItem(agavoideaeMetadataItem);
//            search.setOwner(this.username);
//            MetadataItem addedMetadataItem = search.insertMetadataItem();
//
//            //return metadata item added
//            Assert.assertNotNull(addedMetadataItem);
//        } else {
//            Assert.fail("Permission Manager should allow write for metadata item that doesn't exist.");
//        }
//    }

    @Test
    public void findMetadataItemAsOwner() throws MetadataValidationException, MetadataQueryException, MetadataException,  MetadataStoreException {
        //parse json
        String metadataQueryAgavoideae =
                "  {" +
                        "    \"name\": \"Agavoideae\"," +
                        "    \"value\": {" +
                        "      \"order\": \" Asparagales\", " +
                        "      \"properties\": {" +
                        "        \"profile\": {" +
                        "        \"status\": \"active\"" +
                        "           }," +
                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
                        "       }" +
                        "       }," +
                        "       \"schemaId\": \"" + uuid + "\"" +
                        "   }";

        //validate metadata item from json handler
        JsonHandler agavoideaeJsonHandler = new JsonHandler();
        JsonNode agavoideaeJsonNode = agavoideaeJsonHandler.parseStringToJson(metadataQueryAgavoideae);
        agavoideaeJsonHandler.parseJsonMetadata(agavoideaeJsonNode);
        MetadataItem agavoideaeMetadataItem = agavoideaeJsonHandler.getMetadataItem();

        MetadataSearch search = new MetadataSearch(this.username);
        search.setMetadataItem(agavoideaeMetadataItem);
        search.setOwner(this.username);
        search.insertCurrentMetadataItem();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(agavoideaeMetadataItem, this.username);
        if (pemManager.canRead(username)) {
            MetadataSearch getSearch = new MetadataSearch(this.username);
            getSearch.setUuid(agavoideaeMetadataItem.getUuid());

            MetadataItem foundItem = getSearch.findOne();
            Assert.assertEquals(foundItem, agavoideaeMetadataItem);
        } else {
            fail("Owner should have read access to the Metadata item");
        }
    }


    //user is not the owner but the admin - it should still be returned
    @Test
    public void getAllMetadataItemsImplicit() {
        //insert multiple metadata items
//        List<MetadataItem> addedItemsList = new ArrayList<>();
//
//
//        //get all metadata items where user is the owner or admin, or has read access
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersImplicit();

//        List<MetadataItem> resultList = search.findAll();

//        Assert.assertEquals(resultList, addedItemsList);

    }

    //user is not the owner but the admin - it should only be returned if
    //user is explicitly given permission
    @Test
    public void getAllMetadataItemsExplicit() {
        //insert multiple metadata items
//        List<MetadataItem> addedItemsList = new ArrayList<>();
//
//        //get all metadata items where user is the owner or has read access
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setAccessibleOwnersExplicit();

//        List<MetadataItem> resultList = search.findAll();
//
//        Assert.assertEquals(resultList, addedItemsList);
    }

//    @Test
//    public void findMetadataItemByUuid() {
//        //get metadata by uuid
//        MetadataItem metadataItemToAdd = new MetadataItem();
//
//        //use permission manager to verify that user is owner, admin, or has read access
//
//        MetadataItemPermissionManager permissionManager = new MetadataItemPermissionManager(this.username, metadataItemToAdd.getUuid());
//
//        if (permissionManager.canRead()) {
//            MetadataSearch search = new MetadataSearch(this.username);
//            search.setAccessibleOwnersImplicit();
//            search.setUuid(metadataItemToAdd.getUuid());
//
//            MetadataItem foundItem = search.findOne();
//            Assert.assertEquals(foundItem, metadataItemToAdd);
//        } else {
//            Assert.fail("User with READ permission should allow find Metadata Item.");
//        }
//
//
//    }

    @Test
    public void updateUsingDocument() throws MetadataException, PermissionException, MetadataValidationException, MetadataQueryException, MetadataStoreException, UUIDException, MetadataAssociationException {
        MetadataItem toAdd = createMetadataItem();
        MetadataSearch search = new MetadataSearch( this.username);
        search.setMetadataItem(toAdd);
        MetadataItem addedItem = search.insertCurrentMetadataItem();

        ObjectNode node = mapper.createObjectNode()
                .put("name", "Agavoideae");
        node.putObject("value")
                    .put("order", "Inactive - Asparagales")
                    .putObject("properties")
                    .putObject("profile")
                    .put("status", "inactive")
                    .put("description", "Inactive - Includes desert and dry-zone types such as the agaves and yuucas.");

        JsonHandler updateJsonHandler = new JsonHandler();

        Document toUpdateDoc = updateJsonHandler.parseJsonMetadataToDocument(node);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedItem, this.username);
        if (pemManager.canWrite(username)) {
            //using doc
            MetadataSearch updateWithDoc = new MetadataSearch(this.username);
            updateWithDoc.setMetadataItem(addedItem);
            Document updatedDoc = updateWithDoc.updateMetadataItem(toUpdateDoc);

            Assert.assertNotNull(updatedDoc, "MetadataItem should be not be null after updating.");

            MetadataSearch afterUpdate = new MetadataSearch(this.username);
            afterUpdate.setUuid(addedItem.getUuid());
            afterUpdate.setAccessibleOwnersImplicit();
            MetadataItem updatedItem = afterUpdate.findOne();
            Assert.assertNotNull(updatedDoc, "MetadataItem should be not be null after updating.");

            Assert.assertEquals(updatedItem.getName(), node.get("name").asText(),
                    "MetadataItem name match updated json node name");
            Assert.assertEquals(updatedItem.getValue(), node.get("value"),
                    "MetadataItem value should match updated json node value.");
            Assert.assertEquals(updatedItem.getSchemaId(), addedItem.getSchemaId(),
                    "MetadataItem schemaId should not be changed since it was not included in the update.");
            Assert.assertEquals(updatedItem.getOwner(), addedItem.getOwner(),
                    "MetadataItem owner should not be changed.");
            Assert.assertEquals(updatedItem.getUuid(), addedItem.getUuid(),
                    "MetadataItem uuid should not be changed.");
            Assert.assertEquals(updatedItem.getTenantId(), addedItem.getTenantId(), "" +
                    "MetadataItem tenantId should not be changed.");
            Assert.assertEquals(updatedItem.getPermissions(), addedItem.getPermissions(),
                    "MetadataItem permissions should not be changed since it was not included in the update.");
            Assert.assertEquals(addedItem.getAssociations().getAssociatedIds().keySet(), updatedItem.getAssociations().getAssociatedIds().keySet(),
                    "MetadataItem associatedIds should not be changed since it was not included in the update.");
            Assert.assertEquals(updatedItem.getNotifications(), addedItem.getNotifications(),
                    "MetadataItem notifications should not be changed since it was not included in the update.");
            Assert.assertTrue((updatedItem.getCreated().compareTo(addedItem.getCreated()) == 0),
                    "MetadataItem created date should not be changed.");
            Assert.assertTrue((updatedItem.getLastUpdated().compareTo(addedItem.getLastUpdated()) >= 1),
                    "MetadataItem lastUpdated should be updated to after the initial item's lastUpdated date.");

        } else {
            fail("User with WRITE permission should allow updating Metadata Item.");
        }
    }


//    @Test
//    public void updateUsingMetadataItemTest() throws MetadataException, MetadataStoreException, PermissionException {
//        //add metadata item with permissions
//        MetadataItem addedItem = new MetadataItem();
//        addedItem.setName("Sample Name");
//        ObjectMapper mapper = new ObjectMapper();
//        addedItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        addedItem.setOwner(this.username);
//        addedItem.setPermissions(Arrays.asList(new MetadataPermission("NEW_USER", PermissionType.READ)));
//
//        MetadataSearch search = new MetadataSearch(this.username);
//        search.setMetadataItem(addedItem);
//        search.setOwner(this.username);
//        search.setAccessibleOwnersImplicit();
//        search.insertMetadataItem();
//
//        MetadataItem afterAdd = search.findOne();
//        Assert.assertNotNull(afterAdd);
//
//        //create query to update item
//        MetadataItem updateItem = new MetadataItem();
//        updateItem.setUuid(addedItem.getUuid());
//        updateItem.setName("Updated Name");
//        updateItem.setValue(mapper.createObjectNode().put("updatedKey", "updatedValue"));
//
//        MetadataSearch updateSearch = new MetadataSearch(this.username);
//        updateSearch.setMetadataItem(updateItem);
//        updateSearch.setAccessibleOwnersImplicit();
//
//        MetadataItem updatedItem = updateSearch.updateMetadataItem();
//
//        MetadataSearch afterUpdateSearch = new MetadataSearch(this.username);
//        afterUpdateSearch.setUuid(updatedItem.getUuid());
//        afterUpdateSearch.setAccessibleOwnersImplicit();
//        MetadataItem afterUpdate = afterUpdateSearch.findOne();
//
//        System.out.println("Before updating afterAdd");
//        System.out.println("Get Created: " + afterUpdate.getCreated() + " == " + afterAdd.getCreated());
//        System.out.println("Get lastUpdated: " + afterUpdate.getLastUpdated() + " == " + afterAdd.getLastUpdated());
//
//
//        afterAdd.setName("Updated Name");
//        afterAdd.setValue(mapper.createObjectNode().put("updatedKey", "updatedValue"));
//        afterAdd.setLastUpdated(afterUpdate.getLastUpdated());
//
//        System.out.println("After updating afterAdd");
//
//        System.out.println(afterUpdate.getName() + " == " + afterAdd.getName());
//        System.out.println(afterUpdate.getValue() + " == " + afterAdd.getValue());
//
//        System.out.println("Get Created: " + afterUpdate.getCreated() + " == " + afterAdd.getCreated());
//        System.out.println("Get lastUpdated: " + afterUpdate.getLastUpdated() + " == " + afterAdd.getLastUpdated());
//
//        System.out.println(StringUtils.equals(afterUpdate.getName(), afterAdd.getName()));
//        System.out.println(afterUpdate.getValue().equals(afterAdd.getValue()));
//        System.out.println(StringUtils.equals(afterUpdate.getSchemaId(), afterAdd.getSchemaId()));
//        System.out.println(StringUtils.equals(afterUpdate.getOwner(), afterAdd.getOwner()));
//        System.out.println(StringUtils.equals(afterUpdate.getUuid(), afterAdd.getUuid()));
//        System.out.println(StringUtils.equals(afterUpdate.getTenantId(), afterAdd.getTenantId()));
//        System.out.println(afterUpdate.getLastUpdated().compareTo(afterAdd.getLastUpdated()) == 0);
//        System.out.println(afterUpdate.getAssociations().getAssociatedIds().keySet().equals(afterAdd.getAssociations().getAssociatedIds().keySet()));
//
//        Assert.assertNotNull(afterUpdate);
//        Assert.assertEquals(afterUpdate, afterAdd);
//    }


//    @Test
//    public void deleteMetadataItemByUuid() throws PermissionException {
//        //get metadata by uuid
//        MetadataItem itemToDelete = new MetadataItem();
//
//        //use permission manager to verify that user is owner, admin, or has write access
//        MetadataItemPermissionManager permissionManager = new MetadataItemPermissionManager(this.username, itemToDelete.getUuid());
//
//        if (permissionManager.canWrite()) {
//            MetadataSearch search = new MetadataSearch(this.username);
//            search.setAccessibleOwnersImplicit();
//            search.setUuid(itemToDelete.getUuid());
//            MetadataItem deletedItem = search.deleteMetadataItem();
//            Assert.assertEquals(deletedItem, itemToDelete);
//        } else {
//            Assert.fail("User with WRITE permission should allow deleting Metadata Item.");
//        }
//
//    }


//    public MetadataItem createMetadataItem() throws IOException, MetadataException {
//        String name = "Agavoideae";
//        String value =
//                "  {" +
//                        "    \"value\": {" +
//                        "      \"order\": \" Asparagales\", " +
//                        "      \"properties\": {" +
//                        "        \"profile\": {" +
//                        "        \"status\": \"active\"" +
//                        "           }," +
//                        "        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
//                        "       }" +
//                        "       }" +
//                        "   }";
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode node = mapper.getFactory().createParser(value).readValueAsTree();
//
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(name);
//        metadataItem.setValue(node);
//        metadataItem.setOwner(this.username);
//        metadataItem.setInternalUsername(this.username);
//        metadataItem.setPermissions(Arrays.asList(new MetadataPermission("SHARED_USER", PermissionType.READ)));
//
//        return metadataItem;
//    }

}

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
import org.iplantc.service.metadata.util.ServiceUtils;
import org.joda.time.DateTime;
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
import java.util.Set;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.*;

@Test(groups = {"integration"})
public class MetadataSearchIT {
    String uuid;
    String username = "TEST_USER";
    String readUser = "READ_USER";
    String readWriteUser = "READWRITE_USER";
    String noneUser = "NONE_USER";

    AgaveUUID jobUuid = new AgaveUUID(UUIDType.JOB);
    AgaveUUID schemaUuid = new AgaveUUID(UUIDType.SCHEMA);
    ObjectMapper mapper = new ObjectMapper();


    public List<String> setQueryList() {
        List<String> queryList = new ArrayList<>();
        queryList.add("  {" +
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

        queryList.add("  {" +
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

        queryList.add(
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
        queryList.add("  {" +
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
        return queryList;
    }

    @BeforeClass
    public void setup() {
        createSchema();
    }

    @AfterMethod
    public void cleanUpCollection() {
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
        metadataItem.setPermissions(Arrays.asList(new MetadataPermission(this.readUser, PermissionType.READ)));

        return metadataItem;
    }

    public MetadataItem createMetadataItemFromString(String strMetadataItem, String owner) throws MetadataValidationException, MetadataQueryException, UUIDException, MetadataException, PermissionException, MetadataAssociationException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        MetadataValidation mockValidation = mock(MetadataValidation.class);

        MetadataAssociationList associationList = new MetadataAssociationList();
        associationList.add(jobUuid.toString());

        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIdsUuidApi(mapper.createArrayNode().add(jobUuid.toString()));
        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.setMetadataValidation(mockValidation);


        JsonNode node = mapper.getFactory().createParser(strMetadataItem).readValueAsTree();
        MetadataItem metadataItem = jsonHandler.parseJsonMetadata(node);

        metadataItem.setOwner(owner);
        metadataItem.setInternalUsername(owner);
        metadataItem.setPermissions(Arrays.asList(new MetadataPermission(this.readUser, PermissionType.READ)));

        return metadataItem;
    }

    @DataProvider(name = "initMetadataItemDataProvider")
    public Object[][] initMetadataItemDataProvider() throws MetadataAssociationException, PermissionException, MetadataException {
        return new Object[][]{
                {createMetadataItem(), false, "Valid metadata item should be returned and not throw Exceptions."},
                {new MetadataItem(), true, "Missing fields should throw Exception when interacting with database."},
                {null, true, "Null MetadataItem should throw MetadataException when interacting with database."}
        };
    }

    @Test(dataProvider = "initMetadataItemDataProvider")
    public void insertMetadataItemTest(MetadataItem metadataItem, boolean bolThrowException, String message) {
        MetadataSearch search = new MetadataSearch(this.username);
        search.setAccessibleOwnersExplicit();

        try {
            MetadataItem addedItem = search.insertMetadataItem(metadataItem);
            if (bolThrowException)
                fail("Exception should be thrown: " + message);

            Assert.assertEquals(addedItem, metadataItem, "Item to be added should be returned on successful insert.");
            MetadataItem foundItem = search.findById(metadataItem.getUuid(), metadataItem.getTenantId());

            assertEquals(foundItem, metadataItem, "MetadataItem found should be the same as the added MetadataItem.");

        } catch (Exception e) {
            if (!bolThrowException)
                fail(message);

            if (e.getClass().equals(MetadataException.class))
                assertEquals(e.getMessage(), "Cannot insert a null MetadataItem", message);
            else
                assertEquals(e.getMessage(), "value can not be null", message);

        }

    }

    @DataProvider(name = "initUpdateMetadataItemDataProvider")
    public Object[][] initUpdateMetadataItemDataProvider() {
        Document updateDoc = new Document("name", "New Name")
                .append("value", new Document("newKey", "newValue"))
                .append("schemaId", new AgaveUUID(UUIDType.SCHEMA).toString())
                .append("associationIds",
                        List.of(new AgaveUUID(UUIDType.METADATA).toString()))
                .append("permissions",
                        List.of(new Document("username", "NEW_USER")
                                .append("permission", PermissionType.READ.toString())
                                .append("group", null)));

        Document updateNameAndValueOnlyDoc = new Document("name", "New Name")
                .append("value", new Document("newKey", "newValue"));


        return new Object[][]{
                {updateDoc},
                {updateNameAndValueOnlyDoc},
                {new Document()}

        };
    }

    @Test(dataProvider = "initUpdateMetadataItemDataProvider")
    public void updateMetadataItem(Document doc) throws MetadataAssociationException, MetadataException, PermissionException, MetadataStoreException {
        MetadataItem toAdd = createMetadataItem();
        MetadataSearch createItem = new MetadataSearch(this.username);
        MetadataItem addedItem = createItem.insertMetadataItem(toAdd);

        MetadataSearch updateItem = new MetadataSearch(this.username);

        Document updatedDoc = updateItem.updateMetadataItem(doc, addedItem.getUuid());

        assertNotNull(updatedDoc, "Updated document should be returned.");
        assertEquals(updatedDoc.get("uuid"), addedItem.getUuid(), "MetadataItem uuid should not be updated.");
        assertEquals(updatedDoc.get("owner"), addedItem.getOwner(), "Owner should not be updated.");
        assertEquals(updatedDoc.get("tenantId"), addedItem.getTenantId(), "tenantId should not be updated.");

        if (doc.containsKey("name"))
            assertEquals(updatedDoc.getString("name"), doc.getString("name"), "Name should match the name field in the document used for updating.");
        else
            assertEquals(updatedDoc.getString("name"), addedItem.getName(), "Fields not specified in the document should not be updated.");

        if (doc.containsKey("value"))
            for (String key : doc.keySet()) {
                assertEquals(updatedDoc.getEmbedded(List.of("value", key), String.class), doc.getEmbedded(List.of("value", key), String.class), "Value should match the value field in the document used for updating.");
            }
        else
            for (String key : updatedDoc.keySet()) {
                assertEquals(updatedDoc.getEmbedded(List.of("value", key), String.class), addedItem.getValue().get(key), "Fields not specified in the document should not be updated.");
            }

        if (doc.containsKey("schemaId"))
            assertEquals(updatedDoc.getString("schemaId"), doc.getString("schemaId"), "SchemaId should match the schemaId field in the document used for updating.");
        else
            assertEquals(updatedDoc.getString("schemaId"), addedItem.getSchemaId(), "Fields not specified in the document should not be updated.");

        if (doc.containsKey("associatedIds"))
            assertEquals(updatedDoc.get("associationIds"), doc.get("associationIds"), "AssociationIds should match the associationIds field in the document used for updating.");
        else {
            List<String> updatedAssociatedIds = updatedDoc.getList("associationIds", String.class);
            Set<String> addedAssociatedIds = addedItem.getAssociations().getAssociatedIds().keySet();
            assertEquals(addedAssociatedIds.size(), updatedAssociatedIds.size(), "Associationids should match if field was not specified in the document used for updating.");
            for (String id : addedAssociatedIds)
                assertTrue(addedItem.getAssociations().getAssociatedIds().containsKey(id), "Fields not specified in the document should not be updated.");
        }

        if (doc.containsKey("permissions"))
            assertEquals(updatedDoc.get("permissions"), doc.get("permissions"), "Permissions should match the permissions field in the document used for updating.");
        else {
            List<Document> permissionsDoc = updatedDoc.getList("permissions", Document.class);
            for (Document permission : permissionsDoc) {
                String username = permission.getString("username");
                assertEquals(permission.get("permission"), addedItem.getPermissionForUsername(username).getPermission().toString(), "Fields not specified in the document should not be updated.");
            }
        }
    }

    @Test
    public void findMetadataWithFiltersTest() throws MetadataException,  MetadataQueryException, MetadataStoreException {
        ObjectMapper mapper = new ObjectMapper();

        MetadataItem testEntity = new MetadataItem();
        testEntity.setName("wisteria");
        ObjectNode value = mapper.createObjectNode();
        value.put("type", "a flowering plant")
                .put("order", "Fabales")
                .put("description", "native to China, Korea, Japan, and the Eastern United States.")
                .putObject("profile")
                .put("status", "active");

        testEntity.setValue(value);
        testEntity.setOwner(username);
        testEntity.setInternalUsername(username);

        MetadataSearch toAdd = new MetadataSearch(username);
        MetadataItem addedItem = toAdd.insertMetadataItem(testEntity);

        String strQuery = "{ \"value.type\": { \"$regex\": \".*flowering.*\"}}";
        String[] filters = {"name", "value.type", "lastUpdated"};

        MetadataSearch search = new MetadataSearch(username);
        search.setAccessibleOwnersImplicit();

        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument(strQuery);
        List<Document> foundItems = search.filterFind(docQuery, filters);
        Assert.assertEquals(foundItems.size(), 1);
        Assert.assertTrue(Arrays.asList("wisteria", "Agavoideae").contains(foundItems.get(0).get("name")));
        Assert.assertTrue(foundItems.get(0).getEmbedded(List.of("value", "type"), String.class).contains("flowering"));

    }

    @Test
    public void findAllMetadataForUserImplicitSearchTest() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException, IOException {
        List<String> queryList = setQueryList();

        for (String query : queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, Settings.PUBLIC_USER_USERNAME);
            MetadataSearch addSearch = new MetadataSearch(Settings.PUBLIC_USER_USERNAME);
            addSearch.insertMetadataItem(toAdd);
        }
        List<MetadataItem> searchResult;
        MetadataSearch implicitSearch = new MetadataSearch(this.username);
        implicitSearch.setAccessibleOwnersImplicit();

        searchResult = implicitSearch.find(new Document());
        Assert.assertEquals(searchResult.size(), 0, "Implicit Search should find 0 metadata items ");
    }

    @Test
    public void findAllMetadataForUserExplicitSearchTest() throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException, IOException {
        List<String> queryList = setQueryList();

        for (String query : queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, Settings.PUBLIC_USER_USERNAME);
            MetadataSearch addSearch = new MetadataSearch(Settings.PUBLIC_USER_USERNAME);
            addSearch.insertMetadataItem(toAdd);
        }

        MetadataSearch explicitSearch = new MetadataSearch(this.username);
        explicitSearch.setAccessibleOwnersExplicit();

        List<MetadataItem> searchResult;

        searchResult = explicitSearch.find(new Document());
        Assert.assertEquals(searchResult.size(), 4, "Explicit Search should find 4 metadata items ");

        for (MetadataItem foundItem : searchResult) {
            MetadataPermissionManager pemManager = new MetadataPermissionManager(foundItem, this.username);
            if (!pemManager.canRead(username)) {
                fail("Users should be able to read metadata items.");
            }
        }
    }

    @DataProvider(name = "initUsersForSearchDataProvider")
    public Object[][] initUsersForSearchDataProvider() {
        return new Object[][]{
                {this.username, true, "Owner should have permissions to READ MetadataItem."},
                {this.readUser, true, "User with READ permission should have permissions to READ MetadataItem."},
                {this.readWriteUser, true, "User with READWRITE permission should have permissions to READ MetadataItem."},
                {this.noneUser, false, "User with no permissions should not have permissions to READ MetadataItem."},

        };
    }

    @Test(dataProvider = "initUsersForSearchDataProvider")
    public void findByIdTest(String user, boolean bolCanRead, String message) throws MetadataException, PermissionException, MetadataStoreException, MetadataAssociationException {
        MetadataItem metadataItem = createMetadataItem();

        //add Metadata Item and permissions
        MetadataSearch search = new MetadataSearch(this.username);
        metadataItem.setOwner(this.username);
        MetadataItem addedMetadataItem = search.insertMetadataItem(metadataItem);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, this.username);
        pemManager.setPermission(this.readUser, PermissionType.READ.name());
        pemManager.setPermission(this.readWriteUser, PermissionType.READ_WRITE.name());

        MetadataItem addedItem = search.findById(metadataItem.getUuid(), metadataItem.getTenantId());

        //search
        if (pemManager.canRead(user)) {
            if (!bolCanRead)
                fail(message);
            MetadataSearch getSearch = new MetadataSearch(user);
            MetadataItem foundItem = getSearch.findById(metadataItem.getUuid(), metadataItem.getTenantId());
            Assert.assertEquals(foundItem, addedItem);
        } else {
            if (bolCanRead)
                fail(message);
        }
    }

    @DataProvider(name = "initRegexSearchDataProvider")
    public Object[][] initRegexSearchDataProvider() {
        return new Object[][]{
                {"{ \"value.description\": { \"$regex\": \".*monocots.*\", \"$options\": \"m\"}}", 1, "There should be 1 matching metadata item found: cactus"},
                {"{\"name\":\"mustard plant\"}", 1, "There should be 1 metadata item found: mustard plant"},
                {"{\"value.type\":\"a plant\"}", 2, "There should be 2 metadata items found: mustard plant and cactus"},
                {"{" +
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
                        "}", 2, "There should be 2 metadata items found: cactus and Agavoideae"}
        };
    }

    @Test(dataProvider = "initRegexSearchDataProvider")
    public void findRegexSearchTest(String regexQuery, int foundSize, String message) throws MetadataException, MetadataValidationException, MetadataQueryException, PermissionException, UUIDException, MetadataAssociationException, MetadataStoreException, IOException {
        List<String> queryList = setQueryList();

        for (String query : queryList) {
            MetadataItem toAdd = createMetadataItemFromString(query, this.username);
            MetadataSearch addSearch = new MetadataSearch(this.username);
            addSearch.insertMetadataItem(toAdd);
        }

        JsonHandler jsonHandler = new JsonHandler();
        Document docValueRegexQuery = jsonHandler.parseUserQueryToDocument(regexQuery);

        MetadataSearch search = new MetadataSearch(username);
        search.setAccessibleOwnersImplicit();
        List<MetadataItem> resultList = search.find(docValueRegexQuery);

        Assert.assertEquals(resultList.size(), foundSize, message);

    }

    @Test
    public void deleteMetadataItemTest() throws PermissionException, MetadataException, MetadataAssociationException, MetadataStoreException {
        MetadataItem toAdd = createMetadataItem();
        MetadataItem addedItem = MetadataDao.getInstance().insert(toAdd);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedItem, addedItem.getOwner());
        if (pemManager.canWrite(this.username)) {
            MetadataSearch deleteSearch = new MetadataSearch(this.username);
            deleteSearch.setAccessibleOwnersImplicit();

            MetadataItem deletedItem = deleteSearch.deleteMetadataItem(addedItem.getUuid(), addedItem.getTenantId());
            Assert.assertNotNull(deletedItem, "Deleting metadata item should return the item removed.");

            MetadataItem itemAfterDelete = deleteSearch.findById(addedItem.getUuid(), addedItem.getTenantId());
            Assert.assertNull(itemAfterDelete, "Item should not be found after deleting.");

        } else {
            fail("Owner should have permission to delete item.");
        }
    }

}

package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.exceptions.MetadataValidationException;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.testng.Assert.*;

@Test(groups = {"integration"})
public class MetadataDataProcessingIT {
    String username = "TEST_USER";
    String readUser = "READ_USER";
    String readWriteUser = "READWRITE_USER";
    String noneUser = "NONE_USER";
    MetadataItem addedMetadataItem;
    MongoCollection collection;
    MongoCollection schemaCollection;
    ObjectMapper mapper = new ObjectMapper();

    public MetadataItem setupMetadataItem() throws MetadataException {
        MetadataItem toAdd = new MetadataItem();
        toAdd.setOwner(username);
        toAdd.setInternalUsername(username);
        toAdd.setName("mustard plant");
        toAdd.setValue(mapper.createObjectNode().put("testKey", "testValue"));

        MetadataPermission readPermission = new MetadataPermission(readUser, PermissionType.READ);
        MetadataPermission readWritePermission = new MetadataPermission(readWriteUser, PermissionType.READ_WRITE);
        toAdd.getPermissions().addAll(List.of(readPermission, readWritePermission));
        addedMetadataItem = toAdd;
        return toAdd;
    }

    @BeforeTest
    public void setUpDatabase() {
        ClassModel<JsonNode> valueModel = ClassModel.builder(JsonNode.class).build();
        ClassModel<MetadataPermission> metadataPermissionModel = ClassModel.builder(MetadataPermission.class).build();
        PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(valueModel, metadataPermissionModel).build();

        CodecRegistry registry = CodecRegistries.fromCodecs(new MetadataItemCodec());

        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(pojoCodecProvider),
                registry);

        MongoClient mongo4Client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(
                        new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT))))
                .credential(MongoCredential.createScramSha1Credential(
                        Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray()))
                .codecRegistry(pojoCodecRegistry)
                .build());

        MongoDatabase db = mongo4Client.getDatabase(Settings.METADATA_DB_SCHEME);
        collection = db.getCollection(Settings.METADATA_DB_COLLECTION, MetadataItem.class);
        schemaCollection = db.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION, MetadataItem.class);
    }

    @BeforeMethod
    public void setupMetadataCollection() throws MetadataException {
        collection.insertOne(setupMetadataItem());
    }

    @AfterMethod
    public void cleanUpCollection() {
        collection.deleteMany(new Document());
    }

    public String setupSchema() {
        String schemaUuid = new AgaveUUID(UUIDType.SCHEMA).toString();

        String strItemToAdd = "{" +
                "\"order\" : \"sample order\"," +
                "\"type\" : \"object\", " +
                "\"properties\" : {" +
                "\"profile\" : { \"type\" : \"string\" }, " +
                "\"description\" : { \"type\" : \"string\" }, " +
                "\"status\" : {\"enum\" : [\"active\", \"retired\", \"disabled\"]}" +
                "}" +
                "}";

        Document doc;
        String timestamp = new DateTime().toString();
        doc = new Document("internalUsername", this.username)
                .append("lastUpdated", timestamp)
                .append("schema", ServiceUtils.escapeSchemaRefFieldNames(strItemToAdd))
                .append("uuid", schemaUuid)
                .append("created", timestamp)
                .append("owner", this.username)
                .append("tenantId", TenancyHelper.getCurrentTenantId());

        schemaCollection.insertOne(doc);

        return schemaUuid;
    }

    @DataProvider(name = "initMetadataItemDataProvider")
    public Object[][] initMetadataItemDataProvider() {
        return new Object[][]{
                {this.username, "{\"name\":\"mustard plant\"}", true, "Owner should have READ permission for the MetadataItem."},
                {this.readUser, "{\"name\":\"mustard plant\"}", true, "User with READ permission should find the MetadataItem."},
                {this.readWriteUser, "{\"name\":\"mustard plant\"}", true, "User with READ_WRITE permission should find the MetadataItem."},
                {this.noneUser, "{\"name\":\"mustard plant\"}", false, "User without any permissions should not find the MetadataItem."}
        };
    }

    @DataProvider(name = "initUpdateMetadataItemDataProvider")
    public Object[][] initUpdateMetadataItemDataProvider() throws UnknownHostException {
        String schemaId = setupSchema();
        String nonexistentSchemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
        String invalidId = new AgaveUUID(UUIDType.METADATA).toString();


        return new Object[][]{
                {this.username, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\", " +
                        "\"status\":\"active\"}, \"schemaId\":\"" + schemaId + "\"}",
                        true, "Owner should have READ_WRITE permission for the MetadataItem.", false,},
                {this.username, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\", " +
                        "\"status\":\"invalid status\"}, \"schemaId\":\"" + schemaId + "\"}",
                        true, "Owner should have READ_WRITE permission for the MetadataItem.", true},
                {this.username, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\", " +
                        "\"status\":\"active\"}, \"schemaId\":\"" + nonexistentSchemaId + "\"}",
                        true, "Owner should have READ_WRITE permission for the MetadataItem.", true},
                {this.username, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\", " +
                        "\"status\":\"invalid status\"}, \"schemaId\":\"" + invalidId + "\"}",
                        true, "Owner should have READ_WRITE permission for the MetadataItem.", true},
                {this.readUser, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\"}}",
                        false, "User with READ permission should not be able to change the MetadataItem.", false},
                {this.readWriteUser, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\"}}",
                        true, "User with READ_WRITE permission should be able to change the MetadataItem.", false},
                {this.noneUser, "{\"name\":\"mustard plant\", \"value\": {\"description\": \"sample description\"}}",
                        false, "User without any permissions should not be able to change the MetadataItem.", false}
        };
    }

    @DataProvider(name = "initMultipleMetadataItemDataProvider")
    public Object[][] initMultipleMetadataItemDataProvider() {
        return new Object[][]{
                {this.username, 2},
                {this.readUser, 1},
                {this.readWriteUser, 1},
                {this.noneUser, 0}
        };
    }

    @DataProvider(name = "initJsonMetadataItemDataProvider")
    public Object[][] initJsonMetadataItemDataProvider() throws MetadataException {
        return new Object[][]{

                {"{\"name\": \"sample name\", \"value\": {\"title\": \"sample title\", \"description\": { \"species\" : \"sample species\" }}}",
                        false, "Valid Json request should not throw exception."},
                {"{\"value\": {\"title\": \"sample title\", \"description\": { \"species\" : \"sample\" \"species\" }}}",
                        true, "Json request missing the required name field should throw exception."},
                {"{\"name\": \"sample name\", \"schemaId\": null, \"associationIds\":[]}",
                        true, "Json request missing the required name field should throw exception."}
        };
    }

    @Test(dataProvider = "initJsonMetadataItemDataProvider")
    public void createMetadataItemTest(String strJson, boolean bolThrowException, String message) {
        JsonHandler handler = new JsonHandler();

        try {
            JsonNode jsonNode = mapper.getFactory().createParser(strJson).readValueAsTree();
            MetadataItem toAdd = handler.parseJsonMetadata(jsonNode);

            if (bolThrowException)
                fail(message);
            toAdd.setInternalUsername(this.username);
            toAdd.setOwner(this.username);

            MetadataSearch search = new MetadataSearch(this.username);
            search.insertMetadataItem(toAdd);

            MetadataItem foundItem = search.findById(toAdd.getUuid(), toAdd.getTenantId());
            assertEquals(toAdd, foundItem, "Found item should match the added item.");

        } catch (Exception e) {
            if (!bolThrowException)
                fail(message);

        }
    }


    //find metadata item - with read permission
    @Test(dataProvider = "initMetadataItemDataProvider")
    public void findSingleMetadataItemTest(String user, String jsonQuery, boolean bolCanRead, String message) throws MetadataException, MetadataStoreException, MetadataQueryException {
        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument(jsonQuery);
        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();
        List<MetadataItem> result = search.find(docQuery);


        for (MetadataItem metadataItem : result) {
            Assert.assertEquals(metadataItem, addedMetadataItem, "Found MetadataItem should match the added MetadataItem.");
            MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, metadataItem.getOwner());

            assertEquals(pemManager.canRead(user), bolCanRead, message);
        }
    }

    @Test(dataProvider = "initMetadataItemDataProvider")
    public void filterFindSingleMetadataItemTest(String user, String jsonQuery, boolean bolCanRead, String message) throws MetadataQueryException, MetadataException {
        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument(jsonQuery);
        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();
        String[] filters = new String[]{"uuid", "name", "schemaId"};

        Document result = search.filterFindById(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId(), filters);

        if (result == null) {
            if (bolCanRead)
                fail(message);
        } else {
            if (!bolCanRead)
                fail(message);
            else {
                assertEquals(result.size(), filters.length, "Size of document should match number of filters.");
                assertEquals(result.getString("uuid"), addedMetadataItem.getUuid(), "Found document should have matching uuid as added MetadataItem");
                assertEquals(result.getString("name"), addedMetadataItem.getName(), "Found document should have matching name as added MetadataItem");
                assertEquals(result.getString("schemaId"), addedMetadataItem.getSchemaId(), "Found document should have matching schemaId as added MetadataItem");
                assertNull(result.get("value"), "Field not specified in the filters should not be in the found document.");
            }
        }
    }

    @Test(dataProvider = "initMultipleMetadataItemDataProvider")
    public void findMetadataItemTest(String user, int foundSize) throws MetadataQueryException, MetadataStoreException, MetadataException {
        //create multiple MetadataItem
        List<MetadataItem> addedItems = new ArrayList<>();
        addedItems.add(addedMetadataItem);

        MetadataItem toAdd = setupMetadataItem();
        toAdd.removePermission(toAdd.getPermissionForUsername(this.readUser));
        toAdd.removePermission(toAdd.getPermissionForUsername(this.readWriteUser));
        MetadataDao.getInstance().insert(toAdd);
        addedItems.add(toAdd);


        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();

        JsonHandler jsonHandler = new JsonHandler();
        Document docUserQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");
        List<MetadataItem> foundItems = search.find(docUserQuery);

        assertEquals(foundItems.size(), foundSize, "Results found should only include items that user has READ permissions for.");

        if (foundSize > 0) {
            for (MetadataItem foundItem : foundItems) {

                boolean found = false;
                for (MetadataItem addedItem : addedItems) {
                    if (foundItem.getUuid().equals(addedItem.getUuid())) {
                        found = true;
                        assertEquals(foundItem, addedItem, "Found MetadataItem should match the added MetadataItem.");
                        break;
                    }
                }
                if (!found)
                    fail("All items found should be in the added Metadata Items.");
            }
        }
    }

    @Test(dataProvider = "initMultipleMetadataItemDataProvider")
    public void filterFindMetadataItemTest(String user, int foundSize) throws MetadataException, MetadataStoreException, MetadataQueryException {
        //create multiple MetadataItem
        List<MetadataItem> addedItems = new ArrayList<>();
        addedItems.add(addedMetadataItem);

        MetadataItem toAdd = setupMetadataItem();
        toAdd.removePermission(toAdd.getPermissionForUsername(this.readUser));
        toAdd.removePermission(toAdd.getPermissionForUsername(this.readWriteUser));
        MetadataDao.getInstance().insert(toAdd);
        addedItems.add(toAdd);


        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();

        JsonHandler jsonHandler = new JsonHandler();
        Document docUserQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");

        String[] filters = new String[]{"uuid", "name"};

        List<Document> foundItems = search.filterFind(docUserQuery, filters);

        assertEquals(foundItems.size(), foundSize, "Results found should only include items that user has READ permissions for.");

        if (foundSize > 0) {
            for (Document foundItem : foundItems) {
                boolean found = false;

                assertEquals(foundItem.size(), filters.length, "Result should only contain the fields specified in filters.");

                for (MetadataItem addedItem : addedItems) {
                    if (foundItem.getString("uuid").equals(addedItem.getUuid())) {
                        found = true;
                        assertEquals(foundItem.get("name"), addedItem.getName(), "Found Document name should match the added MetadataItem.");
                        break;
                    }
                }
                if (!found)
                    fail("All items found should be in the added Metadata Items.");
            }
        }
    }

    @Test(dataProvider = "initUpdateMetadataItemDataProvider")
    public void updateMetadataItemTest(String user, String jsonQuery, boolean bolCanWrite, String message, boolean bolThrowException) throws MetadataException, IOException, MetadataQueryException, MetadataValidationException {
        //parse fields to update
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.getFactory().createParser(jsonQuery).readValueAsTree();

        MetadataValidation partialValidation = new MetadataValidation();
        try {
            if (partialValidation.validateMetadataNodeFields(node, username) == null) {
                fail("Validation should not return null if node fields are valid.");
            }
            if (bolThrowException) {
                fail("Invalid field should throw exception");
            }

        } catch (Exception e) {
            if (!bolThrowException)
                fail("Valid node fields should not throw exception.");
        }

        JsonHandler jsonHandler = new JsonHandler();
        Document toUpdate = jsonHandler.parseJsonMetadataToDocument(node);


        //set up for updating
        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();

        //check for permission
        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem.getUuid(), addedMetadataItem.getOwner());

        if (pemManager.canWrite(user)) {
            assertTrue(bolCanWrite, message);

            //update
            Document updatedDoc = search.updateMetadataItem(toUpdate, addedMetadataItem.getUuid());

            for (String field : toUpdate.keySet()) {
                assertEquals(updatedDoc.get(field), toUpdate.get(field), "Updated document fields should match the document used for updating.");
            }

            MetadataItem updatedItem = search.findById(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());

            assertEquals(updatedItem.getOwner(), addedMetadataItem.getOwner(), "Owner should not be changed.");
            assertEquals(updatedItem.getTenantId(), addedMetadataItem.getTenantId(), "TenantId should not be changed.");
            assertEquals(updatedItem.getName(), toUpdate.getString("name"), "MetadataItem name should be match the document used for updating.");

            for (String key : toUpdate.keySet())
                assertEquals(updatedItem.getValue().get(key), toUpdate.getEmbedded(List.of("value", key), String.class), "MetadataItem value should be match the document used for updating.");

            if (updatedDoc.containsKey("schemaId"))
                assertEquals(updatedItem.getSchemaId(), updatedDoc.getString("schemaId"), "SchemaId field should match the document used for updating.");
            else
                assertEquals(updatedItem.getSchemaId(), addedMetadataItem.getSchemaId(), "SchemaId should not be changed because it was not included in the update query.");

            assertEquals(updatedItem.getPermissions(), addedMetadataItem.getPermissions(), "SchemaId should not be changed because it was not included in the update query.");

            assertEquals(updatedItem.getAssociations().getAssociatedIds(), addedMetadataItem.getAssociations().getAssociatedIds(), "AssociationIds should not be changed because it was not included in the update query.");
            assertEquals(updatedItem.getNotifications(), addedMetadataItem.getNotifications(), "Notifications should not be changed because it was not included in the update query.");
        } else {
            assertFalse(bolCanWrite, message);
        }

    }

    @Test(dataProvider = "initUpdateMetadataItemDataProvider")
    public void deleteMetadataItemTest(String user, String jsonQuery, boolean bolCanWrite, String message, boolean bolThrowException) throws MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(user);
        MetadataItem metadataItem = ownerSearch.findById(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(user);
        search.setAccessibleOwnersImplicit();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem.getUuid(), addedMetadataItem.getOwner());

        if (pemManager.canWrite(user)) {
            assertTrue(bolCanWrite, message);
            MetadataItem deletedItem = search.deleteMetadataItem(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());

            assertEquals(deletedItem, addedMetadataItem, "Deleted item should match the original MetadataItem.");

            MetadataItem afterDelete = search.findById(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());
            assertNull(afterDelete, "MetadataItem should not be found after delete.");
        } else {
            assertFalse(bolCanWrite, message);
        }


    }

}

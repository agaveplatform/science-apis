package org.iplantc.service.metadata.dao;


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
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataPermissionManagerIT;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.testng.Assert.*;

@Test(groups = {"integration"})
public class MetadataDaoIT extends AbstractMetadataDaoIT {
    private final String TEST_USER = "TEST_USER";
    private final String TEST_SHARED_USER = "TEST_SHARE_USER";
    private final String TEST_SHARED_USER2 = "TEST_SHARE_USER_2";
    private MongoCollection collection;

    private final ObjectMapper mapper = new ObjectMapper();

//    @Mock
//    private MongoClient mockClient;
//
//    @Mock
//    private MongoDatabase mockDB;
//
//    @Mock
//    private MongoCollection mockCollection;
//
//    @InjectMocks
//    private MetadataDao wrapper;

    @BeforeTest
    public void setUpCollection() {
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
    }

    @AfterMethod
    public void cleanUp() {
        collection.deleteMany(new Document());
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return a persisted instance of the entity
     */
    @Override
    public MetadataItem createEntity() {
        MetadataItem entity = null;
        try {
            entity = new MetadataItem();
            entity.setName(MetadataPermissionManagerIT.class.getName());
            entity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
            entity.setOwner(TEST_USER);
            entity.setInternalUsername(TEST_USER);
            entity.getAssociations().add(new AgaveUUID(UUIDType.METADATA).toString());
            entity.setSchemaId(new AgaveUUID(UUIDType.SCHEMA).toString());
            entity.getPermissions().add(new MetadataPermission(TEST_SHARED_USER, PermissionType.READ));

        } catch (Exception e) {
            fail("Unable to create metadata item", e);
        }

        return entity;
    }

    /**
     * Create and insert a test entity persisted and available for lookup
     *
     * @return a persisted instance of the entity
     */
    @Override
    public MetadataItem insertEntity() {
        MetadataItem addItem = createEntity();
        collection.insertOne(addItem);
        return addItem;
    }

    @Override
    public void insertTest() {
        MetadataItem addedItem = insertEntity();

        MetadataDao dao = MetadataDao.getInstance();
        List<MetadataItem> foundItems = dao.find(TEST_USER, new Document("uuid", addedItem.getUuid()));
        for (MetadataItem metadataItem : foundItems) {
            assertEquals(metadataItem, addedItem, "Added MetadataItem should match the original MetadataItem.");
            assertEquals(metadataItem.getUuid(), addedItem.getUuid(), "Uuid of added MetadataItem should match the original MetadataItem.");
            assertEquals(metadataItem.getOwner(), addedItem.getOwner(), "Added Metadata owner should match the original MetadataItem owner.");
            assertEquals(metadataItem.getTenantId(), addedItem.getTenantId(), "Added Metadata tenant should match the original Metadata owner.");
        }
    }

    @Override
    public void insertNullMetadataItem() throws MetadataStoreException {
        MetadataDao dao = MetadataDao.getInstance();
        try {
            dao.insert(null);
            fail("Inserting null MetadataItem should throw MetadataException.");
        } catch (MetadataException e) {
            assertEquals(e.getMessage(), "Cannot insert a null MetadataItem");
        }
    }

    @DataProvider(name = "initMetadataItemProvider")
    public Object[][] initMetadataItemProvider() {
        return new Object[][]{
                {insertEntity(), "Added MetadataItem should be returned.", 1, false},
                {createEntity(), "MetadataItem that doesn't exist should return null/empty list", 0, false},
                {null, "Null MetadataItem should throw MetadataException", 0, true}
        };
    }

    @Override
    @Test(dataProvider = "initMetadataItemProvider")
    public void deleteMetadataTest(MetadataItem metadataItem, String message, int resultSize, boolean bolThrowException) {
        MetadataDao dao = new MetadataDao().getInstance();

        try {
            MetadataItem deletedItem = dao.deleteMetadata(metadataItem);
            if (bolThrowException)
                fail("Exception should be thrown: " + message);


            List<MetadataItem> resultList = dao.find(TEST_USER, new Document("uuid", metadataItem.getUuid()));

            if (deletedItem == null) {
                assertEquals(resultList.size(), resultSize, message);
            } else {
                assertNotNull(deletedItem, "Removed item should be returned after successful delete.");
                assertEquals(deletedItem.getUuid(), metadataItem.getUuid(), "Deleted item uuid should match.");

                assertEquals(resultList.size(), 0, "Searching for removed metadata item by its uuid should return an empty list.");
            }

        } catch (Exception e) {
            if (!bolThrowException)
                fail("Exception should not be thrown: " + message);
        }

    }

    @Override
    @Test(dataProvider = "initMetadataItemProvider")
    public void findTest(MetadataItem metadataItem, String message, int findSize, boolean bolThrowException) {
        MetadataDao dao = MetadataDao.getInstance();

        try {
            dao.setAccessibleOwners(Arrays.asList(metadataItem.getOwner()));
            List<MetadataItem> foundItems = dao.find(metadataItem.getOwner(), new Document("uuid", metadataItem.getUuid()));

            if (bolThrowException)
                fail("Exception should be thrown: " + message);

            assertEquals(foundItems.size(), findSize, message);

            for (MetadataItem foundItem : foundItems)
                assertEquals(foundItem, metadataItem, "MetadataItem found should match the added entity.");
        } catch (Exception e) {
            if (!bolThrowException) {
                fail("Exception should not be thrown: " + message);
            }
        }
    }

    @Override
    @Test(dataProvider = "initMetadataItemProvider")
    public void findSingleMetadataItemTest(MetadataItem metadataItem, String message, int findSize, boolean bolThrowException) {
        MetadataDao dao = MetadataDao.getInstance();

        try {
            MetadataItem foundItem = dao.findSingleMetadataItem(and(eq("uuid", metadataItem.getUuid()),
                    eq("tenantId", metadataItem.getTenantId())));


            if (bolThrowException)
                fail("Exception should be thrown: " + message);

            if (foundItem == null) {
                assertEquals(findSize, 0, message);
            } else
                assertEquals(foundItem, metadataItem, "MetadataItem found should match the created entity.");

        } catch (Exception e) {
            if (!bolThrowException) {
                fail("Exception should be thrown: " + message);
            }
        }
    }

    @DataProvider(name = "initMetadataPermissionDataProvider")
    public Object[][] initMetadataPermissionDataProvider() throws MetadataException {
        return new Object[][]{
                {new MetadataPermission(TEST_SHARED_USER, PermissionType.READ)},
                {new MetadataPermission(TEST_SHARED_USER, PermissionType.READ_WRITE)},
                {new MetadataPermission(TEST_SHARED_USER, PermissionType.NONE)},
                {new MetadataPermission(TEST_SHARED_USER, PermissionType.UNKNOWN)},
                {new MetadataPermission(TEST_SHARED_USER2, PermissionType.getIfPresent("SOME OTHER PERMISSION"))},
                {new MetadataPermission(TEST_SHARED_USER2, PermissionType.READ)},
                {new MetadataPermission(TEST_SHARED_USER2, PermissionType.WRITE)},
        };
    }

    @Override
    @Test(dataProvider = "initMetadataPermissionDataProvider")
    public void updatePermissionTest(MetadataPermission permission) throws MetadataStoreException {
        MetadataItem addedItem = insertEntity();
        addedItem.getPermissions().add(permission);

        MetadataDao dao = MetadataDao.getInstance();
        List<MetadataPermission> updatedPermissions = dao.updatePermission(addedItem);
        Assert.assertTrue(updatedPermissions.containsAll(addedItem.getPermissions()), "Updated permissions should contain all permissions from the original MetadataItem.");
    }

    @Override
    public void updateDocumentTest() throws MetadataException {
        //insert item
        MetadataItem addedItem = insertEntity();

        //create document to update with
        Document updateDoc = new Document("uuid", addedItem.getUuid())
                .append("tenantId", addedItem.getTenantId())
                .append("name", "Updated Name")
                .append("value", new Document("updatedKey", "updatedValue"))
                .append("invalidField", "invalidValue");

        MetadataDao dao = MetadataDao.getInstance();
//        Document updatedDoc = dao.updateDocument(updateDoc);

        Document updatedDoc = dao.updateDocument(updateDoc);

        assertEquals(updatedDoc.getString("uuid"), addedItem.getUuid(),
                "Updated document uuid should match the initial MetadataItem.");
        assertEquals(updatedDoc.getString("owner"), addedItem.getOwner(),
                "Updated document owner should match the initial MetadataItem.");
        assertEquals(updatedDoc.getString("tenantId"), addedItem.getTenantId(),
                "Updated document tenant should match the initial MetadataItem.");
        assertEquals(updatedDoc.getString("name"), updatedDoc.getString("name"),
                "Updated document name should be updated and match with the document used for updating.");
        assertEquals(updatedDoc.get("value"), updatedDoc.get("value"),
                "Updated document value should updated and match with the document used for updating.");
        assertEquals(updatedDoc.getList("associationIds", String.class), addedItem.getAssociations().getAssociatedIds().keySet(),
                "Updated document associations should match the initial MetadataItem since it was not included in the document used for updating.");


        List<Document> permissionDoc = updatedDoc.getList("permissions", Document.class);
        for (Document pemDoc: permissionDoc) {
            boolean found = false;
            for (MetadataPermission pem : addedItem.getPermissions()) {
                if (pemDoc.getString("username").equals(pem.getUsername())){
                    found = true;
                    assertEquals(pemDoc.getString("permission"), pem.getPermission().toString());
                }
            }
            if (!found)
                fail("All permissions that exist in the original MetadataItem should be found since it was not included in the document for updating.");
        }

        List<Document> allMatchingDocuments = dao.filterFind(and(eq("uuid", addedItem.getUuid()), eq("tenantId", addedItem.getTenantId())), new Document());
        assertNotNull(allMatchingDocuments, "Updated metadata item should be returned in query by tenant and uuid");
        assertEquals(allMatchingDocuments.size(), 1, "Updated metadata item should be only result in query by tenant and uuid.");

        MetadataItem updatedItem = dao.findSingleMetadataItem(and(eq("uuid", addedItem.getUuid()), eq("tenantId", addedItem.getTenantId())));
        assertEquals(updatedItem.getOwner(), addedItem.getOwner(),
                "Retrieved MetadataItem owner should be the same as original MetadataItem");
        assertEquals(updatedItem.getName(), updatedDoc.get("name"),
                "Retrieved MetadataItem should also have updated name value");
        assertEquals(updatedItem.getValue().get("testKey"), updatedDoc.getEmbedded(Arrays.asList("value", "testKey"), String.class),
                "Retrieved MetadataItem should also have updated value");
        assertEquals(updatedItem.getAssociations().getAssociatedIds().keySet(), addedItem.getAssociations().getAssociatedIds().keySet(),
                "Retrieved MetadataItem should match original associationIds since it was not included in update");
        assertEquals(updatedItem.getPermissions(), addedItem.getPermissions(),
                "Retrieved MetadataItem should match original permissions since it was not included in update");

    }

    @DataProvider(name = "initOffsetAndLimitDataProvider")
    public Object[][] initOffsetAndLimitDataProvider() {
        return new Object[][]{
                {0, Settings.DEFAULT_PAGE_SIZE, 5, "Offset of 0 and limit of DEFAULT_PAGE_SIZE should return all matching items."}, //default offset and limit
                {2, 3, 3, "Result should have an offset of 2 with a limit of 3 matching items."},
                {10, 0, 0, "Nothing should be returned if offset is more than matched items."},
                {0, 10, 5, "All matched items should be returned if limit is more than matched items."},
                {4, 2, 1, "If remaining matched items after offset is less than the limit, return all remaining items"}
        };
    }

    @Override
    @Test(dataProvider = "initOffsetAndLimitDataProvider")
    public void findWithOffsetAndLimitTest(int offset, int limit, int expectedSize, String message) {
        MetadataDao dao = MetadataDao.getInstance();

        List<MetadataItem> addedItems = new ArrayList<>();
        for (int numItems = 0; numItems < 5; numItems++) {
            MetadataItem toAdd = insertEntity();
            addedItems.add(toAdd);
        }

        Assert.assertEquals(addedItems.size(), 5);
        dao.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        List<MetadataItem> foundItems = dao.find(TEST_USER, new Document(), offset, limit, new Document());
        assertEquals(foundItems.size(), expectedSize, message);

        for (MetadataItem foundItem : foundItems) {
            boolean found = false;
            for (MetadataItem addedItem : addedItems) {
                if (addedItem.getUuid().equals(foundItem.getUuid())) {
                    found = true;
                    assertEquals(foundItem, addedItem, "Item found should match the item added.");
                }
            }
            if (!found)
                fail("Metadata found should exist in the list of added MetadataItems.");
        }
    }

    @Override
    public void findSingleMetadataItemEmptyFilterTest() throws MetadataStoreException, MetadataException {
        MetadataItem toAdd = createEntity();
        MetadataDao dao = MetadataDao.getInstance();

        MetadataItem addedItem = dao.insert(toAdd);
        MetadataItem foundItem = dao.findSingleMetadataItem(new Document());
        assertEquals(foundItem, addedItem, "Empty filter should return the first Metadata item found");
    }

    @Override
    public void getPermissionTest() throws MetadataException {
        MetadataItem metadataItem = insertEntity();

        MetadataDao dao = MetadataDao.getInstance();

        MetadataPermission sharedPermission = dao.getPermission(metadataItem.getUuid(), TEST_SHARED_USER, metadataItem.getTenantId());
        if (sharedPermission == null)
            assertNull(metadataItem.getPermissionForUsername(TEST_SHARED_USER), "User permission should match the added MetadataItem.");

        MetadataPermission ownerPermission = dao.getPermission(metadataItem.getUuid(), TEST_USER, metadataItem.getTenantId());
        assertNotNull(ownerPermission, "Owner permission should be returned.");
        assertEquals(ownerPermission, metadataItem.getPermissionForUsername(TEST_USER), "Owner permission should match with the added MetadataItem");
        assertEquals(ownerPermission.getPermission(), PermissionType.ALL, "Owner should have PermissionType.All");

        MetadataPermission invalidPermission = dao.getPermission(metadataItem.getUuid(), TEST_SHARED_USER2, metadataItem.getTenantId());
        assertNull(invalidPermission, "Null should be returned if no permissions found for user for given uuid.");
    }

//    @Override
//    public void findMetadataItemWithFiltersTest() {
//        MetadataDao dao = MetadataDao.getInstance();
//
//        MetadataItem metadataItem = insertEntity();
//
//        Document docFilter = new Document("uuid", 1)
//                .append("name", 1)
//                .append("value", 1)
//                .append("invalidField", 1);
//
//        List<Document> foundItems = dao.filterFind(new Document("uuid", metadataItem.getUuid()), docFilter);
//
//        for (Document foundDoc : foundItems) {
//            assertEquals(foundDoc.get("uuid"), metadataItem.getUuid(), "Document found should include the filtered field 'uuid' and match with the added MetadataItem.");
//            assertEquals(foundDoc.get("name"), metadataItem.getName(), "Document found should include the filtered field 'name' and match with the added MetadataItem.");
//            assertEquals(foundDoc.getEmbedded(List.of("value", "testKey"), String.class), metadataItem.getValue().get("testKey").asText(), "Document found should include the filtered field 'value' and match with the added MetadataItem.");
//            assertNull(foundDoc.get("permissions"), "Items not included in the filter should return null.");
//            assertNull(foundDoc.get("associationId"), "Item not included in the filter should return null.");
//            assertNull(foundDoc.get("invalidField"), "Invalid/missing fields should not be included in the result.");
//        }
//    }

    @DataProvider(name = "initFiltersDataProvider")
    public Object[][] initFiltersDataProvider() {
        return new Object[][]{
                {new Document("uuid", 1).append("name", 1).append("value.testKey", 1)},
                {new Document()},
                {new Document("invalidField", 1)}
        };
    }

    @Override
    @Test
    public void findMetadataItemWithFiltersTest() {
        MetadataDao dao = MetadataDao.getInstance();

        MetadataItem metadataItem = insertEntity();

        Document docFilter = new Document("uuid", 1)
                .append("name", 1)
                .append("value", 1)
                .append("invalidField", 1);

        List<Document> foundItems = dao.filterFind(new Document("name", MetadataPermissionManagerIT.class.getName()), docFilter);

        assertEquals(foundItems.size(), 1, "Added item should be found.");
        assertEquals(foundItems.get(0).size(), docFilter.size(), "Number of fields in found item should match the number of filters.");

        for (Document foundDoc : foundItems) {
            assertEquals(foundDoc.get("uuid"), metadataItem.getUuid(), "Document found should include the filtered field 'uuid' and match with the added MetadataItem.");
            assertEquals(foundDoc.get("name"), metadataItem.getName(), "Document found should include the filtered field 'name' and match with the added MetadataItem.");
            assertEquals(foundDoc.getEmbedded(List.of("value", "testKey"), String.class), metadataItem.getValue().get("testKey").asText(), "Document found should include the filtered field 'value' and match with the added MetadataItem.");
            assertNull(foundDoc.get("permissions"), "Items not included in the filter should return null.");
            assertNull(foundDoc.get("associationId"), "Item not included in the filter should return null.");
            assertNull(foundDoc.get("invalidField"), "Invalid/missing fields should not be included in the result.");
        }
    }

    @DataProvider(name="initUsersForReadPermissionCheckDataProvider")
    public Object[][] initUsersForReadPermissionCheckDataProvider(){
        return new Object[][]{
                {TEST_USER, true, "Owner should be able to read Metadata Item."},
                {TEST_SHARED_USER, true, "User with READ permission should be able to read Metadata Item."},
                {TEST_SHARED_USER2, false, "User without permission set should not be able to read Metadata Item."},
        };
    }

    @Override
    @Test(dataProvider = "initUsersForReadPermissionCheckDataProvider")
    public void checkHasReadQueryTest(String user, boolean bolHasRead, String message) throws MetadataStoreException, MetadataException {
        MetadataItem toAdd = createEntity();
        MetadataDao dao = new MetadataDao();
        MetadataItem addedItem = dao.insert(toAdd);
        assertEquals(dao.hasRead(user, addedItem.getUuid()), bolHasRead, message);

    }

    @DataProvider(name="initUsersForWritePermissionCheckDataProvider")
    public Object[][] initUsersForWritePermissionCheckDataProvider(){
        return new Object[][]{
                {TEST_USER, true, "Owner should be able to write to Metadata Item."},
                {TEST_SHARED_USER, false, "User with READ permission should not be able to  write to  Metadata Item."},
                {TEST_SHARED_USER2, true, "User with READ_WRITE permission should be able to  write to  Metadata Item."},
                {"INVALID_USER", false, "User without permission set should not be able to read Metadata Item."},
        };
    }

    @Override
    @Test(dataProvider = "initUsersForWritePermissionCheckDataProvider")
    public void checkHasWriteQueryTest(String user, boolean bolHasWrite, String message) throws MetadataStoreException, MetadataException {
        MetadataItem toAdd = createEntity();
        toAdd.getPermissions().add(new MetadataPermission(TEST_SHARED_USER2, PermissionType.READ_WRITE));

        MetadataDao dao = new MetadataDao();
        MetadataItem addedItem = dao.insert(toAdd);
        assertEquals(dao.hasWrite(user, addedItem.getUuid()), bolHasWrite, message);
    }

}

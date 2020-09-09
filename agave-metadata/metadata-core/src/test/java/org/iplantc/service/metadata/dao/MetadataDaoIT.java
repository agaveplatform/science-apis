package org.iplantc.service.metadata.dao;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import org.bson.conversions.Bson;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataPermissionManagerIT;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.model.serialization.MetadataItemSerializer;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Test(groups={"integration"})
public class MetadataDaoIT extends AbstractMetadataDaoIT {
    private final String TEST_USER = "testuser";
    private final String TEST_SHARED_USER = "testshareuser";
    private final String TEST_SHARED_USER2 = "testshareuser2";

    private ObjectMapper mapper = new ObjectMapper();

    @Mock
    private MongoClient mockClient;

    @Mock
    private MongoDatabase mockDB;

    @Mock
    private MongoCollection mockCollection;

    @InjectMocks
    private MetadataDao wrapper;

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

            MetadataPermission metaPem = new MetadataPermission(entity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
            List<MetadataPermission> listPem = new ArrayList<>();
            listPem.add(metaPem);
            entity.setPermissions(listPem);

            wrapper.insert(entity);
            //MetadataDao.getInstance().insert(entity);
        } catch (Exception e) {
            Assert.fail("Unable to create metadata item", e);
        }

        return entity;
    }

    @Override
    public void insertTest() throws  MetadataException, PermissionException {
        //Create item to insert
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        testEntity.setPermissions(new ArrayList<>(Arrays.asList(metaPem)));

        MetadataDao inst = wrapper.getInstance();

        //clean collection
        inst.clearCollection();

        List<String> accessibleOwners = new ArrayList<>();
        accessibleOwners.add(TEST_USER);
        inst.setAccessibleOwners(accessibleOwners);
        MetadataItem updatedItem = inst.updateMetadata(testEntity, TEST_USER);

        List<MetadataItem>  firstResult = inst.find(TEST_USER, new Document("uuid", updatedItem.getUuid()));
        Assert.assertEquals(firstResult.get(0).getOwner(), TEST_USER);
        Assert.assertEquals(firstResult.get(0).getName(),MetadataDaoIT.class.getName());
        Assert.assertEquals(firstResult.get(0).getValue().get("testKey"), testEntity.getValue().get("testKey"));
        Assert.assertEquals(firstResult.get(0).getPermissions().size(), 1);
        Assert.assertEquals(firstResult.get(0).getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.ALL);
        Assert.assertEquals(firstResult.get(0), updatedItem, "Added Metadata item should be found in the collection.");
    }

    @Override
    public void insertPermissionTest() throws MetadataStoreException, MetadataException, PermissionException {
        MetadataDao inst = wrapper.getInstance();
        inst.clearCollection();

        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);

        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        MetadataItem addedItem = inst.updateMetadata(testEntity, TEST_USER);

        MetadataPermission pemShareUser = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        MetadataPermission pemShareUser2 = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER2, PermissionType.READ_WRITE);
        List<MetadataPermission> addList = new ArrayList<>(Arrays.asList(pemShareUser, pemShareUser2));
        addedItem.setPermissions(addList);
        List<MetadataPermission> updatedPermissions = inst.updatePermission(addedItem, TEST_USER);

        Assert.assertNotNull(updatedPermissions, "updatePermission should return the successfully updated permissions list.");

        List<MetadataItem> updatedItem = inst.find(TEST_USER, new Document("uuid", addedItem.getUuid()));
        Assert.assertEquals(updatedItem.get(0).getPermissions().size(), 2, "There should be 2 permissions added.");
        Assert.assertEquals(updatedItem.get(0).getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.ALL,
                "Permission added for " + TEST_SHARED_USER + " should be ALL.");
        Assert.assertEquals(updatedItem.get(0).getPermissions_User(TEST_SHARED_USER2).getPermission(), PermissionType.READ_WRITE,
                "Permission added for " + TEST_SHARED_USER2 + " should be READ_WRITE.");
    }

    @Override
    public void removePermissionTest() throws MetadataStoreException, MetadataException, PermissionException {
        MetadataDao inst = wrapper.getInstance();
        inst.clearCollection();

        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);

        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        MetadataItem addedItem = inst.updateMetadata(testEntity, TEST_USER);
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
        testEntity.setPermissions(new ArrayList<>(Arrays.asList(metaPem)));

        MetadataPermission sharedUserPermission = testEntity.getPermissions_User(TEST_SHARED_USER);

        testEntity.updatePermissions_delete(sharedUserPermission);
        List<MetadataPermission> updatedPermissions = inst.updatePermission(testEntity, TEST_USER);

        List<MetadataItem> resultList = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));

        Assert.assertNull(resultList.get(0).getPermissions_User(TEST_SHARED_USER), "Removed permission should return null.");
    }

    @Override
    public void removeMetadataTest() throws MetadataException, PermissionException {
        //add entity
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        MetadataPermission metaPem2 = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER2, PermissionType.ALL);
        testEntity.setPermissions(new ArrayList<>(Arrays.asList(metaPem, metaPem2)));

        MetadataDao inst = wrapper.getInstance();

        //clean collection
        inst.clearCollection();
        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));

        MetadataItem addedMetadataItem = inst.updateMetadata(testEntity, TEST_USER);

        Assert.assertNotNull(addedMetadataItem, "Metadata item should be added before removal.");

        MetadataItem removedItem = inst.deleteMetadata(addedMetadataItem, TEST_USER);
        List<MetadataItem> resultList  = inst.find(TEST_USER, new Document("uuid", addedMetadataItem.getUuid()));

        Assert.assertNotNull(removedItem, "Removed item should be returned after successful delete.");
        Assert.assertEquals(resultList.size(), 0, "Searching for removed metadata item by its uuid should return an empty list.");

//
//        Document findDoc_SharedUser = new Document("uuid", testEntity.getUuid())
//                .append("permissions.username", TEST_SHARED_USER);
//        Document findDoc_User = new Document("uuid", testEntity.getUuid())
//                .append("permissions.username", TEST_USER);
//        List<MetadataItem>  insertResult = inst.find(TEST_SHARED_USER, findDoc_SharedUser);

        //check metadataItem was added
//        Assert.assertEquals(inst.getCollectionSize(), 1);
//        List<MetadataItem> resultList = inst.findAll();
//        Assert.assertEquals(resultList.size(), 1, "Should have 1 document in the collection after inserting.");
//        Assert.assertEquals(insertResult.get(0).getPermissions().size(), 2, "Permissions should be 2 after adding the metadataitem.");

        //remove permission for user
//        MetadataPermission permissionToRemove = testEntity.getPermissions_User(TEST_SHARED_USER);
//        testEntity.updatePermissions_delete(permissionToRemove);
//        inst.updatePermission(testEntity, TEST_SHARED_USER);
//        Assert.assertEquals(inst.getCollectionSize() , 1, "Removing a user's permission should not remove the document in the collection");
//
//        //check permission removed
//        Document docToRemove = new Document("uuid", testEntity.getUuid())
//                .append("permissions.username", TEST_SHARED_USER);
//
//        List<MetadataItem> pemRemoveResult = inst.find(TEST_SHARED_USER, findDoc_SharedUser);
//        Assert.assertEquals(pemRemoveResult.size(), 0,"Nothing should be found for the user removed.");
//
//        //secondary check
//        List<MetadataItem>  pemRemoveResult_2 = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));
//        Assert.assertEquals(pemRemoveResult_2.get(0).getPermissions().size(),1, "Permissions list should be 1 after removing 1 user permission");
//
//        //remove metadataItem
//        MetadataItem removeItem = inst.deleteMetadata(testEntity, TEST_USER);
//        Assert.assertNotNull(removeItem, "Item was not removed successfully");
//        Assert.assertEquals(inst.getCollectionSize(), 0, "Collection size should be 0 after removing");
//
//        //check metadataitem removed
//        List<MetadataItem>  removeResult = inst.find(TEST_USER, findDoc_User);
//        Assert.assertEquals(removeResult.size(), 0, "Nothing should be found for the metadata item removed");
    }

    @Override
    public void updateTest() throws MetadataException, MetadataStoreException, PermissionException {
        //add entity without any permissions
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        List<MetadataPermission> listPem = new ArrayList<>();
        testEntity.setPermissions(listPem);

        MetadataDao inst = wrapper.getInstance();

        Document docQuery = new Document("uuid", testEntity.getUuid())
                .append("tenantId", testEntity.getTenantId());

        Bson docFilter = inst.createQueryFromMetadataItem(testEntity);

        //clean collection
        inst.clearCollection();
        Assert.assertEquals(inst.getCollectionSize(), 0);

        //insert metadataItem
        List<String> accessibleOwners = new ArrayList<>();
        inst.setAccessibleOwners(accessibleOwners);
        inst.getMongoClients();
        inst.insert(testEntity);

        //check it was added
        if (inst.hasRead(TEST_SHARED_USER, testEntity.getUuid())) {
            List<MetadataItem>  firstResult = inst.find(TEST_SHARED_USER, docFilter);
            Assert.assertNull(firstResult, "Item should not be found because no permissions were set for the user yet.");
        }
        Assert.assertEquals(inst.getCollectionSize(), 1);

        //add permission for test share user with read
        MetadataPermission sharedUserPermission = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
        List<MetadataPermission> metadataPermissionList = testEntity.getPermissions();
        metadataPermissionList.add(sharedUserPermission);
        testEntity.setPermissions(metadataPermissionList);
        List<MetadataPermission> updatePem = inst.updatePermission(testEntity, TEST_USER);

        //check permission updated
        Assert.assertEquals(inst.getCollectionSize(), 1, "Updating permission should not change collection size.");
        List<MetadataItem>  updatePemResult = inst.find(TEST_SHARED_USER, docFilter);

//        Assert.assertNotNull(updatePemResult, "User permission updated should not be null");
        List<MetadataItem> testResult = inst.find(TEST_USER, docFilter);
        List<MetadataItem>  newResult = inst.find(TEST_USER, new Document());
        List<MetadataItem> resultList = inst.findAll();

        Assert.assertNotNull(updatePemResult, "Item should be found after adding");
        Assert.assertEquals(updatePemResult.get(0).getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.READ,
                "Permission for user should be READ after updating.");

        //change metadata value
        testEntity.setValue(mapper.createObjectNode().put("newKey", "newValue"));

        MetadataItem updateResultItem = null;
        if (inst.hasWrite(TEST_SHARED_USER, testEntity.getUuid())) {
            updateResultItem = inst.updateMetadata(testEntity, TEST_SHARED_USER);
        }

        //metadata should not be updated
        Assert.assertNull(updateResultItem, "User does not have correct permissions, metataItem should not be updated.");

        //update permission to read_write
        sharedUserPermission = testEntity.getPermissions_User(TEST_SHARED_USER);
        sharedUserPermission.setPermission(PermissionType.READ_WRITE);
        testEntity.updatePermissions(sharedUserPermission);

        List<MetadataPermission> metadataPermission = inst.updatePermission(testEntity, TEST_USER);

        Assert.assertNotNull(metadataPermission, "Permission should be updated");
        Assert.assertTrue(metadataPermission.size() > 0, "Permission should be updated");

        if (inst.hasWrite(TEST_SHARED_USER, testEntity.getUuid())){
            updateResultItem = inst.updateMetadata(testEntity, TEST_SHARED_USER);
        }

        Assert.assertNotNull(updateResultItem, "User has correct permissions, metadataItem should be updated.");

        //metadata value should be updated
        List<MetadataItem> updateResult = inst.find(TEST_SHARED_USER, docQuery);
        Assert.assertEquals(updateResult.get(0).getValue(), testEntity.getValue());
    }

    @Override
    public void updatePermissionTest() throws MetadataStoreException, MetadataException, PermissionException {
        MetadataDao inst = wrapper.getInstance();
        inst.clearCollection();

        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission pemShareUser = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        List<MetadataPermission> listPem = new ArrayList<>(Arrays.asList(pemShareUser));
        testEntity.setPermissions(listPem);

        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        MetadataItem addedItem = inst.updateMetadata(testEntity, TEST_USER);

        pemShareUser.setPermission(PermissionType.READ);
        MetadataPermission pemShareUser2 = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER2, PermissionType.READ_WRITE);
        List<MetadataPermission> updatedList = new ArrayList<>(Arrays.asList(pemShareUser, pemShareUser2));
        addedItem.setPermissions(updatedList);
        List<MetadataPermission> updatedPermissions = inst.updatePermission(addedItem, TEST_USER);

        Assert.assertNotNull(updatedPermissions, "updatePermission should return the successfully updated permissions list.");

        List<MetadataItem> updatedItem = inst.find(TEST_USER, new Document("uuid", addedItem.getUuid()));
        Assert.assertEquals(updatedItem.get(0).getPermissions().size(), 2, "There should be 2 permissions added.");
        Assert.assertEquals(updatedItem.get(0).getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.READ,
                "Permission for " + TEST_SHARED_USER + " should be updated to READ.");
        Assert.assertEquals(updatedItem.get(0).getPermissions_User(TEST_SHARED_USER2).getPermission(), PermissionType.READ_WRITE,
                "Permission for " + TEST_SHARED_USER2 + " should be added as READ_WRITE.");
    }

    @Override
    public void findMetadataTest() throws MetadataException, PermissionException {
        MetadataDao inst = wrapper.getInstance();
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);


        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        inst.updateMetadata(testEntity, TEST_USER);


        MetadataItem foundItem = inst.findSingleMetadataItem(new Document("uuid", testEntity.getUuid()));
        Assert.assertEquals(foundItem, testEntity, "MetadataItem found should match the created entity.");
    }

    @Override
    public void findPermissionTest() throws MetadataException,PermissionException {
        MetadataDao inst = wrapper.getInstance();
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission pemShareUser = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
        List<MetadataPermission> listPem = new ArrayList<>(Arrays.asList(pemShareUser));
        testEntity.setPermissions(listPem);

        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        inst.updateMetadata(testEntity, TEST_USER);
        MetadataItem foundItem = inst.findSingleMetadataItem(new Document("uuid", testEntity.getUuid()));
        Assert.assertEquals(foundItem.getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.READ);
    }

    @Override
    public void findMetadataItemWithFiltersTest() throws MetadataException, PermissionException {
        ObjectNode value = mapper.createObjectNode().put("testKey", "testValue");

        MetadataDao inst = wrapper.getInstance();
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(value);
        testEntity.setOwner(TEST_USER);
        MetadataPermission pemShareUser = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
        List<MetadataPermission> listPem = new ArrayList<>(Arrays.asList(pemShareUser));
        testEntity.setPermissions(listPem);
        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        inst.updateMetadata(testEntity, TEST_USER);

        Document docFilter = new Document("uuid", 1)
                .append("name",1)
                .append("value", 1);

        List<Document> foundItems = inst.filterFind(new Document("uuid", testEntity.getUuid()), docFilter);

        Assert.assertEquals(foundItems.get(0).get("uuid"), testEntity.getUuid(), "Document found should include the filtered field 'uuid'.");
        Assert.assertEquals(foundItems.get(0).get("name"), MetadataDaoIT.class.getName(), "Document found should include the filtered field 'name'.");
        Assert.assertEquals(foundItems.get(0).getEmbedded(List.of("value", "testKey"), String.class), "testValue", "Document found should include the filtered field 'value'.");
        Assert.assertNull(foundItems.get(0).get("permissions"), "Items not included in the filter should return null.");
    }

    @Override
    public void findMetadataItemWithInvalidFiltersTest() throws MetadataException, PermissionException {
        ObjectNode value = mapper.createObjectNode().put("testKey", "testValue");

        MetadataDao inst = wrapper.getInstance();
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(value);
        testEntity.setOwner(TEST_USER);
        MetadataPermission pemShareUser = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.READ);
        List<MetadataPermission> listPem = new ArrayList<>(Arrays.asList(pemShareUser));
        testEntity.setPermissions(listPem);
        inst.setAccessibleOwners(new ArrayList<>(Arrays.asList(TEST_USER)));
        inst.updateMetadata(testEntity, TEST_USER);

        Document docFilter = new Document("uuid", 1)
                .append("name",1)
                .append("value", 1)
                .append("invalidField", 1);

        List<Document> foundItems = inst.filterFind(new Document("uuid", testEntity.getUuid()), docFilter);

        Assert.assertNull(foundItems.get(0).get("invalidField"), "Invalid/missing fields should not be included in the result.");
    }
}

package org.iplantc.service.metadata.dao;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import org.bson.conversions.Bson;
import org.bson.Document;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataPermissionManagerIT;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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

            MetadataPermission metaPem = new MetadataPermission(entity.getUuid(), TEST_USER, PermissionType.ALL);
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

    @Test
    public void insertTest() throws MetadataStoreException, MetadataException, UnknownHostException {

        //Create item to insert
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        List<MetadataPermission> listPem = new ArrayList<>();
        listPem.add(metaPem);
        testEntity.setPermissions(listPem);

        MetadataDao inst = wrapper.getInstance();

        //clean collection
        inst.clearCollection();
        Assert.assertEquals(inst.getCollectionSize(), 0);

        inst.getMongoClients();
        inst.insertMetadataItem(testEntity);
        List<MetadataItem>  firstResult = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));

        Assert.assertNotNull(firstResult, "Document should be found by uuid after inserting to collection");

        //check metadata item was inserted
        Assert.assertEquals(inst.getCollectionSize(), 1, "Document size should be 1 after inserting new document.");
        Assert.assertEquals(firstResult.get(0).getOwner(), TEST_USER);
        Assert.assertEquals(firstResult.get(0).getName(),MetadataDaoIT.class.getName());
        Assert.assertEquals(firstResult.get(0).getValue().get("testKey"), testEntity.getValue().get("testKey"));
        Assert.assertEquals(firstResult.get(0).getPermissions().size(), 1);
    }

    @Test
    public void removeTest() throws MetadataException, MetadataStoreException, UnknownHostException {
        //add entity
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        List<MetadataPermission> listPem = new ArrayList<>();
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        MetadataPermission metaPem2 = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER2, PermissionType.ALL);
        listPem.add(metaPem);
        listPem.add(metaPem2);
        testEntity.setPermissions(listPem);

        MetadataDao inst = wrapper.getInstance();

        //clean collection
        inst.clearCollection();
        Assert.assertEquals(inst.getCollectionSize(), 0);

        //insert metadataItem
        inst.getMongoClients();
        inst.insertMetadataItem(testEntity);

        Document findDoc_SharedUser = new Document("uuid", testEntity.getUuid())
                .append("permissions.username", TEST_SHARED_USER);
        Document findDoc_User = new Document("uuid", testEntity.getUuid())
                .append("permissions.username", TEST_USER);
        List<MetadataItem>  insertResult = inst.find(TEST_SHARED_USER, findDoc_SharedUser);

        //check metadataItem was added
        Assert.assertEquals(inst.getCollectionSize(), 1);
        List<MetadataItem> resultList = inst.findAll();
        Assert.assertEquals(resultList.size(), 1, "Should have 1 document in the collection after inserting.");
        Assert.assertEquals(insertResult.get(0).getPermissions().size(), 2, "Permissions should be 2 after adding the metadataitem.");

        //remove permission for user
        inst.deleteUserPermission(testEntity, TEST_SHARED_USER);
        Assert.assertEquals(inst.getCollectionSize() , 1, "Removing a user's permission should not remove the document in the collection");

        //check permission removed
        Document docToRemove = new Document("uuid", testEntity.getUuid())
                .append("permissions.username", TEST_SHARED_USER);

        List<MetadataItem>  pemRemoveResult = inst.find(TEST_SHARED_USER, findDoc_SharedUser);
        Assert.assertEquals(pemRemoveResult.size(), 0,"Nothing should be found for the user removed.");

        //secondary check
        List<MetadataItem>  pemRemoveResult_2 = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));
        Assert.assertEquals(pemRemoveResult_2.get(0).getPermissions().size(),1, "Permissions list should be 1 after removing 1 user permission");

        //remove metadataItem
        MetadataItem removeItem = inst.deleteMetadata(testEntity, TEST_USER);
        Assert.assertNotNull(removeItem, "Item was not removed successfully");
        Assert.assertEquals(inst.getCollectionSize(), 0, "Collection size should be 0 after removing");

        //check metadataitem removed
        List<MetadataItem>  removeResult = inst.find(TEST_USER, findDoc_User);
        Assert.assertEquals(removeResult.size(), 0, "Nothing should be found for the metadata item removed");
    }

    @Test
    public void updateTest() throws MetadataException, MetadataStoreException, UnknownHostException {
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

        Bson docFilter = inst.createDocQuery(testEntity);

        //clean collection
        inst.clearCollection();
        Assert.assertEquals(inst.getCollectionSize(), 0);

        //insert metadataItem
        inst.getMongoClients();
        inst.insertMetadataItem(testEntity);

        //check it was added
        if (inst.hasRead(TEST_SHARED_USER, testEntity.getUuid())) {
            List<MetadataItem>  firstResult = inst.find(TEST_SHARED_USER, docFilter);
            Assert.assertNull(firstResult, "Item should not be found because no permissions were set for the user yet.");
        }
        Assert.assertEquals(inst.getCollectionSize(), 1);

        //add permission for test share user with read
        MetadataPermission updatePem = inst.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ);

        //check permission updated
        Assert.assertEquals(inst.getCollectionSize(), 1, "Updating permission should not change collection size.");
        List<MetadataItem>  updatePemResult = inst.find(TEST_SHARED_USER, docFilter);

//        Assert.assertNotNull(updatePemResult, "User permission updated should not be null");
        List<MetadataItem> testResult = inst.find(TEST_USER, docFilter);

        List<MetadataItem>  newResult = inst.find(TEST_USER, new Document());

        List<MetadataItem> resultList = inst.findAll();

        Assert.assertNotNull(updatePemResult, "Item should be found after adding");
        Assert.assertEquals(updatePemResult.get(0).getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.READ, "Permission for user should be READ after updating.");

        //change metadata value
        testEntity.setValue(mapper.createObjectNode().put("newKey", "newValue"));

        MetadataItem updateResultItem = null;
        if (inst.hasWrite(TEST_SHARED_USER, testEntity.getUuid())) {
            updateResultItem = inst.updateMetadata(testEntity, TEST_SHARED_USER);
        }

        //metadata should not be updated
        Assert.assertNull(updateResultItem, "User does not have correct permissions, metataItem should not be updated.");

        //update permission to read_write
        inst.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ_WRITE);

        if (inst.hasWrite(TEST_SHARED_USER, testEntity.getUuid())){
            updateResultItem = inst.updateMetadata(testEntity, TEST_SHARED_USER);
        }

        Assert.assertNotNull(updateResultItem, "User has correct permissions, metadataItem should be updated.");

        //metadata value should be updated
        List<MetadataItem> updateResult = inst.find(TEST_SHARED_USER, docQuery);
        Assert.assertEquals(updateResult.get(0).getValue(), testEntity.getValue());
    }
}

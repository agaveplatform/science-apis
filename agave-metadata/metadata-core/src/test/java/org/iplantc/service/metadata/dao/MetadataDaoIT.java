package org.iplantc.service.metadata.dao;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import io.grpc.Metadata;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.managers.MetadataPermissionManagerIT;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.joda.time.DateTime;
import org.json.JSONString;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.persistence.Basic;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

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
            List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
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
    public void insertTest() throws MetadataStoreException, MetadataException, PermissionException, UnknownHostException {

        //Create item to insert
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
        listPem.add(metaPem);
        testEntity.setPermissions(listPem);

        MetadataDao inst = wrapper.getInstance();

        //clean collection
        inst.clearCollection();
        Assert.assertEquals(inst.getCollectionSize(), 0);

        inst.getMongoClients();
        inst.insertMetadataItem(testEntity);
        MetadataItem firstResult = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));

        Assert.assertNotNull(firstResult, "Document should be found by uuid after inserting to collection");

        //check metadata item was inserted
        Assert.assertEquals(inst.getCollectionSize(), 1, "Document size should be 1 after inserting new document.");
        Assert.assertEquals(firstResult.getOwner(), TEST_USER);
        Assert.assertEquals(firstResult.getName(),MetadataDaoIT.class.getName());
        Assert.assertEquals(firstResult.getValue().get("testKey"), testEntity.getValue().get("testKey"));
        Assert.assertEquals(firstResult.getPermissions().size(), 1);

    }

    @Test
    public void removeTest() throws MetadataException, MetadataStoreException, UnknownHostException {
        //add entity
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
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
        MetadataItem insertResult = inst.find(TEST_SHARED_USER, new Document("uuid", testEntity.getUuid()));

        //check metadataItem was added
        Assert.assertEquals(inst.getCollectionSize(), 1);
        List<MetadataItem> resultList = new ArrayList<MetadataItem>();
        resultList = inst.findAll();
        Assert.assertEquals(resultList.size(), 1, "Should have 1 document in the collection after inserting.");
        Assert.assertEquals(insertResult.getPermissions().size(), 2, "Permissions should be 2 after adding the metadataitem.");

        //remove permission for user
        inst.deleteUserPermission(testEntity, TEST_SHARED_USER);
        Assert.assertEquals(inst.getCollectionSize() , 1, "Removing a user's permission should not remove the document in the collection");

        //check permission removed
        MetadataItem pemRemoveResult = inst.find(TEST_SHARED_USER, new Document("uuid", testEntity.getUuid()));
        Assert.assertNull(pemRemoveResult,"Nothing should be found for the user removed.");

        //secondary check
        MetadataItem pemRemoveResult_2 = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));
        Assert.assertEquals(pemRemoveResult_2.getPermissions().size(),1, "Permissions list should be 1 after removing 1 user permission");

        //remove metadataItem
        MetadataItem removeItem = inst.deleteMetadata(testEntity, TEST_USER);
        Assert.assertNotNull(removeItem, "Item was not removed successfully");
        Assert.assertEquals(inst.getCollectionSize(), 0, "Collection size should be 0 after removing");

        //check metadataitem removed
        MetadataItem removeResult = inst.find(TEST_USER, new Document("uuid", testEntity.getUuid()));
        Assert.assertNull(removeResult, "Nothing should be found for the metadata item removed");
    }

    @Test
    public void updateTest() throws MetadataException, MetadataStoreException, UnknownHostException {

        //add entity without any permissions
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataDaoIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);
        List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
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
            MetadataItem firstResult = inst.find(TEST_SHARED_USER, docFilter);
            Assert.assertNull(firstResult, "Item should not be found because no permissions were set for the user yet.");
        }
        Assert.assertEquals(inst.getCollectionSize(), 1);

        //add permission for test share user with read
        MetadataPermission updatePem = inst.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ);

        //check permission updated
        Assert.assertEquals(inst.getCollectionSize(), 1, "Updating permission should not change collection size.");
        MetadataItem updatePemResult = inst.find(TEST_SHARED_USER, docFilter);

//        Assert.assertNotNull(updatePemResult, "User permission updated should not be null");
        MetadataItem testResult = inst.find(TEST_USER, docFilter);

        MetadataItem newResult = inst.find(TEST_USER, new Document());

        List<MetadataItem> resultList = inst.findAll();

        Assert.assertNotNull(updatePemResult, "Item should be found after adding");
        Assert.assertEquals(updatePemResult.getPermissions_User(TEST_SHARED_USER).getPermission(), PermissionType.READ, "Permission for user should be READ after updating.");

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
        MetadataItem updateResult = inst.find(TEST_SHARED_USER, docQuery);
        Assert.assertEquals(updateResult.getValue(), testEntity.getValue());

    }

}

package org.iplantc.service.metadata.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.DBCollectionUpdateOptions;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.persistence.Basic;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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

//            MetadataDao inst = mock(MetadataDao.class);
//            when(inst.insert(entity)).thenReturn(entity);

            wrapper.insert(entity);

            //MetadataDao.getInstance().insert(entity);
        } catch (Exception e) {
            Assert.fail("Unable to create metadata item", e);
        }

        return entity;
    }

    @Test
    public void insertTest() throws MetadataStoreException, MetadataException, PermissionException {
        mockDB = mock(MongoDatabase.class);
        mockCollection = mock(MongoCollection.class);
        mockClient = mock(MongoClient.class);

        when(mockClient.getDatabase(anyString())).thenReturn(mockDB);
        when (mockDB.getCollection(anyString())).thenReturn(mockCollection);
        MockitoAnnotations.initMocks(this);

        FindIterable iterable = mock(FindIterable.class);
        DBCursor cursor = mock(DBCursor.class);
        String uuid = new AgaveUUID(UUIDType.METADATA).toString();
        DateTime created = new DateTime();
        String strCreated = created.toString();

        BasicDBObject doc = new BasicDBObject("uuid", uuid)
                .append("owner", TEST_USER)
                .append("tenantId", "")
                .append("schemaId", "")
                .append("lastUpdated", created.toDate().toString())
                .append("associationIds", new BasicDBList())
                .append("name", MetadataPermissionManagerIT.class.getName())
                .append("value", new BasicDBObject("testKey", "testValue"))
                .append("created", created.toDate().toString());

        BasicDBList docList = new BasicDBList();
        docList.add(doc);

        BasicDBObject[] arrDoc = {doc};

        when(mockCollection.find(new BasicDBObject("value.testKey", "testValue"))).thenReturn(iterable);
        when(cursor.hasNext()).thenReturn(true).thenReturn(false);
        when(cursor.next()).thenReturn(doc);
        when(cursor.toArray()).thenReturn(Arrays.asList(arrDoc));

        MetadataItem entity = new MetadataItem();
        entity.setUuid(uuid);
        entity.setOwner(TEST_USER);
        entity.setTenantId("");
        entity.setSchemaId("");
        entity.setLastUpdated(created.toDate());
        entity.setAssociations(new MetadataAssociationList());
        entity.setName(MetadataPermissionManagerIT.class.getName());
        entity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        entity.setCreated(created.toDate());

        wrapper= mock(MetadataDao.class);
        when(wrapper.insert(entity)).thenReturn(entity);

        MetadataItem result = wrapper.insert(entity);

        Assert.assertEquals(doc.get("owner"), result.getOwner(), "Expected the same owner");
        Assert.assertEquals(doc.get("created").toString(), result.getCreated().toString(), "Expected the same created date");
        Assert.assertEquals(doc.get("uuid"), uuid, "Expected the same uuid");
        Assert.assertEquals(doc.get("lastUpdated"), result.getLastUpdated().toString(), "Expected same last updated date");
        Assert.assertEquals(doc.get("tenantId"), result.getTenantId(), "Expected the same tenantId");
        Assert.assertEquals(doc.get("schemaId"), result.getSchemaId(), "Expected the same SchemaId");
        Assert.assertEquals(doc.get("name"), result.getName(), "Expected the same metadata name.");
        Assert.assertEquals(doc.get("value.testKey"), result.getValue().get("value.testKey"), "Expected the same value");
        BasicDBList docAssociationId = (BasicDBList) doc.get("associationIds");
        Assert.assertEquals(docAssociationId.size(), result.getAssociations().size(), "Expected associationId count to be 0");

        //add permissions for metadata item
        MetadataPermissionManager pm = new MetadataPermissionManager(uuid, TEST_USER);
        pm.setPermission(TEST_USER, "ALL");
    }


    @Test
    public void testGetResourceData(){
        try {
//            MongoClient mongoClient = ((MetadataApplication) getApplication()).getMongoClient();
//            DB db = mongoClient.getDB(Settings.METADATA_DB_SCHEME);
//            // Gets a collection, if it does not exist creates it
//            DBCollection collection = db.getCollection(Settings.METADATA_DB_COLLECTION);
//            DBCollection schemaCollection = db.getCollection(Settings.METADATA_DB_SCHEMATA_COLLECTION);

//            MongoClient mongo = mock(MongoClient.class);
//            MongoDatabase db = mock(MongoDatabase.class);
//            MongoCollection col = mock(MongoCollection.class);

//            when(mongo.getDatabase("testDB")).thenReturn(db);
//            when(db.getCollection("testCollection")).thenReturn(col);

            //MetadataItem testEntity = createEntity();
            MetadataItem testEntity = new MetadataItem();
            testEntity.setName(MetadataPermissionManagerIT.class.getName());
            testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
            testEntity.setOwner(TEST_USER);

            MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_USER, PermissionType.ALL);
            List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
            listPem.add(metaPem);
            testEntity.setPermissions(listPem);

            //           MetadataDao inst = mock(MetadataDao.class);
            //           when(inst.insert(testEntity)).thenReturn(testEntity);
//            when(inst.getMongoClient()).thenReturn(mongo);
//            when(inst.getDefaultCollection()).thenReturn(col);
//            when(inst.getCollection("testDB", "testCollection")).thenReturn(col);




            DBCursor cursor = null;
            BasicDBObject query = new BasicDBObject("tenantId", TenancyHelper.getCurrentTenantId());

            List<String> sortableFields = Arrays.asList("uuid",
                    "tenantId",
                    "schemaId",
                    "internalUsername",
                    "lastUpdated",
                    "name",
                    "value",
                    "created",
                    "owner");

            String orderField = "lastUpdated";

            int orderDirection = 1;

            MetadataDao inst = wrapper.getInstance();
            MongoCollection collection = inst.getDefaultCollection();
            inst.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ);

            BasicDBList agg = new BasicDBList();
            BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("uuid", testEntity.getUuid()));
            //agg.add(match);

            //cursor = (DBCursor) collection.find(new BasicDBObject("uuid", getEntityUuid(testEntity)));

            agg.add(Aggregates.match(match));

            MongoCursor mongoCursor =  collection.aggregate(agg).cursor();
            List<DBObject> permittedResults = new ArrayList<DBObject>();

            if (mongoCursor!=null) {
                while (mongoCursor.hasNext()) {
                    DBObject result = (DBObject)mongoCursor.next();
                    Assert.assertEquals("testValue", result.get("value.testKey"));
                    Assert.assertEquals(PermissionType.ALL.toString(), result.get("permissions.permissions.0"));
                    permittedResults.add(result);
                }
            }

            Assert.assertEquals(1, permittedResults.size(), "Size should be 1.");


        } catch (MetadataException | MetadataStoreException | UnknownHostException e) {
            Assert.fail("Resolving metadata item UUID should not throw exception.", e);
        }
    }

    @Test
    public void removeTest() throws MetadataException, MetadataStoreException {
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataPermissionManagerIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);

        MetadataPermission metaPem = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER, PermissionType.ALL);
        MetadataPermission metaPem2 = new MetadataPermission(testEntity.getUuid(), TEST_SHARED_USER2, PermissionType.READ);
        List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
        listPem.add(metaPem);
        listPem.add(metaPem2);
        testEntity.setPermissions(listPem);


        //remove user that doesn't have permissions
        try {
            wrapper.deleteUserPermission(testEntity, "newuser");

            verify(wrapper, times(1)).deleteUserPermission(testEntity, "newuser");
        } catch (MetadataStoreException e){
            Assert.fail("Deleting user that doesn't have explicit permissions should not throw exception.");
        }


        //remove user permissions
        wrapper.deleteUserPermission(testEntity, TEST_SHARED_USER);
        verify(wrapper, times(1)).deleteUserPermission(testEntity, TEST_SHARED_USER);

        MetadataItem result = wrapper.find(testEntity);
        Assert.assertEquals(result.getPermissions().size(), 1,
                "Permission size should be 1 after removing a user's permissions");

        //remove all permissions
        wrapper.deleteAllPermissions(testEntity);
        verify(wrapper, times(1)).deleteAllPermissions(testEntity);
        result = wrapper.find(testEntity);
        Assert.assertEquals(result.getPermissions().size(), 0,
                "Permission size should be 0 after removing all permissions.");


    }

    @Test
    public void updateTest() throws MetadataException, MetadataStoreException {
        MetadataItem testEntity = new MetadataItem();
        testEntity.setName(MetadataPermissionManagerIT.class.getName());
        testEntity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        testEntity.setOwner(TEST_USER);

        List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
        testEntity.setPermissions(listPem);

        wrapper.insert(testEntity);

        MetadataItem returnItem = wrapper.find(testEntity);

        Assert.assertEquals(returnItem.getPermissions().size(), 0,
                "Permission list should be 0.");

        wrapper.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ);

        returnItem = wrapper.find(testEntity);
        Assert.assertEquals(returnItem.getPermissions().size(), 1,
                "Permission list should be 1 after adding a new user permission.");

        wrapper.updatePermission(testEntity, TEST_SHARED_USER, PermissionType.READ_WRITE);
        returnItem = wrapper.find(testEntity);
        for (MetadataPermission pem : returnItem.getPermissions()){
            if (pem.getUsername().equals(TEST_SHARED_USER)){
                Assert.assertEquals(pem.getPermission(), PermissionType.READ_WRITE,
                        "Permission type should be READ_WRITE after updating.");
            }
        }
    }

}

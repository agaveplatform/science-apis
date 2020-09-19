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
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Test(groups = {"integration"})
public class MetadataDataProcessingIT {
    String username = "TEST_USER";
    String readUser = "READ_USER";
    String readWriteUser = "READWRITE_USER";
    String noneUser = "NONE_USER";
    MetadataItem addedMetadataItem;
    MongoCollection collection;

    public MetadataItem setupMetadataItem() throws MetadataException {
        ObjectMapper mapper = new ObjectMapper();

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

    @BeforeMethod
    public void setUpDatabase() throws MetadataException {
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

        collection.insertOne(setupMetadataItem());
    }

    @AfterMethod
    public void cleanUpCollection() {
        collection.deleteMany(new Document());
    }


    //find metadata item - with read permission
    @Test
    public void findMetadataItemReadPermissionTest() throws PermissionException, MetadataQueryException, MetadataException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(this.readUser);
        search.setUuid(addedMetadataItem.getUuid());
        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");
        List<MetadataItem> result = search.find(docQuery);

        for (MetadataItem metadataItem : result) {
            Assert.assertEquals(metadataItem, addedMetadataItem);
            MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, metadataItem.getOwner());
            Assert.assertTrue(pemManager.canRead(readUser));
        }
    }

    //find metadata item - with read/write permission
    @Test
    public void findMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException, MetadataException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setUuid(addedMetadataItem.getUuid());
        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");
        List<MetadataItem> result = search.find(docQuery);

        for (MetadataItem metadataItem : result) {
            Assert.assertEquals(metadataItem, addedMetadataItem);
            MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, metadataItem.getOwner());
            Assert.assertTrue(pemManager.canRead(readWriteUser));
            Assert.assertTrue(pemManager.canWrite(readWriteUser));
        }
    }

    //find metadata item - with no permissions
    @Test
    public void findMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException, MetadataException, MetadataStoreException {
        MetadataSearch search = new MetadataSearch(this.noneUser);
        search.setUuid(addedMetadataItem.getUuid());

        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");
        List<MetadataItem> result = search.find(docQuery);
        Assert.assertEquals(result.size(), 0);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.noneUser);
        Assert.assertFalse(pemManager.canRead(noneUser));
    }

    //update metadata item - with read permission
    @Test(expectedExceptions = PermissionException.class)
    public void updateMetadataItemReadPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readUser);
        search.setAccessibleOwnersImplicit();
        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.readUser);
        Assert.assertFalse(pemManager.canWrite(readUser));

        search.updateCurrentMetadataItem();
    }

    //update metadata item - with read/write permission
    @Test
    public void updateMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException, MetadataException, MetadataStoreException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setAccessibleOwnersImplicit();

        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.readWriteUser);
        Assert.assertTrue(pemManager.canWrite(readWriteUser));

        Assert.assertNotNull(search.updateCurrentMetadataItem());

        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"mustard plant\"}");
        List<MetadataItem> originalResult = search.find(docQuery);
        Assert.assertEquals(originalResult.size(), 0);

        Document updatedQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"New Name\"}");
        List<MetadataItem> updatedResult = search.find(updatedQuery);
        Assert.assertEquals(updatedResult.size(), 1);


//        List<MetadataItem> originalResult = search.find("{\"name\":\"mustard plant\"}");
//        Assert.assertEquals(originalResult.size(), 0);
//        List<MetadataItem> updatedResult = search.find("{\"name\":\"New Name\"}");
//        Assert.assertEquals(updatedResult.size(), 1);
    }

    //update metadata item - with no permissions
    @Test(expectedExceptions = PermissionException.class)
    public void updateMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.noneUser);
        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.noneUser);
        Assert.assertFalse(pemManager.canWrite(noneUser));

        search.updateCurrentMetadataItem();
    }

    //delete metadata item - with no permissions
    @Test(expectedExceptions = PermissionException.class)
    public void DeleteMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.noneUser);
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.username);
        Assert.assertFalse(pemManager.canWrite(noneUser));

        search.deleteCurrentMetadataItem();

    }

    //delete metadata item - with read permission
    @Test
    public void DeleteMetadataItemReadPermissionTest() throws MetadataException, PermissionException {
//        //verify that metadata item exists before trying to delete
//        MetadataSearch ownerSearch = new MetadataSearch(this.username);
//        MetadataItem metadataItem = ownerSearch.findOne(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());
//        Assert.assertNotNull(metadataItem);
//
//        //verify that user cannot write
//        MetadataPermissionManager pemManager = new MetadataPermissionManager(metadataItem, metadataItem.getOwner());
//        Assert.assertFalse(pemManager.canWrite(readUser));
//
//        //try to delete -- should fail
//        MetadataSearch search = new MetadataSearch(this.readUser);
//        search.setMetadataItem(metadataItem);
//        Assert.assertThrows(PermissionException.class, ()->search.deleteCurrentMetadataItem());
//
//        //verify metadataitem was not updated
//        search.setMetadataItem(metadataItem);
//        MetadataItem afterDelete = search.findByUuidAndTenant(addedMetadataItem.getUuid(), addedMetadataItem.getTenantId());
//        Assert.assertNotNull(afterDelete);
    }

    //delete metadata item - with read/write permission
    @Test
    public void DeleteMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException, MetadataException, MetadataStoreException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataPermissionManager pemManager = new MetadataPermissionManager(addedMetadataItem, this.username);
        Assert.assertTrue(pemManager.canWrite(readWriteUser));

        Assert.assertNotNull(search.deleteCurrentMetadataItem());

        JsonHandler jsonHandler = new JsonHandler();
        Document docQuery = jsonHandler.parseUserQueryToDocument("{\"name\":\"New Name\"}");
        List<MetadataItem> updatedResult = search.find(docQuery);

//        List<MetadataItem> updatedResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(updatedResult.size(), 0);
    }

}

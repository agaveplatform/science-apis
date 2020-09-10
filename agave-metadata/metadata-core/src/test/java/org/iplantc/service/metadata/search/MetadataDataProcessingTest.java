package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.managers.MetadataItemPermissionManager;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataItemCodec;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Test(groups = {"integration"})

public class MetadataDataProcessingTest {
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

        MetadataPermission readPermission = new MetadataPermission(toAdd.getUuid(), readUser, PermissionType.READ);
        MetadataPermission readWritePermission = new MetadataPermission(toAdd.getUuid(), readWriteUser, PermissionType.READ_WRITE);
        toAdd.setPermissions(new ArrayList<>(Arrays.asList(readPermission, readWritePermission)));
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
    public void findMetadataItemReadPermissionTest() throws PermissionException, MetadataQueryException {
        MetadataSearch search = new MetadataSearch(this.readUser);
        search.setUuid(addedMetadataItem.getUuid());
        List<MetadataItem> result = search.find("{\"name\":\"mustard plant\"}");

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readUser, addedMetadataItem.getUuid());
        Assert.assertTrue(pemManager.canRead());
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), addedMetadataItem);
    }

    //find metadata item - with read/write permission
    @Test
    public void findMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException {
        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setUuid(addedMetadataItem.getUuid());
        List<MetadataItem> result = search.find("{\"name\":\"mustard plant\"}");

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readWriteUser, addedMetadataItem.getUuid());
        Assert.assertTrue(pemManager.canRead());
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), addedMetadataItem);
    }

    //find metadata item - with no permissions
    @Test
    public void findMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException {
        MetadataSearch search = new MetadataSearch(this.noneUser);
        search.setUuid(addedMetadataItem.getUuid());

        List<MetadataItem> result = search.find("{\"name\":\"mustard plant\"}");

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.noneUser, addedMetadataItem.getUuid());
        Assert.assertFalse(pemManager.canRead());
        Assert.assertEquals(result.size(), 0);
    }

    //update metadata item - with read permission
    @Test
    public void updateMetadataItemReadPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readUser);
        search.setAccessibleOwnersImplicit();
        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readUser, addedMetadataItem.getUuid());
        Assert.assertFalse(pemManager.canWrite());

        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());

        List<MetadataItem> originalResult = search.find("{\"name\":\"mustard plant\"}");
        Assert.assertEquals(originalResult.size(), 1);
        List<MetadataItem> updatedResult = search.find("{\"name\":\"NewName\"}");
        Assert.assertEquals(updatedResult.size(), 0);
    }

    //update metadata item - with read/write permission
    @Test
    public void updateMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setAccessibleOwnersImplicit();

        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readWriteUser, addedMetadataItem.getUuid());
        Assert.assertTrue(pemManager.canWrite());

        Assert.assertNotNull(search.updateMetadataItem());

        List<MetadataItem> originalResult = search.find("{\"name\":\"mustard plant\"}");
        Assert.assertEquals(originalResult.size(), 0);
        List<MetadataItem> updatedResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(updatedResult.size(), 1);
    }

    //update metadata item - with no permissions
    @Test
    public void updateMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.noneUser);
        metadataItem.setName("New Name");
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.noneUser, addedMetadataItem.getUuid());
        Assert.assertFalse(pemManager.canWrite());

        Assert.assertThrows(PermissionException.class, () -> search.updateMetadataItem());

        List<MetadataItem> originalResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(originalResult.size(), 0);
    }

    //delete metadata item - with no permissions
    @Test
    public void DeleteMetadataItemNoPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.noneUser);
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.noneUser, addedMetadataItem.getUuid());
        Assert.assertFalse(pemManager.canWrite());

        Assert.assertThrows(PermissionException.class, () -> search.deleteMetadataItem());

        List<MetadataItem> originalResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(originalResult.size(), 0);
    }

    //delete metadata item - with read permission
    @Test
    public void DeleteMetadataItemReadPermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readUser);
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readUser, addedMetadataItem.getUuid());
        Assert.assertFalse(pemManager.canWrite());

        Assert.assertThrows(PermissionException.class, () -> search.deleteMetadataItem());

        List<MetadataItem> originalResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(originalResult.size(), 0);
    }

    //delete metadata item - with read/write permission
    @Test
    public void DeleteMetadataItemReadWritePermissionTest() throws PermissionException, MetadataQueryException, MetadataException {
        MetadataSearch ownerSearch = new MetadataSearch(this.username);
        ownerSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem metadataItem = ownerSearch.findOne();
        Assert.assertNotNull(metadataItem);

        MetadataSearch search = new MetadataSearch(this.readWriteUser);
        search.setMetadataItem(metadataItem);
        search.setAccessibleOwnersImplicit();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(this.readWriteUser, addedMetadataItem.getUuid());
        Assert.assertTrue(pemManager.canWrite());

        Assert.assertNotNull(search.deleteMetadataItem());

        List<MetadataItem> updatedResult = search.find("{\"name\":\"New Name\"}");
        Assert.assertEquals(updatedResult.size(), 0);
    }

}

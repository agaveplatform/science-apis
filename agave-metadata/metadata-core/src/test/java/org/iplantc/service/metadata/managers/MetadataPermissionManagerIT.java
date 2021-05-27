package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

@Test(groups = {"integration"})
public class MetadataPermissionManagerIT implements AbstractUUIDTest<MetadataItem> {
    private final String TEST_USER = "TEST_USER";
    private final String TEST_SHARED_USER = "TEST_SHARED_USER";
    private final String TEST_WRITE_USER = "TEST_WRITE_USER";

    private MetadataItem addedItem;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return the UUIDType of the entity under testing
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.METADATA;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return a persisted instance of the entity
     */
    @Override
    public MetadataItem createEntity() {
        MetadataItem toAdd = null;
        try {
            toAdd = new MetadataItem();
            toAdd.setName(MetadataPermissionManagerIT.class.getName());
            toAdd.setValue(mapper.createObjectNode().put("testKey", "testValue"));
            toAdd.setOwner(TEST_USER);
            toAdd.setInternalUsername(TEST_USER);
            MetadataPermission readPermission = new MetadataPermission(TEST_SHARED_USER, PermissionType.READ);
            toAdd.getPermissions().addAll(List.of(readPermission));


//            addedItem = MetadataDao.getInstance().insert(toAdd);

            MetadataSearch search = new MetadataSearch(TEST_USER);
//            search.setMetadataItem(toAdd);
            addedItem = search.insertMetadataItem(toAdd);

//            MetadataDao inst = mock(MetadataDao.class);
//            when(inst.insert(toAdd)).thenReturn(toAdd);

            //MetadataDao.getInstance().insert(entity);
        } catch (Exception e) {
            Assert.fail("Unable to create metadata item", e);
        }

        return addedItem;
    }

    /**
     * Serialize the entity into a JSON string. This should be handled by the standard
     * {@link ObjectMapper} via Jackson Annotations.
     *
     * @param testEntity entity to serialize
     * @return serialized json representation of the entity
     */
    @Override
    public String serializeEntityToJSON(MetadataItem testEntity) {
        String json = null;

        json = testEntity.toObjectNode().toString();
//
//        try {
////            json = mapper.writeValueAsString(testEntity);
//            json = testEntity.toObjectNode().toString();
//        } catch (JsonProcessingException e) {
//            Assert.fail("Failed to serialize metadata item", e);
//        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link MetadataItem#getUuid()}
     * method in the entity.
     *
     * @param testEntity the test entity
     * @return the uuid for the entity
     */
    public String getEntityUuid(MetadataItem testEntity) {
        return testEntity.getUuid();
    }

    @AfterMethod
    public void cleanUpCollection() {
        MetadataDao.getInstance().getDefaultMetadataItemCollection().deleteMany(new Document());
    }


    @Test
    public void testGetResourceUrl() {
        try {
            MetadataItem testEntity = createEntity();
            String resolvedUrl = UUIDEntityLookup
                    .getResourceUrl(getEntityType(), getEntityUuid(testEntity));

            Assert.assertEquals(
                    TenancyHelper.resolveURLToCurrentTenant(resolvedUrl),
                    getUrlFromEntityJson(serializeEntityToJSON(testEntity)),
                    "Resolved "
                            + getEntityType().name().toLowerCase()
                            + " urls should match those created by the entity class itself.");
        } catch (UUIDException | IOException e) {
            Assert.fail("Resolving metadata item UUID should not throw exception.", e);
        }
    }


    @Test
    public void testGetPermissions() throws MetadataException {
        MetadataItem testEntity = createEntity();
        Assert.assertNotNull(testEntity, "Metadata item should not be after added to collection.");
        MetadataPermissionManager pmTest = new MetadataPermissionManager(testEntity, TEST_USER);
        MetadataPermission ownerPem = pmTest.getPermission(TEST_USER);
        MetadataPermission sharedUserPem = pmTest.getPermission(TEST_SHARED_USER);

        Assert.assertEquals(ownerPem, new MetadataPermission(TEST_USER, PermissionType.ALL));
        Assert.assertEquals(sharedUserPem, new MetadataPermission(TEST_SHARED_USER, PermissionType.READ));
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Owner should have all permissions. ");
        Assert.assertTrue(pmTest.canRead(TEST_SHARED_USER) && !pmTest.canWrite(TEST_SHARED_USER),
                "Shared user should have same permissions as in the added metadata item.");

    }

    @Test
    public void testClearPermissions() throws MetadataException, MetadataStoreException {
        MetadataItem testEntity = createEntity();
        Assert.assertNotNull(testEntity, "Metadata item should not be after added to collection.");

        MetadataPermissionManager pmTest = new MetadataPermissionManager(testEntity, TEST_USER);
        pmTest.clearPermissions();

        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Clearing permissions should save ownership. ");
        Assert.assertFalse(pmTest.canRead(TEST_SHARED_USER) && pmTest.canWrite(TEST_SHARED_USER), "Clearing permissions should remove permissions for user.");
    }

    @Test
    public void testSetPermission() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem testEntity = createEntity();
        Assert.assertNotNull(testEntity, "Metadata item should not be after added to collection.");
        MetadataPermissionManager pmTest = new MetadataPermissionManager(testEntity, TEST_USER);

        //add permissions
        pmTest.setPermission(TEST_WRITE_USER, PermissionType.READ_WRITE.toString());
        MetadataPermission pem = pmTest.getPermission(TEST_WRITE_USER);
        Assert.assertEquals(pem.getPermission(), PermissionType.READ_WRITE,
                "Permission for TEST_WRITE_USER should be READ_WRITE after update.");

        //update permissions
        pmTest.setPermission(TEST_WRITE_USER, PermissionType.READ.toString());
        pem = pmTest.getPermission(TEST_WRITE_USER);
        Assert.assertEquals(pem.getPermission(), PermissionType.READ,
                "Permission for TEST_WRITE_USER should be READ after update.");

        //delete permissions
        pmTest.setPermission(TEST_WRITE_USER, PermissionType.NONE.toString());
        pem = pmTest.getPermission(TEST_WRITE_USER);
        Assert.assertEquals(pem.getPermission(), PermissionType.NONE,
                "Permission for TEST_WRITE_USER should be removed after update.");

    }

    /**
     * Extracts the HAL from a json representation of an object and returns the
     * _links.self.href value.
     *
     * @param entityJson the serialized json from which to extract the self link
     * @return the value of the HAL self link for the json object
     * @throws JsonProcessingException if the json is invalid
     * @throws IOException             if the entity cannot be read
     */
    private String getUrlFromEntityJson(String entityJson)
            throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(entityJson).get("_links").get("self").get("href").asText();
    }
}

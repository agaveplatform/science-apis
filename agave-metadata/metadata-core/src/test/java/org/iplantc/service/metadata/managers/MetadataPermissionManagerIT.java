package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@Test(groups={"integration"})
public class MetadataPermissionManagerIT implements AbstractUUIDTest<MetadataItem> {
    private final String TEST_USER = "testuser";
    private final String TEST_SHARED_USER = "testshareuser";

    private ObjectMapper mapper = new ObjectMapper();

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
        MetadataItem entity = null;
        try {
            entity = new MetadataItem();
            entity.setName(MetadataPermissionManagerIT.class.getName());
            entity.setValue(mapper.createObjectNode().put("testKey", "testValue"));
            entity.setOwner(TEST_USER);

            MetadataPermission metaPem = new MetadataPermission(TEST_USER, PermissionType.ALL);
            List<MetadataPermission> listPem = new ArrayList<MetadataPermission>();
            listPem.add(metaPem);
            entity.setPermissions(listPem);

            MetadataDao inst = mock(MetadataDao.class);

            when(inst.insert(entity)).thenReturn(entity);

            //MetadataDao.getInstance().insert(entity);
        } catch (Exception e) {
            Assert.fail("Unable to create metadata item", e);
        }

        return entity;
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
    public void testSetClearPermissions() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem testEntity = createEntity();

        MetadataPermissionManager pmTest = new MetadataPermissionManager(testEntity, TEST_USER);
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Owner has implicit permissions.");
        Assert.assertFalse(pmTest.canRead(TEST_SHARED_USER), "User doesn't have permissions yet.");
        Assert.assertFalse(pmTest.canWrite(TEST_SHARED_USER), "User doesn't have any permissions yet.");

        pmTest.setPermission(TEST_SHARED_USER, PermissionType.READ.toString());
         pmTest = new MetadataPermissionManager(testEntity, TEST_USER);
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Owner has implicit permissions.");

        Assert.assertTrue(pmTest.canRead(TEST_SHARED_USER), "User should have updated permission to READ.");
        Assert.assertFalse(pmTest.canWrite(TEST_SHARED_USER), "User should not have permission to WRITE.");

        pmTest.setPermission(TEST_SHARED_USER, PermissionType.NONE.toString());
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Owner has implicit permissions.");

        Assert.assertFalse(pmTest.canRead(TEST_SHARED_USER), "User should no longer have permission to READ.");
        Assert.assertFalse(pmTest.canWrite(TEST_SHARED_USER), "User should no longer have permission to WRITE.");

        pmTest.setPermission(TEST_SHARED_USER, PermissionType.READ_WRITE.toString());
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Owner has implicit permissions.");
        Assert.assertTrue(pmTest.canRead(TEST_SHARED_USER) && pmTest.canWrite(TEST_SHARED_USER), "User should have updated permissions to READ_WRITE.");

        pmTest.clearPermissions();

        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Clearing permissions should save ownership. ");
        Assert.assertFalse(pmTest.canRead(TEST_SHARED_USER) && pmTest.canWrite(TEST_SHARED_USER), "Clearing permissions should remove permissions for user.");

        pmTest.setPermission(TEST_SHARED_USER, "");
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Clearing permissions should save ownership. ");
        Assert.assertFalse(pmTest.canRead(TEST_SHARED_USER) && pmTest.canWrite(TEST_SHARED_USER), "Clearing permissions should remove permissions for user.");

        pmTest.setPermission(TEST_USER, PermissionType.NONE.toString());
        Assert.assertTrue(pmTest.canRead(TEST_USER) && pmTest.canWrite(TEST_USER), "Permissions will not be changed for the wner. ");


        try {
            pmTest.setPermission("", PermissionType.READ_WRITE.toString());
            Assert.fail("Empty username should throw exception.");
        } catch (MetadataException e ){
            //pass
        }
    }

    @Test
    public void testRemovePermission() throws MetadataException, PermissionException, MetadataStoreException {
            MetadataItem testEntity = createEntity();
            MetadataPermissionManager pmTest = new MetadataPermissionManager(testEntity, TEST_USER);

            pmTest.setPermission(TEST_SHARED_USER, "");

            MetadataPermission pem = pmTest.getPermission(TEST_SHARED_USER);
            Assert.assertEquals(pem.getPermission(), PermissionType.NONE, "Removing a permission that doesn't exist.");

            pmTest.setPermission(TEST_SHARED_USER, PermissionType.READ_WRITE.toString());

            pem = pmTest.getPermission(TEST_SHARED_USER);
            Assert.assertEquals(pem.getPermission(), PermissionType.NONE, "There should be no permissions.");
    }

    /**
     * Extracts the HAL from a json representation of an object and returns the
     * _links.self.href value.
     *
     * @param entityJson the serialized json from which to extract the self link
     * @return the value of the HAL self link for the json object
     * @throws JsonProcessingException if the json is invalid
     * @throws IOException if the entity cannot be read
     */
    private String getUrlFromEntityJson(String entityJson)
            throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(entityJson).get("_links").get("self").get("href").asText();
    }
}

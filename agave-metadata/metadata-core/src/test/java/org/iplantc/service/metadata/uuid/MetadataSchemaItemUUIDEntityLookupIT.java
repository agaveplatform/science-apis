package org.iplantc.service.metadata.uuid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups={"integration"})
public class MetadataSchemaItemUUIDEntityLookupIT implements AbstractUUIDTest<MetadataSchemaItem> {
    private final String TEST_USER = "testuser";
    private final String TEST_SHARED_USER = "testshareuser";

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return the UUIDType of the entity under testing
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.SCHEMA;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return a persisted instance of the entity
     */
    @Override
    public MetadataSchemaItem createEntity() {
        MetadataSchemaItem entity = null;
        try {
            entity = new MetadataSchemaItem();
            entity.setSchema(mapper.createObjectNode());
            entity.setOwner(TEST_USER);
            MetadataSchemaDao.getInstance().insert(entity);
        } catch (Exception e) {
            Assert.fail("Unable to create metadata schema event", e);
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
    public String serializeEntityToJSON(MetadataSchemaItem testEntity) {
        String json = null;

        json = testEntity.toObjectNode().toString();

//        try {
//            json = mapper.writeValueAsString(testEntity);
//        } catch (JsonProcessingException e) {
//            Assert.fail("Failed to serialize metadata schema", e);
//        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link MetadataSchemaItem#getUuid()}
     * method in the entity.
     *
     * @param testEntity the test entity
     * @return the uuid for the entity
     */
    public String getEntityUuid(MetadataSchemaItem testEntity) {
        return testEntity.getUuid();
    }

    @Test
    public void testGetResourceUrl() {
        try {
            MetadataSchemaItem testEntity = createEntity();
            String resolvedUrl = UUIDEntityLookup
                    .getResourceUrl(getEntityType(), getEntityUuid(testEntity));

            Assert.assertEquals(
                    TenancyHelper.resolveURLToCurrentTenant(resolvedUrl),
                    getUrlFromEntityJson(serializeEntityToJSON(testEntity)),
                    "Resolved "
                            + getEntityType().name().toLowerCase()
                            + " urls should match those created by the entity class itself.");
        } catch (UUIDException | IOException e) {
            Assert.fail("Resolving metadata schema UUID should not throw exception.", e);
        }
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
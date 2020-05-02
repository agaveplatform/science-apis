package org.iplantc.service.apps.uuid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.apps.dao.AbstractDaoTest;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups={"integration"})
public class SoftwareUUIDEntityLookupTest extends AbstractDaoTest implements AbstractUUIDTest<Software> {

    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return the UUIDType of the entity under testing
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.APP;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return a persisted instance of the entity
     */
    @Override
    public Software createEntity() {
        Software software = null;
        try {
            software = createSoftware();
        } catch (Exception e) {
            Assert.fail("Unable to create software event", e);
        }

        return software;
    }

    /**
     * Serialize the entity into a JSON string. This should call out
     * to a {@link Software#toJSON()} method in the entity or delegate to
     * Jackson Annotations.
     *
     * @param testEntity entity to serialize
     * @return serialized json representation of the entity
     */
    @Override
    public String serializeEntityToJSON(Software testEntity) {
        String json = null;
        try {
            json = testEntity.toJSON();
        } catch (JSONException e) {
            Assert.fail("Failed to serialize event", e);
        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link Software#getUuid()}
     * method in the entity.
     *
     * @param testEntity the test entity
     * @return the uuid for the entity
     */
    public String getEntityUuid(Software testEntity) {
        return testEntity.getUuid();
    }

    @Test
    public void testGetResourceUrl() {
        try {
            Software testEntity = createEntity();
            String resolvedUrl = UUIDEntityLookup
                    .getResourceUrl(getEntityType(), getEntityUuid(testEntity));

            Assert.assertEquals(
                    TenancyHelper.resolveURLToCurrentTenant(resolvedUrl),
                    getUrlFromEntityJson(serializeEntityToJSON(testEntity)),
                    "Resolved "
                            + getEntityType().name().toLowerCase()
                            + " urls should match those created by the entity class itself.");
        } catch (UUIDException | IOException e) {
            Assert.fail("Resolving software event UUID should not throw exception.", e);
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

        return mapper.readTree(entityJson).get("_links").get("self")
                .get("href").asText();
    }
}
package org.iplantc.service.apps.uuid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.Session;
import org.iplantc.service.apps.dao.AbstractDaoTest;
import org.iplantc.service.apps.dao.SoftwareEventDao;
import org.iplantc.service.apps.model.JSONTestDataUtil;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareEvent;
import org.iplantc.service.apps.model.enumerations.SoftwareEventType;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE;

@Test(groups={"integration"})
public class SoftwareEventUUIDEntityLookupTest extends AbstractDaoTest implements AbstractUUIDTest<SoftwareEvent> {

    private Software software = null;
    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.APP_EVENT;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return
     */
    @Override
    public SoftwareEvent createEntity() {
        SoftwareEvent testEvent = null;
        try {
            software = createSoftware();

            testEvent = new SoftwareEvent(software, SoftwareEventType.CREATED, SoftwareEventType.CREATED.getDescription(), SYSTEM_OWNER);
            new SoftwareEventDao().persist(testEvent);
        } catch (Exception e) {
            Assert.fail("Unable to create software event", e);
        }

        return testEvent;
    }

    /**
     * Serialize the entity into a JSON string. This should call out
     * to a {@link SoftwareEvent#toJSON(Software)} method in the entity or delegate to
     * Jackson Annotations.
     *
     * @param testEntity entity to serialize
     * @return
     */
    @Override
    public String serializeEntityToJSON(SoftwareEvent testEntity) {
        String json = null;

        json = testEntity.toJSON(software);
//
//        try {
//
//        } catch (JsonProcessingException e) {
//            Assert.fail("Failed to serialize event", e);
//        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link SoftwareEvent#getUuid()}
     * method in the entity.
     *
     * @param testEntity
     * @return
     */
    public String getEntityUuid(SoftwareEvent testEntity) {
        return testEntity.getUuid();
    }

    @Test
    public void testGetResourceUrl() {
        try {
            SoftwareEvent testEntity = createEntity();
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
    protected String getUrlFromEntityJson(String entityJson)
            throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(entityJson).get("_links").get("self")
                .get("href").asText();
    }
}

package org.iplantc.service.notification.uuid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.notification.AbstractNotificationTest;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups={"integration"})
public class NotificationUUIDEntityLookupTest implements AbstractUUIDTest<Notification> {
    UUIDEntityLookup uuidEntityLookup = new UUIDEntityLookup();
    AbstractNotificationTest it;

    @BeforeClass
    protected void beforeClass() throws Exception {
        it = new AbstractNotificationTest();
    }

    @AfterClass
    public void afterClass() throws NotificationException {
        it.clearNotifications();
    }

    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.NOTIFICATION;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return
     */
    @Override
    public Notification createEntity() {
        Notification testMonitor = null;
        try {
            testMonitor = it.createEmailNotification();
            new NotificationDao().persist(testMonitor);
        } catch (Exception e) {
            Assert.fail("Unable to create sotrage Notification", e);
        }

        return testMonitor;
    }

    /**
     * Serialize the entity into a JSON string. This should call out
     * to a {@link Notification#toJSON()} method in the entity or delegate to
     * Jackson Annotations.
     *
     * @param testEntity entity to serialize
     * @return
     */
    @Override
    public String serializeEntityToJSON(Notification testEntity) {
        String json = null;
        try {
            json = testEntity.toJSON();
        } catch (NotificationException e) {
            Assert.fail("Failed to serialized notification", e);
        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link Notification#getUuid()}
     * method in the entity.
     *
     * @param testEntity
     * @return
     */
    @Override
    public String getEntityUuid(Notification testEntity) {
        return testEntity.getUuid();
    }

    @Test
    public void testGetResourceUrl() {
        try {
            Notification testEntity = createEntity();
            String resolvedUrl = UUIDEntityLookup
                    .getResourceUrl(getEntityType(), getEntityUuid(testEntity));

            Assert.assertEquals(
                    TenancyHelper.resolveURLToCurrentTenant(resolvedUrl),
                    getUrlFromEntityJson(serializeEntityToJSON(testEntity)),
                    "Resolved "
                            + getEntityType().name().toLowerCase()
                            + " urls should match those created by the entity class itself.");
        } catch (UUIDException | IOException e) {
            Assert.fail("Resolving logical file path from UUID should not throw exception.", e);
        }
    }

    /**
     * Extracts the HAL from a json representation of an object and returns the
     * _links.self.href value.
     *
     * @param entityJson
     * @return
     * @throws JsonProcessingException
     * @throws IOException
     */
    protected String getUrlFromEntityJson(String entityJson)
            throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(entityJson).get("_links").get("self")
                .get("href").asText();
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme
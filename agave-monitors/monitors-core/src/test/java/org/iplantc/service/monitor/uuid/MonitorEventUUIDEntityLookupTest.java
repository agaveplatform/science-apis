package org.iplantc.service.monitor.uuid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AbstractUUIDTest;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.events.DomainEntityEvent;
import org.iplantc.service.monitor.events.DomainEntityEventDao;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.enumeration.MonitorEventType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups={"integration"})
public class MonitorEventUUIDEntityLookupTest implements AbstractUUIDTest<DomainEntityEvent> {
    UUIDEntityLookup uuidEntityLookup = new UUIDEntityLookup();
    AbstractMonitorIT it;

    @BeforeClass
    protected void beforeClass() throws Exception {
        it = new AbstractMonitorIT();
        it.beforeClass();
    }

    @AfterClass
    public void afterClass() throws MonitorException {
        it.afterClass();
    }

    /**
     * Get the type of the entity. This is the type of UUID to test.
     *
     * @return
     */
    @Override
    public UUIDType getEntityType() {
        return UUIDType.MONITOR_EVENT;
    }

    /**
     * Create a test entity persisted and available for lookup.
     *
     * @return
     */
    @Override
    public DomainEntityEvent createEntity() {
        DomainEntityEvent testMonitorEvent = null;
        try {
            Monitor testMonitor = it.createStorageMonitor();
            new MonitorDao().persist(testMonitor);

            testMonitorEvent = new DomainEntityEvent();
            testMonitorEvent.setEntity(testMonitor.getUuid());
            testMonitorEvent.setStatus(MonitorEventType.CREATED.name());
            testMonitorEvent.setDescription(MonitorEventType.CREATED.getDescription());
            testMonitorEvent.setCreatedBy(AbstractMonitorIT.TEST_USER);
            new DomainEntityEventDao().persist(testMonitorEvent);
        } catch (Exception e) {
            Assert.fail("Unable to create monitor event", e);
        }

        return testMonitorEvent;
    }

    /**
     * Serialize the entity into a JSON string. This should call out
     * to a {@link Monitor#toJSON()} method in the entity or delegate to
     * Jackson Annotations.
     *
     * @param testEntity entity to serialize
     * @return
     */
    @Override
    public String serializeEntityToJSON(DomainEntityEvent testEntity) {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(testEntity);
        } catch (JsonProcessingException e) {
            Assert.fail("Failed to serialize event", e);
        }

        return json;
    }

    /**
     * Get the uuid of the entity. This should call out to the {@link Monitor#getUuid()}
     * method in the entity.
     *
     * @param testEntity
     * @return
     */
    public String getEntityUuid(DomainEntityEvent testEntity) {
        return testEntity.getUuid();
    }

    @Test
    public void testGetResourceUrl() {
        try {
            DomainEntityEvent testEntity = createEntity();
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
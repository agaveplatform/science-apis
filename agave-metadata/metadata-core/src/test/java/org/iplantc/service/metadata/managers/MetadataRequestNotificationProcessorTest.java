package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class MetadataRequestNotificationProcessorTest {
    ObjectMapper mapper = new ObjectMapper();
    private final String TEST_USER = "TEST_USER";


    protected ArrayNode createTestNotification() {
        ArrayNode node = mapper.createArrayNode();

        node.addObject()// optional array in request
                .put("url", "foo@example.com")
                .put("event", "CREATED")
                .put("persistent", false);

        return node;
    }

    @Test
    public void processTest() throws NotificationException {
        ArrayNode node = createTestNotification();

        NotificationDao mockNotificationDao = mock(NotificationDao.class);

        String uuid = new AgaveUUID(UUIDType.METADATA).toString();
        MetadataRequestNotificationProcessor notificationProcessor = new MetadataRequestNotificationProcessor(TEST_USER, uuid) {
            @Override
            public NotificationDao getDao() {
                return mockNotificationDao;
            }
        };

        notificationProcessor.process(node);
        verify(mockNotificationDao, times(node.size())).persist(any());

        List<Notification> notificationList = notificationProcessor.getNotifications();
        Assert.assertEquals(notificationList.size(), node.size(), "Notification size should be the same as the original json array.");
        for (Notification notification : notificationList) {
            boolean found = false;
            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                JsonNode notifNode = it.next();
                if (notification.getCallbackUrl().equals(notifNode.get("url").textValue())) {
                    assertEquals(notification.getEvent(), notifNode.get("event").textValue(), "Notification event should be the same as the original json array.");
                    assertEquals(notification.isPersistent(), notifNode.get("persistent").asBoolean(), "Notification persistence should be the same as the original json array.");
                    assertEquals(notification.getAssociatedUuid(), uuid,
                            "Associated uuid for notification should match the uuid passed to the processor.");
                    assertEquals(notification.getOwner(), TEST_USER,
                            "Owner of notification should match the owner passed to the processor.");
                    found = true;
                    break;
                }
            }

            if (!found) {
                fail("All notifications from the original json should be present in the parsed metadata item");
            }
        }
    }

}

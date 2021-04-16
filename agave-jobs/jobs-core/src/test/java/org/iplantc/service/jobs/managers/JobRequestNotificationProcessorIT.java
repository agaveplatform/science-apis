package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobEventType;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.jobs.model.enumerations.JobEventType.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test(groups = {"integration"})
public class JobRequestNotificationProcessorIT {

    private final ObjectMapper mapper = new ObjectMapper();


    /**
     * Mocks out the {@link JobRequestNotificationProcessor} class for testing. A live list of notifications is
     * available via {@link JobRequestNotificationProcessor#getNotifications()};
     *
     * @return mocked instance of JobRequestNotificationProcessor
     */
    private JobRequestNotificationProcessor getMockJobRequetNotificationProcessor() {
        List<Notification> processedNotifications = new ArrayList<>();
        Job job = mock(Job.class);
        when(job.getUuid()).thenReturn(new AgaveUUID(UUIDType.JOB).toString());
        JobRequestNotificationProcessor jobRequestNotificationProcessor = mock(JobRequestNotificationProcessor.class);
        when(jobRequestNotificationProcessor.getJob()).thenReturn(job);
        when(jobRequestNotificationProcessor.getNotifications()).thenReturn(processedNotifications);
        when(jobRequestNotificationProcessor.getUsername()).thenReturn(TEST_OWNER);

        try {
            doCallRealMethod().when(jobRequestNotificationProcessor).addNotification(any(ObjectNode.class));
            doNothing().when(jobRequestNotificationProcessor).process(anyString());
            doNothing().when(jobRequestNotificationProcessor).process(any(JsonNode.class));
            doNothing().when(jobRequestNotificationProcessor).process(any(ArrayNode.class));
        } catch (NotificationException e) {
            fail("Mocking JobRequestNotificationProcessor#process() method should not throw exception", e);
        }

        return jobRequestNotificationProcessor;
    }

    /**
     * Sets an field in a ObjectNode object, determining the proper type on the fly.
     *
     * @param json  the object posted to the job reqeust
     * @param field the field to add to the object
     * @param value the value of the field
     * @return the updated ObjectNode
     */
    private ObjectNode updateObjectNode(ObjectNode json, String field, Object value) {
        if (value == null)
            json.putNull(field);
        else if (value instanceof ArrayNode)
            json.putArray(field).addAll((ArrayNode) value);
        else if (value instanceof ObjectNode)
            json.putObject(field);
        else if (value instanceof Long)
            json.put(field, (Long) value);
        else if (value instanceof Integer)
            json.put(field, (Integer) value);
        else if (value instanceof Float)
            json.put(field, (Float) value);
        else if (value instanceof Double)
            json.put(field, (Double) value);
        else if (value instanceof BigDecimal)
            json.put(field, (BigDecimal) value);
        else if (value instanceof Boolean)
            json.put(field, (Boolean) value);
        else if (value instanceof Collection) {
            ArrayNode arrayNode = new ObjectMapper().createArrayNode();
            for (Object o : (Collection) value) {
                if (o instanceof ArrayNode)
                    arrayNode.addArray().addAll((ArrayNode) o);
                else if (o instanceof ObjectNode) {
                    assert value instanceof ObjectNode;
                    arrayNode.add((ObjectNode) value);
                } else if (o instanceof Long) {
                    assert value instanceof Long;
                    arrayNode.add((Long) value);
                } else if (o instanceof Integer) {
                    assert value instanceof Long;
                    arrayNode.add((Long) value);
                } else if (o instanceof Float) {
                    assert value instanceof Long;
                    arrayNode.add((Long) value);
                } else if (o instanceof Double) {
                    assert value instanceof Long;
                    arrayNode.add((Long) value);
                } else if (o instanceof Boolean) {
                    assert value instanceof Boolean;
                    arrayNode.add((Boolean) value);
                } else if (o instanceof String) {
                    assert value instanceof String;
                    arrayNode.add((String) value);
                } else
                    arrayNode.addObject();
            }
            json.putArray(field).addAll(arrayNode);
        } else if (value instanceof Map) {
            for (String key : ((Map<String, Object>) value).keySet()) {
                json = updateObjectNode(json, key, ((Map<String, Object>) value).get(key));
            }
        } else if (value instanceof String)
            json.put(field, (String) value);
        else
            json.putObject(field);

        return json;
    }

    /**
     * Creates a JsonNode representation of a job notification that expires after first delivery
     *
     * @param url   the url target of the notification
     * @param event the name of the event to which to subscribe
     * @return a json representation of the notification
     */
    private JsonNode createJsonNotification(Object url, Object event) {
        return createJsonNotification(url, event, false);
    }

    /**
     * Creates a JsonNode representation of a job notification using the supplied values
     * and determining the types on the fly.
     *
     * @param url        the url target of the notification
     * @param event      the name of the event to which to subscribe
     * @param persistent true if the notification should persist after firing once.
     * @return a json representation of the notification
     */
    private JsonNode createJsonNotification(Object url, Object event, boolean persistent) {
        ObjectNode json = mapper.createObjectNode()
                .put("persistent", persistent);
        json = updateObjectNode(json, "url", url);
        json = updateObjectNode(json, "event", event);

        return json;
    }

    @DataProvider
    protected Object[][] testProcessStringCallbackUrlProvider() {
        List<Object[]> testCases = new ArrayList<>();

        String[] validCallbackUrls = {"foo@example.com", "http://example.com", "http://foo@example.com", "http://foo:bar@example.com", "http://example.com/job/${JOB_ID}/${JOB_STATUS}", "http://example.com?foo=${JOB_ID}", "foo@example.com"};

        for (String callbackUrl : validCallbackUrls) {
            testCases.add(
                    new Object[]{callbackUrl}
            );
        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "testProcessStringCallbackUrlProvider")
    public void testProcessStringCallbackUrl(String callbackUrl) {
        JobRequestNotificationProcessor jobRequestNotificationProcessor = getMockJobRequetNotificationProcessor();

        try {
            doCallRealMethod().when(jobRequestNotificationProcessor).process(anyString());
            doCallRealMethod().when(jobRequestNotificationProcessor).process(any(ArrayNode.class));

            // call the method under test
            jobRequestNotificationProcessor.process(callbackUrl);

            // it should convert the callbackurl to json notification subscription objects and send to the primary
            // process(ArrayNode) method.
            verify(jobRequestNotificationProcessor, times(1)).process(any(ArrayNode.class));
            // this method should call to add each notification in turn.
            verify(jobRequestNotificationProcessor, times(3)).addNotification(any(ObjectNode.class));

            // ensure it didn't call out to the jsonnode method
            verify(jobRequestNotificationProcessor, never()).process(any(JsonNode.class));

            // 3 results should return
            assertEquals(jobRequestNotificationProcessor.getNotifications().size(), 3,
                    "Notification passed in as a callback url string should result in 3 notifications");

            // ensure all the expected events are there
            for (JobEventType event : List.of(FINISHED, FAILED, STOPPED)) {
                assertTrue(jobRequestNotificationProcessor.getNotifications().stream().anyMatch(n -> {
                    return event.name().equals(n.getEvent()) && // event should match
                            TEST_OWNER.equals(n.getOwner()) && // owner should be job owner
                            callbackUrl.equals(n.getCallbackUrl()) && // callback url should be callbackUrl
                            !n.isPersistent();   // these should not be persistent notifications as the event can only fire once.
                        }
                ), "Notification for " + event.name() + " should be present in the generated notifications.");
            }
        } catch (NotificationException e) {
            fail("Valid callbackUrl, \"" + callbackUrl + "\" should create 3 valid notifications.", e);
        }
    }

    @DataProvider
    protected Object[][] testProcessStringThrowsExceptionOnBadUrlProvider() {
        List<Object[]> testCases = new ArrayList<>();

        String[] invalidWebhookUrls = {"example.com", "example", "{}", "[]", "[{}]"};
        for (String callbackUrl : invalidWebhookUrls) {
            testCases.add(
                    new Object[]{callbackUrl}
            );
        }

        String[] invalidEmailAddresses = {"@example.com", "@example", "foo@example", "foo@", "@.com", "foo@.com"};
        for (String callbackUrl : invalidEmailAddresses) {
            testCases.add(
                    new Object[]{callbackUrl}
            );
        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "testProcessStringThrowsExceptionOnBadUrlProvider", expectedExceptions = NotificationException.class)
    public void testProcessStringThrowsExceptionOnBadUrl(String callbackUrl) throws NotificationException {
        JobRequestNotificationProcessor jobRequestNotificationProcessor = getMockJobRequetNotificationProcessor();

        doCallRealMethod().when(jobRequestNotificationProcessor).process(anyString());
        doCallRealMethod().when(jobRequestNotificationProcessor).process(any(ArrayNode.class));

        // call the method under test
        jobRequestNotificationProcessor.process(callbackUrl);
    }

    @DataProvider
    protected Object[][] testProcessStringIgnoresBlankCallbackUrlProvider() {
        List<Object[]> testCases = new ArrayList<>();

        String[] blankCallbackUrls = {"", " ", "   "};
        for (String callbackUrl : blankCallbackUrls) {
            testCases.add(
                    new Object[]{callbackUrl}
            );
        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "testProcessStringIgnoresBlankCallbackUrlProvider")
    public void testProcessStringIgnoresBlankCallbackUrl(String callbackUrl) {
        JobRequestNotificationProcessor jobRequestNotificationProcessor = getMockJobRequetNotificationProcessor();

        try {
            doCallRealMethod().when(jobRequestNotificationProcessor).process(anyString());

            // call the method under test
            jobRequestNotificationProcessor.process(callbackUrl);

            // no subsequent calls should be made.
            verify(jobRequestNotificationProcessor, never()).process(any(ArrayNode.class));
            verify(jobRequestNotificationProcessor, never()).process(any(JsonNode.class));
            verify(jobRequestNotificationProcessor, never()).addNotification(any(ObjectNode.class));

            assertTrue(jobRequestNotificationProcessor.getNotifications().isEmpty(),
                    "No notifications should be present when a blank callbackUrl is provided");
        } catch(NotificationException e) {
            fail("Blank callbackUrl should be ignored, not throw exception", e);
        }
    }

//    @Test
//    public void testProcessJsonNode() {
//        when(job.getUuid()).thenReturn("getUuidResponse");
//
//        jobRequestNotificationProcessor.process(null);
//    }

    /**
     * Test data for job notification test values
     *
     * @return job notification test cases
     */
    @DataProvider
    public Object[][] processArrayNodeProvider() {
        ObjectMapper mapper = new ObjectMapper();
        Object[] validUrls = {"http://example.com", "http://foo@example.com", "http://foo:bar@example.com", "http://example.com/job/${JOB_ID}/${JOB_STATUS}", "http://example.com?foo=${JOB_ID}", "foo@example.com"};
        Object[] invalidWebhookUrls = {"example.com", "example", "", null, 1L, mapper.createArrayNode(), mapper.createObjectNode()};
        Object[] invalidEmailAddresses = {"@example.com", "@example", "foo@example", "foo@", "@.com", "foo@.com"};
        Object[] validEvents = {JobStatusType.RUNNING.name(), JobStatusType.RUNNING.name().toLowerCase()};
        Object[] invalidEvents = {"", null, 1L, mapper.createArrayNode(), mapper.createObjectNode()};

        boolean pass = false;
        boolean fail = true;

        JsonNode validNotificationJsonNode = createJsonNotification(validUrls[0], JobStatusType.FINISHED.name(), false);

        List<Object[]> testCases = new ArrayList<Object[]>();
        for (Object url : validUrls) {
            for (Object event : validEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event, false)), pass, "Valid notifications should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event, true)), pass, "Valid notifications should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event)), pass, "Valid notifications without persistence field should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event)), pass, "Valid notifications without persistence field should pass"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event, false)).add(validNotificationJsonNode), pass, "Valid multiple notifications should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event, true)).add(validNotificationJsonNode), pass, "Valid multiple notifications should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event)).add(validNotificationJsonNode), pass, "Valid multiple notifications without persistence field should pass"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, event)).add(validNotificationJsonNode), pass, "Valid multiple notifications without persistence field should pass"});
            }

            for (Object invalidEvent : invalidEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, false)), fail, "Invalid notifications event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, true)), fail, "Invalid notifications event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)), fail, "Invalid notifications event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)), fail, "Invalid notifications event " + invalidEvent + " without persistence field should fail"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " without persistence field should fail"});
            }
        }

        for (Object invalidWebhookUrl : invalidWebhookUrls) {
            for (Object event : validEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, false)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, true)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail"});
            }

            for (Object invalidEvent : invalidEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, false)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, true)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail"});
            }
        }

        for (Object invalidEmailAddress : invalidEmailAddresses) {
            for (Object event : validEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, false)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, true)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail"});
            }

            for (Object invalidEvent : invalidEvents) {
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, false)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, true)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail"});

                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail"});
                testCases.add(new Object[]{mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail"});
            }
        }

        return testCases.toArray(new Object[][]{});
    }

    /**
     * Tests job notifications validation on jobs submitted as json
     *
     * @param notificationsJsonArray the array of json objects to be added to the job
     * @param shouldThrowException   true if processing should throw an exception
     * @param message                the message to assert for a failed test
     */
    @Test(dataProvider = "processArrayNodeProvider")
    public void processArrayNode(ArrayNode notificationsJsonArray, boolean shouldThrowException, String message) {

        JobRequestNotificationProcessor jobRequestNotificationProcessor = getMockJobRequetNotificationProcessor();

        try {
            // ensure this method call goes through
            doCallRealMethod().when(jobRequestNotificationProcessor).process(any(ArrayNode.class));

            // call the method under test
            jobRequestNotificationProcessor.process(notificationsJsonArray);

            // ensure it didn't call out to the json methods
            verify(jobRequestNotificationProcessor, never()).process(anyString());
            verify(jobRequestNotificationProcessor, never()).process(any(JsonNode.class));

            // this method should call to add each notification in turn.
            verify(jobRequestNotificationProcessor, times(notificationsJsonArray.size())).addNotification(any(ObjectNode.class));

            List<Notification> notifications = jobRequestNotificationProcessor.getNotifications();

            // ensure all the expected events are there
            assertEquals(notifications.size(), notificationsJsonArray.size(), "Unexpected notification count. Found " + notifications.size() + " expected " + notificationsJsonArray.size());

            assertFalse(shouldThrowException, message);

            // this won't correctly check for multiple notifications to the same event
            for (int i = 0; i < notificationsJsonArray.size(); i++) {
                JsonNode notificationJson = notificationsJsonArray.get(i);

                Optional<Notification> matchingNotification = notifications.stream().filter(n -> notificationJson.get("event").textValue().equals(n.getEvent())).findFirst();
                assertTrue(matchingNotification.isPresent(), "Notification should be created matching test " + notificationJson.get("event") + " event");
                assertEquals(matchingNotification.get().getCallbackUrl(), notificationJson.get("url").textValue(),
                        "Saved " + notificationJson.get("event").textValue() + " notification had the wrong callback url. Expected " +
                                notificationJson.get("url").textValue() + " found " + matchingNotification.get().getCallbackUrl());

                if (notificationJson.has("persistent")) {
                    assertEquals(matchingNotification.get().isPersistent(), notificationJson.get("persistent").asBoolean(),
                            "Saved " + notificationJson.get("event").textValue() + " notification had the wrong persistent value. Expected " +
                                    notificationJson.get("persistent").asBoolean() + " found " + matchingNotification.get().isPersistent());
                } else {
                    assertEquals(matchingNotification.get().isPersistent(), false,
                            "Saved " + notificationJson.get("event").textValue() + " notification defaulted to the wrong persistent value. Expected " +
                                    Boolean.FALSE + " found " + matchingNotification.get().isPersistent());
                }
            }
        } catch (NotificationException e) {
            if (!shouldThrowException) {
                fail(message, e);
            }
        }
    }
}
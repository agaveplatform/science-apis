package org.iplantc.service.notification.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.iplantc.service.notification.AbstractNotificationTest;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.events.NotificationMessageProcessor;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.notification.model.NotificationAttempt;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Test(groups={"integration"})
public class FailedNotificationAttemptQueueIT extends AbstractNotificationTest {
    private String TEST_USER = "TEST_USER";
    private ObjectMapper mapper = new ObjectMapper();

    public NotificationAttempt createAttempt(Notification notification) throws NotificationException {
        return NotificationMessageProcessor.process(notification, "SENT", TEST_USER, notification.getAssociatedUuid(), notification.toJSON());
    }

    public MongoDatabase getDatabase() {
        MongoDatabase mongoDatabase = null;
        try {
            MongoClient mongoClient = new MongoClient(
                    new ServerAddress(Settings.FAILED_NOTIFICATION_DB_HOST, Settings.FAILED_NOTIFICATION_DB_PORT),
                    MongoCredential.createScramSha1Credential(
                            Settings.FAILED_NOTIFICATION_DB_USER, "api", Settings.FAILED_NOTIFICATION_DB_PWD.toCharArray()),
                    MongoClientOptions.builder().build());

            mongoDatabase = mongoClient.getDatabase(Settings.FAILED_NOTIFICATION_DB_SCHEME);
        } catch (Exception e) {
            Assert.fail("Failed to establish connection to database.");
        }
        return mongoDatabase;
    }

    public void clearDeadLetterQueue() {
        try {
            MongoDatabase db = getDatabase();
            for (String collectionName : db.listCollectionNames()) {
                db.getCollection(collectionName).drop();
            }
        } catch (Exception e) {
            Assert.fail("Failed to clean out dead letter queues after tests run ", e);
        }
    }

    @AfterClass
    public void afterClass() throws NotificationException {
        clearNotifications();
        clearDeadLetterQueue();
    }

    @DataProvider(name = "fireProvider")
    protected Object[][] fireProvider() throws Exception {
        Notification validEmail = createEmailNotification();
        Notification validURL = createWebhookNotification();
        Notification validPort = createWebhookNotification();
        validPort.setCallbackUrl(requestBin.toString().replace("requestb.in", "requestb.in:80") + TEST_URL_QUERY);

        return new Object[][]{
                {validEmail, "Valid email notification attempt failed", true},
                {validURL, "Valid url notification attempt failed", true},
                {validPort, "Valid url with port notification attempt failed", true},
        };
    }

    @Test(dataProvider = "fireProvider", singleThreaded = true)
    public void pushTest(Notification notification, String errorMessage, boolean shouldSucceed) throws NotificationException, IOException {
        NotificationAttempt attempt = createAttempt(notification);
        FailedNotificationAttemptQueue queue = FailedNotificationAttemptQueue.getInstance();

        try {
            queue.push(attempt);
            if (!shouldSucceed)
                Assert.fail(errorMessage);
        } catch (Exception e) {
            if (shouldSucceed)
                Assert.fail("Pushing valid notification should not throw an exception.");
        }

        queue.findMatching(attempt.getNotificationId(), new HashMap<>(), 250, 0);

        MongoCursor cursor = getDatabase().getCollection(attempt.getNotificationId())
                .find(new BasicDBObject("notificationId", attempt.getNotificationId())).cursor();

        Assert.assertTrue(cursor.hasNext(), "Notification Attempt should be inserted into the capped collection.");

        String strResult = JSON.serialize(cursor.next());
        List<NotificationAttempt> attempts = new ArrayList<>();
        attempts.add(mapper.readValue(strResult, NotificationAttempt.class));

        for (NotificationAttempt notificationAttempt : attempts) {
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getNotificationId(), attempt.getNotificationId()), "NotificationId for attempt found should match the pushed object NotificationId.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getAssociatedUuid(), attempt.getAssociatedUuid()), "AssociatedId for attempt found should match the pushed object AssociatedId.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getCallbackUrl(), attempt.getCallbackUrl()), "CallbackUrl for attempt found should match the pushed object CallbackUrl.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getContent(), attempt.getContent()), "Content for attempt found should match the pushed object content.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getEventName(), attempt.getEventName()), "Event name for attempt found should match the pushed object event name.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getTenantId(), attempt.getTenantId()), "TenantId for attempt found should match the pushed object TenantId.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getOwner(), attempt.getOwner()), "Owner for attempt found should match the pushed object owner.");
            Assert.assertTrue(StringUtils.equals(notificationAttempt.getUuid(), attempt.getUuid()), "Uuid for attempt found should match the pushed object uuid.");
        }
    }

    @Test(dataProvider = "fireProvider")
    public void removeTest(Notification notification, String errorMessage, boolean shouldSucceed) throws NotificationException {
        FailedNotificationAttemptQueue queue = FailedNotificationAttemptQueue.getInstance();
        List<NotificationAttempt> addedAttempts = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            NotificationAttempt attempt = createAttempt(notification);
            addedAttempts.add(attempt);
        }

        try {
            NotificationAttempt removedAttempt =  queue.remove(addedAttempts.get(0).getNotificationId(), addedAttempts.get(0).getUuid());
            Assert.assertNull(removedAttempt);

            if (!shouldSucceed)
                Assert.fail(errorMessage);

        } catch (Exception e) {
            if (shouldSucceed)
                Assert.fail("Removing valid notification attempt should not throw an exception.");
        }

        MongoCursor cursor = getDatabase().getCollection(addedAttempts.get(0).getNotificationId())
                .find(new Document("notificationId", addedAttempts.get(0).getNotificationId())
                        .append("id", addedAttempts.get(0).getUuid())).cursor();

        if (cursor.hasNext()) {
            Document doc = (Document) cursor.next();
            Assert.fail("Nothing should be found for attempt that was removed.");
        }

    }

    @Test(dataProvider = "fireProvider")
    public void nextTest(Notification notification, String errorMessage, boolean shouldSucceed) throws NotificationException {
        FailedNotificationAttemptQueue queue = FailedNotificationAttemptQueue.getInstance();
        NotificationAttempt attempt = createAttempt(notification);
        NotificationAttempt nextAttempt = null;

        try {
            nextAttempt = queue.next(attempt.getNotificationId());

            if (!shouldSucceed)
                Assert.fail(errorMessage);

        } catch (Exception e) {
            if (shouldSucceed)
                Assert.fail("Getting the next valid notification should not throw exception.");
        }

        Assert.assertNotNull(nextAttempt, "Getting the next attempt in the queue should not return null.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getNotificationId(), attempt.getNotificationId()), "NotificationId for attempt found should match the pushed object NotificationId.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getAssociatedUuid(), attempt.getAssociatedUuid()), "AssociatedId for attempt found should match the pushed object AssociatedId.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getCallbackUrl(), attempt.getCallbackUrl()), "CallbackUrl for attempt found should match the pushed object CallbackUrl.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getContent(), attempt.getContent()), "Content for attempt found should match the pushed object content.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getEventName(), attempt.getEventName()), "Event name for attempt found should match the pushed object event name.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getTenantId(), attempt.getTenantId()), "TenantId for attempt found should match the pushed object TenantId.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getOwner(), attempt.getOwner()), "Owner for attempt found should match the pushed object owner.");
        Assert.assertTrue(StringUtils.equals(nextAttempt.getUuid(), attempt.getUuid()), "Uuid for attempt found should match the pushed object uuid.");
    }


    @Test(dataProvider = "fireProvider")
    public void removeAllTest(Notification notification, String errorMessage, boolean shouldSucceed) throws NotificationException {
        FailedNotificationAttemptQueue queue = FailedNotificationAttemptQueue.getInstance();
        List<NotificationAttempt> addedAttempts = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            NotificationAttempt attempt = createAttempt(notification);
            addedAttempts.add(attempt);
        }

        try {
            queue.removeAll(notification.getUuid());

            if (!shouldSucceed)
                Assert.fail(errorMessage);

        } catch (Exception e) {
            if (shouldSucceed)
                Assert.fail("Removing attempts for valid NotificationUuid should not throw exception.");
        }

        Assert.assertEquals(getDatabase().getCollection(notification.getUuid()).countDocuments(), 0, "Collection should be empty after removing all attempts.");

    }


    @Test(dataProvider = "fireProvider")
    public void findMatchingTest(Notification notification, String errorMessage, boolean shouldSucceed) throws NotificationException {
        FailedNotificationAttemptQueue queue = FailedNotificationAttemptQueue.getInstance();
        List<NotificationAttempt> addedAttempts = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            NotificationAttempt attempt = createAttempt(notification);
            addedAttempts.add(attempt);
        }

        List<NotificationAttempt> attempts = new ArrayList<>();

        try {
            attempts = queue.findMatching(notification.getUuid(), new HashMap<>(), 250, 0);

            if (!shouldSucceed)
                Assert.fail(errorMessage);

        } catch (Exception e) {
            if (shouldSucceed)
                Assert.fail("Valid notification attempt should not throw exception.");
        }

        Assert.assertEquals(attempts.size(), 5, "Found attempts should match the number of attempts added.");
        for (NotificationAttempt attempt : attempts) {
            boolean foundAttempt = false;
            for (NotificationAttempt addedAttempt : addedAttempts) {
                if (StringUtils.equals(addedAttempt.getUuid(), (attempt.getUuid()))) {
                    foundAttempt = true;
                    Assert.assertTrue(StringUtils.equals(attempt.getNotificationId(), addedAttempt.getNotificationId()), "NotificationId for attempt found should match the pushed object NotificationId.");
                    Assert.assertTrue(StringUtils.equals(attempt.getAssociatedUuid(), addedAttempt.getAssociatedUuid()), "AssociatedId for attempt found should match the pushed object AssociatedId.");
                    Assert.assertTrue(StringUtils.equals(attempt.getCallbackUrl(), addedAttempt.getCallbackUrl()), "CallbackUrl for attempt found should match the pushed object CallbackUrl.");
                    Assert.assertTrue(StringUtils.equals(attempt.getContent(), addedAttempt.getContent()), "Content for attempt found should match the pushed object content.");
                    Assert.assertTrue(StringUtils.equals(attempt.getEventName(), addedAttempt.getEventName()), "Event name for attempt found should match the pushed object event name.");
                    Assert.assertTrue(StringUtils.equals(attempt.getTenantId(), addedAttempt.getTenantId()), "TenantId for attempt found should match the pushed object TenantId.");
                    Assert.assertTrue(StringUtils.equals(attempt.getOwner(), addedAttempt.getOwner()), "Owner for attempt found should match the pushed object owner.");
                    Assert.assertTrue(StringUtils.equals(attempt.getUuid(), addedAttempt.getUuid()), "Uuid for attempt found should match the pushed object uuid.");
                }
            }
            if (!foundAttempt) {
                Assert.fail("Added attempt should be found.");
            }
        }
    }
}

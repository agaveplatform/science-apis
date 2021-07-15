package org.agaveplatform.service.transfers.messaging;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.support.JsonUtils;
import io.vertx.junit5.VertxExtension;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("NATS JetStream message client tests")
public class NatsJetstreamMessageClientIT {
    private static final Logger log = LoggerFactory.getLogger(NatsJetstreamMessageClientIT.class);

    public static String TEST_STREAM = "AGAVE_TRANSFERS_INTEGRATION_TESTS";
    public static String TEST_STREAM_SUBJECT = "test.transfers.>";
    public static String TEST_MESSAGE_SUBJECT_PREFIX = "test.transfers.agave_dev.";
    public static final String TEST_CLIENT_NAME = NatsJetstreamMessageClientIT.class.getSimpleName();
    public static final String NATS_URL = "nats://nats:4222";

    protected JetStreamSubscription nativeClientSubscription;
    protected Connection nc;
    protected JetStreamManagement jsm;

    @BeforeAll
    public void beforeAll() throws IOException, InterruptedException, JetStreamApiException {
        Options.Builder builder = new Options.Builder()
                .server(NATS_URL)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1)
                .connectionListener(new NatsConnectionListener())
                .errorListener(new NatsErrorListener());

        nc = Nats.connect(builder.build());
        jsm = nc.jetStreamManagement();
        try {
            StreamInfo si = jsm.getStreamInfo(TEST_STREAM);
            log.debug("Found test stream {}", TEST_STREAM);
            JsonUtils.printFormatted(si);
            PurgeResponse pr = jsm.purgeStream(TEST_STREAM);
            JsonUtils.printFormatted(pr);
            if (!pr.isSuccess()) {
                fail("Failed to purge test queue prior to test run");
            }
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                log.debug("Missing test stream {}", TEST_STREAM);
                StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                        .name(TEST_STREAM)
                        .addSubjects(TEST_STREAM_SUBJECT)
                        .storageType(StorageType.Memory)
                        .build();

                StreamInfo si = jsm.addStream(streamConfiguration);
                log.debug("Created test stream {}", TEST_STREAM);
                JsonUtils.printFormatted(si);
            }
        }

        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                .stream(TEST_STREAM)
                .durable(getTestClientName())
                .configuration(ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .build())
                .build();
        nativeClientSubscription = nc.jetStream().subscribe(TEST_STREAM_SUBJECT, pullSubscribeOptions);
    }

    @AfterEach
    protected void afterEach() throws IOException, JetStreamApiException {
        if (jsm != null) {
            jsm.purgeStream(TEST_STREAM);
        }
    }

    @AfterAll
    protected void afterAll() throws IOException, JetStreamApiException {
        if (jsm != null) {
            jsm.purgeStream(TEST_STREAM);
            jsm.deleteConsumer(TEST_STREAM, getTestClientName());
        }
    }

    /**
     * Helper method to generate a durable clientname based on the test class.
     *
     * @return unique name based on the test client
     */
    private String getTestClientName() {
        return NatsJetstreamMessageClientIT.class.getSimpleName();
    }

    /**
     * Helper function to fetch and ack a single message from NATS using the native test client.
     *
     * @return the next message of any subject in the stream, or null if none exist
     */
    private Message getSingleNatsMessage() {
        List<io.nats.client.Message> messages = nativeClientSubscription.fetch(1, Duration.ofSeconds(1));

        if (messages.isEmpty()) {
            log.debug("No message returned from stream");
            return null;
        } else {
            io.nats.client.Message msg = messages.get(0);
            msg.ack();
            log.debug("Fetched message from nats {}", msg.getSubject());
            JsonUtils.printFormatted(msg.metaData());
            return msg;
        }
    }

    @Test
    @DisplayName("Push message onto queue...")
    public void push() {
        NatsJetstreamMessageClient agaveMessageClient = null;
        String testBody = UUID.randomUUID().toString();
        io.nats.client.Message msg = null;

        try {
            agaveMessageClient = new NatsJetstreamMessageClient(NATS_URL, TEST_STREAM, "push-test-consumer");
            agaveMessageClient.getOrCreateStream(TEST_STREAM_SUBJECT);

            // test pushing of message for each message type
//            for (String messageType : MessageType.values()) {
            String subject = TEST_MESSAGE_SUBJECT_PREFIX + MessageType.TRANSFER_COMPLETED + ".push";
            try {
                // push a message with a unique body to ensure we can get it back.
                agaveMessageClient.push(subject, testBody);
            } catch (Exception e) {
                log.error("Failed to push {} message to stream {}", subject, TEST_STREAM, e);
                fail(e);
            }

            nc.flush(Duration.ofSeconds(1));
            msg = getSingleNatsMessage();

            assertNotNull(msg,
                    "No message found on test stream with the given subject after pushing message.");

            assertEquals(subject, msg.getSubject(),
                    "Subject of read message should match subject of message sent. " +
                            "Something else is likely writing test data");

            assertEquals(testBody, new String(msg.getData()),
                    "Received message should match sent message");

            try {
                // clean up test client
                nc.jetStreamManagement().deleteConsumer(TEST_STREAM, subject);
            } catch (Exception ignored) {}
        }
        catch (Exception e) {
            fail("Failed to push a message to the message queue", e);
        }
        finally {
            if (agaveMessageClient != null) agaveMessageClient.stop();
        }
    }

    @Test
    @DisplayName("Fetch message from queue...")
    public void fetch() {
        NatsJetstreamMessageClient agaveMessageClient = null;

        try {
            agaveMessageClient = new NatsJetstreamMessageClient(NATS_URL, TEST_STREAM, "fetch-test-consumer");
            agaveMessageClient.getOrCreateStream(TEST_STREAM_SUBJECT);

            // test pushing of message for each message type
//            for (String messageType : MessageType.values()) {

            String testBody = UUID.randomUUID().toString();
            String subject = TEST_MESSAGE_SUBJECT_PREFIX + MessageType.TRANSFERTASK_CREATED + ".fetch";

            try {
                // push a message with a unique body to ensure we can get it back.
                agaveMessageClient.push(subject, testBody);
            } catch (Exception e) {
                log.error("Failed to push {} message to stream {}", subject, TEST_STREAM, e);
                fail(e);
            }

            List<org.iplantc.service.common.messaging.Message> messages = null;
            try {
                messages = agaveMessageClient.fetch(subject, 1, 2);
            } catch (Exception e) {
                log.error("Failed to fetch message for subject {} in stream {}", subject, TEST_STREAM, e);
                fail(e);
            }

            assertFalse(messages.isEmpty(), "Test message pushed to stream should be fetched ");

            assertEquals(1, messages.size(),
                    "Exactly one message should be returned from test stream with when one is requested");

            assertEquals(testBody, messages.get(0).getMessage(),
                    "Body of fetched message should match body of message sent. " +
                            "Something else is likely writing test data");

        } catch (Exception e) {
            fail("Failed to push a message to the message queue", e);
        }
        finally {
            if (agaveMessageClient != null) agaveMessageClient.stop();
        }
    }

    @Test
    @DisplayName("Fetch multiple messages from queue...")
    public void fetchMany() {
        NatsJetstreamMessageClient agaveMessageClient = null;
        int testMessageCount = 5;

        try {
            agaveMessageClient = new NatsJetstreamMessageClient(NATS_URL, TEST_STREAM, "fetchMany-test-consumer");
            agaveMessageClient.getOrCreateStream(TEST_STREAM_SUBJECT);

            // test pushing of message for each message type
            String subject = TEST_MESSAGE_SUBJECT_PREFIX + MessageType.TRANSFERTASK_CREATED + ".fetchMultiple";
            List bodies = new ArrayList<String>();
            // push a message with a unique body to ensure we can get it back.
            for (int i=0; i<testMessageCount; i++) {
                String testBody = UUID.randomUUID().toString();
                bodies.add(testBody);
                try {
                    agaveMessageClient.push(subject, testBody);
                } catch (Exception e) {
                    log.error("Failed to push message to subject {} in stream {}", subject, TEST_STREAM, e);
                    fail(e);
                }
            }

            nc.flush(Duration.ofSeconds(1));

            List<org.iplantc.service.common.messaging.Message> messages = null;
            try {
                messages = agaveMessageClient.fetch(subject, testMessageCount, 5);
            } catch (Exception e) {
                log.error("Failed to fetch message for subject {} in stream {}", subject, TEST_STREAM, e);
                fail(e);
            }
            assertFalse(messages.isEmpty(), "Test message pushed to stream should be fetched ");

            assertEquals(testMessageCount, messages.size(),
                    "Exactly " + testMessageCount + " messages should be returned from test stream with when one is requested");

        } catch (Exception e) {
            fail("Fetching multiple tasks should work", e);
        }
        finally {
            if (agaveMessageClient != null) agaveMessageClient.stop();
        }
    }

    @Test
    @DisplayName("Pop single message from queue...")
//    @Disabled
    public void pop() {
        NatsJetstreamMessageClient agaveMessageClient = null;

        try {
            agaveMessageClient = new NatsJetstreamMessageClient(NATS_URL, TEST_STREAM, "pop-test-consumer");
            agaveMessageClient.getOrCreateStream(TEST_STREAM_SUBJECT);

            // test pushing of message for each message type
//            for (String messageType : MessageType.values()) {
                String testBody = UUID.randomUUID().toString();
                String subject = TEST_MESSAGE_SUBJECT_PREFIX + MessageType.TRANSFERTASK_DELETED + ".pop";

                try {
                    // push a message with a unique body to ensure we can get it back.
                    agaveMessageClient.push(subject, testBody);
                } catch (Exception e) {
                    log.error("Failed to push {} message to stream {}", subject, TEST_STREAM, e);
                    fail(e);
                }

                org.iplantc.service.common.messaging.Message popMessage = null;
                try {
                    popMessage = agaveMessageClient.pop(subject);
                } catch (Exception e) {
                    log.error("Failed to pop message for subject {} in stream {}", subject, TEST_STREAM, e);
                    fail(e);
                }

                assertNotNull(popMessage, "Popped message should not be null.");

                assertEquals(testBody, popMessage.getMessage(),
                        "Body of fetched message should match body of message sent. " +
                                "Something else is likely writing test data");

        } catch (Exception e) {
            fail("Popping message should succeed", e);
        }
        finally {
            if (agaveMessageClient != null) agaveMessageClient.stop();
        }
    }

    @Test
    @DisplayName("Listen for messages pushed from a stream...")
    public void listen() {
        NatsJetstreamMessageClient agaveMessageClient = null;
        int testMessageCount = 5;

        try {
            agaveMessageClient = new NatsJetstreamMessageClient(NATS_URL, TEST_STREAM, "listen-test-consumer");
            agaveMessageClient.getOrCreateStream(TEST_STREAM_SUBJECT);

            CountDownLatch msgLatch = new CountDownLatch(testMessageCount);
            AtomicInteger received = new AtomicInteger();
            AtomicInteger ignored = new AtomicInteger();
            List<io.nats.client.Message> messages = new ArrayList<>();
            // create our message handler.
            MessageHandler handler = msg -> {
                if (msgLatch.getCount() == 0) {
                    ignored.incrementAndGet();
                    if (msg.isJetStream()) {
                        msg.nak();
                    }
                }
                else {
                    received.incrementAndGet();
                    msg.ack();
                    messages.add(msg);
                    msgLatch.countDown();
                }
            };

            // test pushing of message for each message type
            String subject = TEST_MESSAGE_SUBJECT_PREFIX + MessageType.TRANSFERTASK_CREATED + ".listen";
            List bodies = new ArrayList<String>();
            // push a message with a unique body to ensure we can get it back.
            for (int i=0; i<testMessageCount; i++) {
                String testBody = UUID.randomUUID().toString();
                bodies.add(testBody);
                agaveMessageClient.push(subject, testBody);
            }

            nc.flush(Duration.ofSeconds(1));

            agaveMessageClient.listen(subject, handler);

            // Wait for messages to arrive using the countdown latch. But don't wait forever.
            boolean countReachedZero = msgLatch.await(5, TimeUnit.SECONDS);

            assertTrue(countReachedZero, "All messages should be consumed in the allotted time.");

            assertEquals(testMessageCount, received.get(), "All test messages should be fetched.");

            assertEquals(0, ignored.get(),
                    "No message should be ingored in the test");

        } catch (Exception e) {
            fail("Failed to push a message to the message queue", e);
        }
        finally {
            if (agaveMessageClient != null) agaveMessageClient.stop();
        }
    }

//
//	@Test(dependsOnMethods={"pop"})
//	public void popMultiple()
//	{
//		BeanstalkClient messageClient = new BeanstalkClient();
//
//		try
//		{
//			String messageText = "abcd1234-abcd1234-abcd1234-abcd1234";
//			// push a message onto the exchange
//			nativeClient.useTube(TEST_EXCHANGE_TOPIC_QUEUE);
//			List<Long> notifs = new ArrayList<Long>();
//			for(int i=0;i<5;i++)
//			{
//				notifs.add(nativeClient.put(65536, 0, 120, messageText.getBytes()));
//			}
//
//			for(int i=0;i<5;i++)
//			{
//				Message poppedMessage = messageClient.pop(TEST_STREAM, TEST_EXCHANGE_TOPIC_QUEUE);
//
//				Assert.assertNotNull(poppedMessage, "No message popped from the queue.");
//				Assert.assertNotNull(poppedMessage.getId(), "No message id returned.");
//				Assert.assertEquals(notifs.get(i), (Long)poppedMessage.getId(), "Retrieved wrong message from queue");
//				Assert.assertEquals(poppedMessage.getMessage(), messageText);
//
//				messageClient.delete(TEST_STREAM, TEST_EXCHANGE_TOPIC_QUEUE, (Long)poppedMessage.getId());
//			}
//		}
//		catch (Throwable e)
//		{
//			Assert.fail("Failed to pop multiple messages", e);
//		}
//		finally {
//			messageClient.stop();
//		}
//	}
//
//
//	@Test(dependsOnMethods={"push"})
//	public void reject()
//	{
//		BeanstalkClient messageClient = new BeanstalkClient();
//		String message = "abcd1234-abcd1234-abcd1234-abcd1234";
//		try
//		{
//			// push a message onto the exchange
//			messageClient.push(TEST_NATS_JS_STREAM, TEST_EXCHANGE_TOPIC_QUEUE, message);
//
//			Message msg = messageClient.pop(TEST_STREAM, TEST_EXCHANGE_TOPIC_QUEUE);
//
//			Assert.assertEquals(msg.getMessage(), message, "Pushed and popped messages do not match");
//
//			messageClient.reject(TEST_STREAM, TEST_EXCHANGE_TOPIC_QUEUE, (Long)msg.getId(), message);
//
//			nativeClient.watch(TEST_EXCHANGE_TOPIC_QUEUE);
//			nativeClient.delete((Long)msg.getId());
//
//			Assert.assertNull(nativeClient.peek((Long)msg.getId()), "Job was not deleted");
//		}
//		catch (Exception e)
//		{
//			Assert.fail("Failed to release message back to queue", e);
//		}
//		finally {
//			messageClient.stop();
//		}
//	}
}

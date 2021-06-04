package org.agaveplatform.service.transfers.messaging;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;
import org.agaveplatform.service.transfers.exception.DuplicateMessageException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.util.Slug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * This is a convenience class for interacting with NATS messaging service. NATS supports push, pull, and (https://docs.nats.io/) request-reply semantics over streaming and ephemeral queues. Agave primarily leverages the <a href="https://docs.nats.io/jetstream/jetstream" target="_top">JetStream</a> functionality of NATS to enable durable queues and message replay as needed.
 * Jetstream client interfaces needed for this client will always be reading from a stream. We can do this 2 ways
 * 1. PULL use cases: we fetch what we need manually
 *   - one or many messages
 *   - consumer group (work pool) or singular consumer
 *   - sync (?and async?)
 * 2. PUSH use case: we let nats push the messages to us as they are written to the stream
 *   - accept handler to process messages as they come in
 *   - consumer group (work pool) or singular consumer
 * 3. PUBLISH: we write a message to the stream
 *    - write a single message to a single subject
 *    - allow message duplication for the time being?
 *
 * Each instance of this class maintains a {@link #subscriptionMap} of all subscription that have been crated over the
 * life of the instance. Subscriptions are lightweight and can be reused and replicated across threads. Thus, one JVM
 * can have a single connection with many subscriptions created in each thread. Each instance also has a {@link #stop()}
 * method, which will unsubscribe every active subscription from the server and remove them from the cache. The
 * connection will, however be left in tact.  The {@link #disconnect()} is a nuclear option that kills the current
 * connection without unsubscribing or deleting any clients. This should be called in the shutdown handler of the
 * parent thread of any application, whereas {@link #stop} should be called in the shutdown handler of each thread
 * invoking the class.
 *
 * Consumer groups are defined as unique combinations of a consumer name (referred to as the "durable" value in nats
 * consumer parlance), and a consumer group name (referred to as the "queue" value in nats consumer parlance). Every
 * subscription created with these same two values, subscribed to the same subject, across instances, threads, and JVM,
 * will be viewed as a member of the same consumer group in NATS and thus participate in an "exactly once" delivery
 * strategy by NATS. That means that for any given message written to the subject listened to by a subject group, only
 * one subscriber will receive the message. (Usual ACK and timeout behavior apply, but to the entire group).
 * The connection parameters for NATS are specified by the vertx config file and overridden by environment variables.
 * We do this for flexibility and to maintain DRY principles. Streams are set up extetrnally as part of our devops
 * pipeline. Subjects are established by domain logic of individual services. Auth is not currently enabled, though
 * both tls and auth will be put in place for our release.
 *
 */
public class NatsJetstreamMessageClient {
    private static final Logger log = LoggerFactory.getLogger(NatsJetstreamMessageClient.class);

    protected static int DEFAULT_TIMEOUT_SECONDS;
    protected static final int MAX_ATTEMPTS = 3;
    private static final Object lock = new Object();

    public NatsJetstreamMessageClient (){}

    /**
     * Static connection shared across all instances.
     */
    private static Connection conn;
    private static Dispatcher dispatcher;
    private final Map<String, JetStreamSubscription> subscriptionMap = new HashMap<String, JetStreamSubscription>();

    private JetStreamManagement jsm;
    private JetStream js;
    private String consumerName;
    private String streamName;

    /**
     * Instantiates a new Nats Jetstream client. If no connection has been established, it will be here using a
     * threadsafe static {@link #connect(String)} method.
     * @param connectionUri the nats url
     * @param streamName the name of the stream to which to connect
     * @param consumerName the name of the consumer to create.
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public NatsJetstreamMessageClient(String connectionUri, String streamName, String consumerName) throws IOException, InterruptedException {
        connect(connectionUri);
        setConsumerName(consumerName);
        setStreamName(streamName);
    }

    /**
     * The current connnection. This is a singleton instantiate by the first instance of this class at runtime. The
     * underlying NATS library will handle reconnections, pauses, and eventing through notifications to the dispatcher
     * we register with our subscription, and with the connection, so no further work is needed on our behalf.
     * @return an active NATS connection
     */
    public static Connection getConn() {
        return conn;
    }

    /**
     * Gets the name of the stream to which the client will interact
     * @return name of the JetStream stream with which this client interacts
     */
    public String getStreamName() {
        return this.streamName;
    }

    /**
     * Sets the name of the stream to which the client will interact
     * @param streamName name of the JetStream stream name to interact with
     */
    protected void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    /**
     * Establishes a connection to the NATS server at the given {@code connectionUrl}. A single NATS Connection object
     * can handle multiple consumers subscribing and publishing to a single stream. We use a sychronized singleton
     * pattern here to have thread safety on the connection when creating multiple instances.
     *
     * @param connectionUrl the url of the NATS server
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public void connect(String connectionUrl) throws IOException, InterruptedException {
        synchronized(lock) {
            if (conn == null) {
                Options.Builder builder = new Options.Builder()
                        .server(connectionUrl)
                        .connectionTimeout(Duration.ofSeconds(5))
                        .pingInterval(Duration.ofSeconds(10))
                        .reconnectWait(Duration.ofSeconds(1))
                        .maxReconnects(-1)
                        .connectionListener(new NatsConnectionListener())
                        .errorListener(new NatsErrorListener());

               conn = Nats.connect(builder.build());
               dispatcher = conn.createDispatcher();
            }
        }
    }

    /**
     * Creates a JetStream subscription to {@code streamName} for subject {@code subject} and saves for future use.
     * Subscriptions are left open until explicitly unsubscribed. Calling {@link #stop()} will unsubscribe all
     * {@link Subscription} for this instance before closing the {@link #conn}.
     *
     * @param subject the subject
     * @return a subscription to the stream filtered by subject
     * @throws IOException IOException if a networking issue occurs
     * @throws JetStreamApiException the request had an error related to the data
     */
    protected JetStreamSubscription getOrCreatePullSubscription(String subject) throws IOException, JetStreamApiException {
        String durable = Slug.toSlug("pull-" + subject);
        String subscriptionKey = getStreamName() + "-" + durable;
        if (!subscriptionMap.containsKey(subscriptionKey)) {
            PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                    .durable(durable)
                    .build();

            JetStreamSubscription subscription = getJetStream().subscribe(subject, pullSubscribeOptions);
            subscriptionMap.put(subscriptionKey, subscription);
        }

        return subscriptionMap.get(subscriptionKey);
    }

    /**
     * Creates a JetStream subscription to {@code streamName} for subject {@code subject} and saves for future use.
     * Subscriptions are left open until explicitly unsubscribed. Calling {@link #stop()} will unsubscribe all
     * {@link Subscription} for this instance before closing the {@link #conn}.
     *
     * @param subject the subject
     * @return a subscription to the stream filtered by subject
     * @throws IOException IOException if a networking issue occurs
     * @throws JetStreamApiException the request had an error related to the data
     */
    protected JetStreamSubscription getOrCreatePullGroupSubscription(String group, String subject) throws IOException, JetStreamApiException {
        String durable = Slug.toSlug("pull-" + subject);
        String subscriptionKey = getStreamName() + "-" + durable;
        if (!subscriptionMap.containsKey(subscriptionKey)) {
            PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder()
                    .stream(getStreamName())
                    .durable(durable)
                    .build();

            JetStreamSubscription subscription = getJetStream().subscribe(subject, group, pushSubscribeOptions);
            subscriptionMap.put(subscriptionKey, subscription);
        }

        return subscriptionMap.get(subscriptionKey);
    }

    /**
     * Creates a JetStream subscription to {@code streamName} for subject {@code subject} and saves for future use.
     * Subscriptions are left open until explicitly unsubscribed. Calling {@link #stop()} will unsubscribe all
     * {@link Subscription} for this instance before closing the {@link #conn}.
     *
     * @param group the name of the consumer group to join. The client name and  effective creates
     * @param subject the subject
     * @return a subscription to the stream filtered by subject
     * @throws IOException IOException if a networking issue occurs
     * @throws JetStreamApiException the request had an error related to the data
     */
    protected JetStreamSubscription getOrCreatePushGroupSubscription(String group,  String subject) throws IOException, JetStreamApiException {
        String subscriptionKey = getStreamName() + "-" + subject;
        if (!subscriptionMap.containsKey(subscriptionKey)) {
            PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder()
                    .stream(getStreamName())
                    .durable(Slug.toSlug(subject))
                    .build();

            JetStreamSubscription subscription = getJetStream().subscribe(subject, group, pushSubscribeOptions);
            subscriptionMap.put(subscriptionKey, subscription);
        }

        return subscriptionMap.get(subscriptionKey);
    }

    /**
     * Creates a JetStream subscription to {@code streamName} for subject {@code subject} and saves for future use.
     * Subscriptions are left open until explicitly unsubscribed. Calling {@link #stop()} will unsubscribe all
     * {@link Subscription} for this instance before closing the {@link #conn}.
     *
     * @param subject the subject
     * @return a subscription to the stream filtered by subject
     * @throws IOException IOException if a networking issue occurs
     * @throws JetStreamApiException the request had an error related to the data
     */
    protected JetStreamSubscription getOrCreatePushSubscription(String subject) throws IOException, JetStreamApiException {
        String subscriptionKey = getStreamName() + "-" + subject;
        if (!subscriptionMap.containsKey(subscriptionKey)) {
            PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                    .stream(getStreamName())
                    .durable(subject)
                    .build();

            JetStreamSubscription subscription = getJetStream().subscribe(subject, pullSubscribeOptions);
            subscriptionMap.put(subscriptionKey, subscription);
        }

        return subscriptionMap.get(subscriptionKey);
    }

    /**
     * Publishes a message onto the given subject
     *
     * @param subject    name of subject to which the message will be pushed
     * @param body  the message content to persist
     * @throws MessagingException if communication with Nats fails
     */
    public void push(String subject, String body) throws MessagingException {
        // create a typical NATS message
        io.nats.client.Message natsMessage = NatsMessage.builder()
                .subject(subject)
                .data(body.getBytes(StandardCharsets.UTF_8))
                .build();

        PublishOptions pubishOptions = PublishOptions.builder()
                .expectedStream(getStreamName())
                .build();

        // Publish a message and print the results of the publish acknowledgement.
        // An exception will be thrown if there is a failure.
        int attempts = 0;

        while (attempts < MAX_ATTEMPTS) {
            try {
                PublishAck pa = getJetStream().publish(natsMessage, pubishOptions);
                if (pa.isDuplicate()) {
                    throw new DuplicateMessageException("Message already exists in NATS stream");
                } else if (pa.hasError()) {
                    attempts++;
                    log.error("[{}] Failed to publish {} message to NATS: {}", attempts, subject, pa.getError());

                    if (attempts == MAX_ATTEMPTS) {
                        throw new MessagingException("Unable to publish message to NATS server after " + MAX_ATTEMPTS + " attempts.");
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("NatsJetstreamMessage{" +
                                "stream='" + getStreamName() + "'" +
                                ", subject=" + subject +
                                ", seq=" + pa.getSeqno() +
                                "}");
                    }
                    return;
                }
            } catch (JetStreamApiException e) {
                throw new MessagingException("Invalid message body response returned from NATS.", e);
            } catch (IOException e) {
                throw new MessagingException("Unexpected error while pushing message to NATS server.", e);
            }
        }
    }

    /**
     * Publishes a message onto the given subject with a delayed execution
     *
     * @param subject  subject to filter for messages
     * @param body        the message body to persist
     * @param secondsToDelay the number of seconds to delay the message from being processed
     * @throws MessagingException if communication with the subject fails
     */
    public void push(String subject, String body, int secondsToDelay) throws MessagingException {
        log.debug("Delayed messages are not supported by NATS JetStream. Message will be added immediately");
        push(subject, body);
    }

    /**
     * Returns a message back to the stream for consumption by a different process.
     *
     * @param subject  subject to filter for messages. This is ignored for this client
     * @param messageId the unique id of the message to reject
     * @param message   the body of the message to reject. This is ignored for this client
     * @throws MessagingException if communication with the NATS server fails
     */
    public void reject(String subject, Object messageId, String message) throws MessagingException {
        log.info("Message rejection is not supported. The same message will instead be pushed back to the stream.");
        delete(messageId);
        push(subject, message);
    }

    /**
     * Deletes a message from the stream.
     *
     * @param messageId the unique id of the message
     * @throws MessagingException if communication with the NATS server fails or delete fails
     */
    public void delete(Object messageId) throws MessagingException {
        try {
            if (!getJetStreamManagement().deleteMessage(getStreamName(), (long)messageId)) {
                throw new MessagingException("Failed to delete message from the stream");
            }
        } catch (IOException e) {
            throw new MessagingException("Unexpected error connecting to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Error returned from NATS server deleting message " + messageId, e);
        }
    }

    /**
     * Gets the next event from the stream with the given subject. The distribution of messages is controlled by
     * the NATS. This will simply pull the next messages.
     *
     * @param subject  subject to filter for messages
     * @return the next message with the given the subject on the given stream
     * @throws MessagingException if communication with the NATS server fails
     */
    public Message pop(String subject) throws MessagingException {
        return pop(subject, 1).stream().findFirst().orElse(null);
    }

    /**
     * Gets the next count events from the stream with the given subject. The distribution of messages is controlled by
     * Jetstream. This will simply pull the next {@code numberOfMessages} messages.
     *
     * @param subject  subject to filter for messages
     * @param numberOfMessages    the number of messages to pull from the stream
     * @return the next message in the stream matching the given subject
     * @throws MessagingException if communication with the NATS server fails
     */
    public List<Message> pop(String subject, int numberOfMessages) throws MessagingException {
        return fetch(subject, numberOfMessages, DEFAULT_TIMEOUT_SECONDS * numberOfMessages);
    }

    /**
     * Gets the next {@code count} events from the {@code stream} matching subject {@code subject}. The distribution
     * of messages is controlled by Jetstream. This will simply pull the next {@code numberOfMessages} messages and
     * timeout if it takes longer than {@code timeoutSeconds} seconds
     *
     * @param subject  subject to filter for messages
     * @param numberOfMessages    the number of messages to pull from the stream
     * @param timeoutSeconds the time in seconds to wait for the fetch operation to timeout
     * @return list of at most {@code numberOfMessages} messages
     * @throws MessagingException if communication with the NATS server fails or syntax is invalid
     */
    public List<Message> fetch(String subject, int numberOfMessages, int timeoutSeconds) throws MessagingException {
        try {
            List<Message> agaveMessages = new ArrayList<>();
            JetStreamSubscription subscription = getOrCreatePullSubscription(subject);
            int read = 0;
            while (read < numberOfMessages) {
                List<io.nats.client.Message> message = subscription.fetch(10, Duration.ofSeconds(timeoutSeconds));
                for (io.nats.client.Message m : message) {
                    read++;
                    m.ack();
                    agaveMessages.add(new Message(m.getSID(), new String(m.getData())));
                }
            }
            return agaveMessages;
        } catch (IOException e) {
            throw new MessagingException("Unexpected error connecting to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Error returned from NATS server fetching " + numberOfMessages +
                    " messages from " + getStreamName() + " with subject " + subject, e);
        }
    }

    /**
     * Touches a message or job in the stream to keep it alive. With NATS JetStream, timeout is set at the stream level.
     * Messages are acknowledged immediately, so touching has no effect. We would need to find, delete, then
     * reinsert the message for this operation to make sense. That simply is not useful with a durable queue, so this
     * is a noop.
     *
     * @param messageId the unique id of the message
     * @param stream the name of the stream
     * @return true if the refresh succeeded, false otherwise
     * @throws MessagingException if communication with the NATS server fails
     */

    public boolean touch(Object messageId, String stream) throws MessagingException {
        log.info("Touching is not supported by NATS JetStream. Ignoring request to touch message " + messageId);
        return true;
    }

    /**
     * Starts an async listener to process messages matching the given {@code subject}, in order, resolving
     * the {@link MessageHandler} with each message. NATS will deliver messages to every consumer subscribed to a
     * subject matching the message subject. This means that multiple consumers will all receive the message. If
     * exactly once delivery is desired, use the {@link #listen(String, String, MessageHandler)} method.
     *
     * If this method is subsequently called, the subscription will be terminated and recreated with the new
     * {@code handler}. This allows for handlers to be updated over time as needed. Calling the {@link #stop()} method
     * will unsubscribe this and all other subscriptions.
     *
     * @param subject name of the subject
     * @param handler the message dispatch handler
     * @throws MessagingException if communication with the NATS server fails
     * @throws InterruptedException if the current thread is interrupted
     *
     */
    public void listen(String subject, MessageHandler handler) throws MessagingException, InterruptedException {
        listen(subject, null, handler);
    }

    /**
     * Starts an async listener to process messages matching the given {@code subject}, in order, resolving
     * the {@link MessageHandler} with each message. The subscription will join all other consumers subscribing to the
     * same subject and group name. NATS will ensure exactly once delivery of a message to a group. The listener will
     * continue to run until unsubscribed via a {@link #stop()} call. When {@code queueName} is null, this falls back
     * on a broadcast subscription and exactly once delivery is not guaranteed.
     *
     * @param subject name of the subject
     * @param queueName    name of queue group to join
     * @param handler the message dispatch handler
     * @throws MessagingException if communication with the NATS server fails
     * @throws InterruptedException if the current thread is interrupted
     */
    public void listen(String subject, String queueName, MessageHandler handler) throws MessagingException, InterruptedException {

        JetStreamSubscription subscription = subscriptionMap.get(subject);
        if (subscription!= null) {
            subscription.unsubscribe();
            subscriptionMap.remove(subject);
        }

        try {
            ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(Duration.ofSeconds(60))
                    .durable(this.getConsumerName())
                    .filterSubject(subject)
                    .deliverPolicy(DeliverPolicy.All)
                    .maxDeliver(10)
                    .rateLimit(10)
                    .replayPolicy(ReplayPolicy.Instant)
                    .build();

            PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder()
                    .stream(getStreamName())
                    .configuration(consumerConfiguration)
                    .durable(getConsumerName())
                    .build();
            if (queueName != null) {
                //JetStreamSubscription jsm = getOrCreatePushGroupSubscription(queueName, subject);
                subscriptionMap.put(subject, getJetStream().subscribe(subject, queueName, getDispatcher(), handler, true, pushSubscribeOptions));
            } else {
                subscriptionMap.put(subject, getJetStream().subscribe(subject, getDispatcher(), handler, true, pushSubscribeOptions));
            }
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while subscribing to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Invalid request made to NATS Jetstream to subscribe for push messages", e);
        }
    }


    /**
     * Forces all consumers to drain and the connection to close.
     */
    public void stop() {
        subscriptionMap.forEach((key, subscription) -> {
            try {
                subscription.unsubscribe();
            } catch (Exception e) {
                log.debug("Failed to unsubscribe client " + key);
            }
        });
    }

    /**
     * Closes the thread-safe {@link #conn} for this class. Any subscriptions created by concrete instances of this
     * class will be abandoned and have to be cleaned up manually.
     */
    public static void disconnect() {
        try {
            conn.close();
        } catch (InterruptedException e) {
            log.error("Thread interrupted while closing the connection to NATS", e);
        }
    }

    /**
     * Returns a list of all the subscriptions current present in this instance.
     *
     * @return List of all the available subscriptions in this instance
     */
    public Collection<JetStreamSubscription> listSubscriptions() {
        return subscriptionMap.values();
    }

    /**
     * Returns a set of the names of all the subscription names for this instance.
     *
     * @return List of the available session names in this intsance
     */
    public Set<String> listSubscriptionNames() {
        return subscriptionMap.keySet();
    }

    /**
     * Returns true if a stream exists matching the given name.
     *
     * @param subjectName the name of the subject subscription to lookup
     * @return true if the stream exists, false otherwise
     * @throws MessagingException if communication with the NATS server fails
     * @see #listSubscriptionNames()
     */
    public boolean subscriptionExists(String subjectName) throws MessagingException {
        return listSubscriptionNames().contains(subjectName);
    }

    /**
     * Find a stream whose name matches the given regular expression. There is no guarantee
     * that this will be the best fit match, just that it will match.
     *
     * @param regex the regular expression used to match the subject name
     * @return name of subject matching the regex
     * @throws MessagingException if communication with the NATS server fails
     */
    public String findQueueMatching(String regex) throws MessagingException {
        try {
            Optional<String> matchingSubject = getJetStreamManagement().getStreamInfo(getStreamName())
                    .getConfiguration()
                        .getSubjects().stream()
                            .filter(subject -> Pattern.matches(regex, subject)).findFirst();

            return matchingSubject.orElse(null);
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while fetching stream subjects from NATS server.", e);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                return null;
            }
            throw new MessagingException("Invalid request made to NATS Jetstream to fetch stream subjects", e);
        }
    }

    /**
     * Checks for the existence of a given message.
     *
     * @param messageId the unique id of the message
     * @return true if a message with the given ID exists. False otherwise.
     * @throws MessagingException if communication with the NATS server fails
     */
    public boolean messageExist(Object messageId) throws MessagingException {
        try {
            return (getJetStreamManagement().getMessage(getStreamName(), (long)messageId) != null);

        } catch (IOException e) {
            throw new MessagingException("Unexpected error connecting to NATS server.", e);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                return false;
            }
            throw new MessagingException("Error returned from NATS server fetching message " + messageId, e);
        }
    }

    /**
     * Identical to {@link #pop(String)}, but waits {@code timeoutSeconds} to receive a reserved message.
     *
     * @param subject    name of subject
     * @param timeoutSeconds  the number of seconds to wait before the reservation times out
     * @return Successfully reserved message
     * @throws MessagingException if a message is not received during timeoutSeconds or the operation fails
     */
    public Message reserve(String subject, int timeoutSeconds) throws MessagingException {
        List<Message> messages = fetch(subject, 1, timeoutSeconds);
        return messages.isEmpty() ? null : messages.get(0);
    }

    /**
     * Obtains a JetStream instance to the NATS server at the given {@code conn}
     *
     * @return a JetStream instance
     * @throws IOException if a networking issue occurs
     */
    protected JetStream getJetStream() throws IOException {
        if (js == null) {
            setJetStream(conn.jetStream());
        }

        return js;
    }

    /**
     * @param jetStream the JetStream to set
     */
    private void setJetStream(JetStream jetStream) {
        this.js = jetStream;
    }

    /**
     * Obtains a JetStreamManagement handle to the NATS server at the given {@code conn}
     * @return an admin client to manage jetstream streams for the current connection
     * @throws IOException if a networking issue occurs
     */
    public JetStreamManagement getJetStreamManagement() throws IOException {
        if (jsm == null) {
            setJetStreamManagement(conn.jetStreamManagement());
        }
        return jsm;
    }

    /**
     * @param jetStreamManagement The jetStreamManagement to set
     */
    private void setJetStreamManagement(JetStreamManagement jetStreamManagement) {
        this.jsm = jetStreamManagement;
    }
    protected Dispatcher getDispatcher() {
        return dispatcher;
    }
    public String getConsumerName() {
        return consumerName;
    }
    protected void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }
}
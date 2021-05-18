package org.agaveplatform.service.transfers.messaging;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import org.agaveplatform.service.transfers.exception.DuplicateMessageException;
import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.messaging.MessageQueueListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NatsJetstreamMessageClient implements MessageQueueClient {
    private static final Logger log = LoggerFactory.getLogger(NatsJetstreamMessageClient.class);

    protected static int DEFAULT_TIMEOUT_SECONDS;

    private static final Object lock = new Object();



    /**
     * Static connection shared across all instances.
     */
    private static Connection conn;
    private static Dispatcher dispatcher;

    private JetStreamManagement jsm;
    private JetStream js;
    private String consumerName;
    private final Map<String, JetStreamSubscription> subscriptionMap = new HashMap<String, JetStreamSubscription>();

    public NatsJetstreamMessageClient(String connectionUri, String consumerName) throws IOException, InterruptedException {
        setConsumerName(consumerName);
        connect(connectionUri);
    }
    public NatsJetstreamMessageClient(String connectionUri) throws IOException, InterruptedException {
        connect(connectionUri);
    }

    public static Connection getConn() {
        return conn;
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
     * Establishes a connection to the NATS server at the given {@code connectionUrl}. A single NATS Connection object
     * can handle multiple consumers subscribing and publishing to a single stream. We use a sychronized singleton
     * pattern here to have thread safety on the connection when creating multiple instances.
     *
     * @param connectionUrl the url of the NATS server
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public Connection connectNats(String connectionUrl) throws IOException, InterruptedException {
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
                return conn;
            }
        }
        return conn;
    }


    /**
     * Creates a JetStream subscription to {@code streamName} for subject {@code subject} and saves for future use.
     * Subscriptions are left open until explicitly unsubscribed. Calling {@link #stop()} will unsubscribe all
     * {@link Subscription} for this instance before closing the {@link #conn}.
     *
     * @param stream the name of the stream
     * @param subject the subject
     * @return a subscription to the stream filtered by subject
     * @throws IOException IOException if a networking issue occurs
     * @throws JetStreamApiException the request had an error related to the data
     */
    protected JetStreamSubscription getOrCeateSubscription(String stream, String subject) throws IOException, JetStreamApiException {
        String subscriptionKey = stream + "-" + subject;
        if (!subscriptionMap.containsKey(subscriptionKey)) {
            PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                    .stream(stream)
                    .durable(subject)
                    .build();
            // TODO: add queue name to ensure grouping.
            // TODO: Client name must be unique to subject. find a way to ensure it is.
            JetStreamSubscription subscription = getJetStream().subscribe(subject, pullSubscribeOptions);
            subscriptionMap.put(subscriptionKey, subscription);
        }

        return subscriptionMap.get(subscriptionKey);
    }

//    /**
//
//     *
//     * @param connectionUri the custom URL to conect to
//     * @return a MessageQueueClient capable of interacting with nats.
//     * @throws IOException if a networking issue occurs
//     * @throws InterruptedException if the current thread is interrupted
//     */
//    public static NatsJetstreamMessageClient getInstance(String connectionUri) throws IOException, InterruptedException {
//        if (natsJetstreamMessageClient == null) {
//            natsJetstreamMessageClient = new NatsJetstreamMessageClient(connectionUri);
//        }
//
//        return natsJetstreamMessageClient;
//    }

    /**
     * Publishes a message onto the given subject
     *
     * @param stream the stream to which to push the message
     * @param subject    name of subject to which the message will be pushed
     * @param body  the message content to persist
     * @throws MessagingException if communication with Nats fails
     */
    @Override
    public void push(String stream, String subject, String body) throws MessagingException {
        // create a typical NATS message
        io.nats.client.Message natsMessage = NatsMessage.builder()
                .subject(subject)
                .data(body.getBytes(StandardCharsets.UTF_8))
                .build();

        PublishOptions pubishOptions = PublishOptions.builder()
                .expectedStream(stream)
                .build();

        // Publish a message and print the results of the publish acknowledgement.
        // An exception will be thrown if there is a failure.
        try {
            PublishAck pa = getJetStream().publish(natsMessage, pubishOptions);

            if (pa.isDuplicate()) {
                throw new DuplicateMessageException("Message already exists in NATS stream");
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("NatsJetstreamMessage{" +
                            "stream='" + stream + "'" +
                            ", subject=" + subject +
                            ", seq=" + pa.getSeqno() +
                            "}");
                }
            }
        } catch (JetStreamApiException e) {
            throw new MessagingException("Invalid message body response returned from NATS.", e);
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while pushing message to NATS server.", e);
        }
    }

    /**
     * Publishes a message onto the given subject with a delayed execution
     *
     * @param stream    name of stream
     * @param subject  subject to filter for messages
     * @param body        the message body to persist
     * @param secondsToDelay the number of seconds to delay the message from being processed
     * @throws MessagingException if communication with the subject fails
     */
    @Override
    public void push(String stream, String subject, String body, int secondsToDelay) throws MessagingException {
        log.debug("Delayed messages are not supported by NATS JetStream. Message will be added immediately");
        push(stream, subject, body);
    }

    /**
     * Returns a message back to the stream for consumption by a different process.
     *
     * @param stream    name of stream
     * @param subject  subject to filter for messages. This is ignored for this client
     * @param messageId the unique id of the message to reject
     * @param message   the body of the message to reject. This is ignored for this client
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
    public void reject(String stream, String subject, Object messageId, String message) throws MessagingException {
        log.info("Message rejection is not supported. The same message will instead be pushed back to the stream.");
        push(stream, subject, message);
    }

    /**
     * Deletes a message from the stream.
     *
     * @param stream    name of stream
     * @param subject  subject to filter for messages
     * @param messageId the unique id of the message
     * @throws MessagingException if communication with the NATS server fails or delete fails
     */
    @Override
    public void delete(String stream, String subject, Object messageId) throws MessagingException {
        try {
            if (!getJetStreamManagement().deleteMessage(stream, (long)messageId)) {
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
     * @param stream    name of stream
     * @param subject  subject to filter for messages
     * @return the next message with the given the subject on the given stream
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
    public Message pop(String stream, String subject) throws MessagingException {
        return pop(stream, subject, 1).stream().findFirst().orElse(null);
    }

    /**
     * Gets the next count events from the stream with the given subject. The distribution of messages is controlled by
     * Jetstream. This will simply pull the next {@code numberOfMessages} messages.
     *
     * @param stream    name of stream
     * @param subject  subject to filter for messages
     * @param numberOfMessages    the number of messages to pull from the stream
     * @return the next message in the stream matching the given subject
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
    public List<Message> pop(String stream, String subject, int numberOfMessages) throws MessagingException {
        return fetch(stream, subject, numberOfMessages, DEFAULT_TIMEOUT_SECONDS * numberOfMessages);
    }

    /**
     * Gets the next {@code count} events from the {@code stream} matching subject {@code subject}. The distribution
     * of messages is controlled by Jetstream. This will simply pull the next {@code numberOfMessages} messages and
     * timeout if it takes longer than {@code timeoutSeconds} seconds
     *
     * @param stream    name of stream
     * @param subject  subject to filter for messages
     * @param numberOfMessages    the number of messages to pull from the stream
     * @param timeoutSeconds the time in seconds to wait for the fetch operation to timeout
     * @return list of at most {@code numberOfMessages} messages
     * @throws MessagingException if communication with the NATS server fails or syntax is invalid
     */
    public List<Message> fetch(String stream, String subject, int numberOfMessages, int timeoutSeconds) throws MessagingException {
        try {
            JetStreamSubscription subscription = getOrCeateSubscription(stream, subject);
            return subscription.fetch(numberOfMessages, Duration.ofSeconds(timeoutSeconds)).stream()
                    .map(message -> {
                        // have to explicitly ack when fetching from a stream
                        message.ack();
                        return new Message(message.getSID(), new String(message.getData()));
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new MessagingException("Unexpected error connecting to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Error returned from NATS server fetching " + numberOfMessages + " messages from " + stream +
                    " with subject " + subject, e);
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
    @Override
    public boolean touch(Object messageId, String stream) throws MessagingException {
        log.info("Touching is not supported by NATS JetStream. Ignoring request to touch message " + messageId);
        return true;
    }

    /**
     * Starts an async listener to process messages with the given subject on the named stream. Listener will continue
     * to run until {@link MessageQueueListener#stop()} is called.
     *
     * @param stream name of the stream
     * @param subject    name of subject
     * @param listener the listener with which to listen
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
    public void listen(String stream, String subject, MessageQueueListener listener) throws MessagingException, MessageProcessingException {
        MessageHandler handler = msg -> {
            try {
                // Q: should we handle the ack after processing the message so we can retry?
                // Current thinking is no since any failure should result in an error message being written instead
                // of automatically retrying the message.
                msg.ack();
                listener.processMessage(new String(msg.getData()));
            } catch (MessageProcessingException e) {
                log.error("Failed to handle message from stream " + stream + " with subject " + msg.getSubject());
                // no need to Message.nck() the message since it will have been acknowledged up front.
                // any exception here is too late and should result in an error message being written.
            }
        };

        try {
            PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder()
                    .stream(stream)
                    .build();
            getJetStream().subscribe(subject, getDispatcher(), handler, true, pushSubscribeOptions);
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while subscribing to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Invalid request made to NATS Jetstream to subscribe for push messages", e);
        }
    }


    /**
     * Forces all consumers to drain and the connection to close.
     */
    @Override
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

    private String getStreamName() {
        return "AGAVE";
    }

    /**
     * Returns a list of all the queues available in the underlying
     * service. This may not be available uniformly.
     *
     * @return List of all the available streams on the server
     * @throws MessagingException      if communication with the NATS server fails
     * @throws NotImplementedException if the underlying service does not support messaging
     */
    @Override
    public List<Object> listQueues() throws MessagingException, NotImplementedException {
        try {
            return Collections.singletonList(getJetStreamManagement().getStreams());
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while pushing message to NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Invalid request made to NATS Jetstream to fetch streams", e);
        }
    }

    /**
     * Returns a list of the names of all the queues available in the underlying
     * service. This may not be available uniformly.
     *
     * @return List of the available stream names
     * @throws MessagingException      if communication with the NATS server fails
     * @throws NotImplementedException if the underlying service does not support messaging
     */
    @Override
    public List<String> listQueueNames() throws MessagingException, NotImplementedException {
        try {
            return getJetStreamManagement().getStreamNames();
        } catch (IOException e) {
            throw new MessagingException("Unexpected error while listing streams NATS server.", e);
        } catch (JetStreamApiException e) {
            throw new MessagingException("Invalid request made to NATS Jetstream to fetch stream names", e);
        }
    }

    /**
     * Returns true if a stream exists matching the given name.
     *
     * @param stream the name of the stream
     * @return true if the stream exists, false otherwise
     * @throws MessagingException if communication with the NATS server fails
     * @see #listQueueNames()
     */
    @Override
    public boolean queueExist(String stream) throws MessagingException {
        return listQueueNames().contains(stream);
    }

    /**
     * Find a stream whose name matches the given regular expression. There is no guarantee
     * that this will be the best fit match, just that it will match.
     *
     * @param regex the regular expression used to match the subject name
     * @return name of subject matching the regex
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
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
     * @param stream the name of the stream
     * @param messageId the unique id of the message
     * @return true if a message with the given ID exists. False otherwise.
     * @throws MessagingException if communication with the NATS server fails
     */
    @Override
    public boolean messageExist(String stream, Object messageId) throws MessagingException {
        try {
            return (getJetStreamManagement().getMessage(stream, (long)messageId) != null);

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
     * Identical to {@link #pop(String, String)}, but waits {@code timeoutSeconds} to receive a reserved message.
     *
     * @param stream name of the stream
     * @param subject    name of subject
     * @param timeoutSeconds  the number of seconds to wait before the reservation times out
     * @return Successfully reserved message
     * @throws MessagingException if a message is not received during timeoutSeconds or the operation fails
     */
    @Override
    public Message reserve(String stream, String subject, int timeoutSeconds) throws MessagingException {
        List<Message> messages = fetch(stream, subject, 1, timeoutSeconds);
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

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }
}

/**
 * Generic connection listener for NATS connections
 */
class NatsConnectionListener implements ConnectionListener {
    private static final Logger log = LoggerFactory.getLogger(NatsConnectionListener.class);

    /**
     * Connection related events that occur asynchronously in the client code are
     * sent to a ConnectionListener via a single method. The ConnectionListener can
     * use the event type to decide what to do about the problem.
     *
     * @param conn the connection associated with the error
     * @param type the type of event that has occurred
     */
    @Override
    public void connectionEvent(Connection conn, io.nats.client.ConnectionListener.Events type) {
        if (log.isDebugEnabled()) {
            log.debug("NATS server event: " + type.toString());
            if (type == io.nats.client.ConnectionListener.Events.DISCOVERED_SERVERS) {
                log.debug("Known servers: " + conn.getServers().stream().collect(Collectors.joining(",")));
            }
        }
    }
}

/**
 * Generic error listener for NATS connections
 */
class NatsErrorListener implements ErrorListener {
    private static final Logger log = LoggerFactory.getLogger(NatsErrorListener.class);

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        log.info("NATS connection slow consumer detected");
    }
    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        log.info("NATS connection exception occurred");
        log.debug(exp.getMessage());
    }
    @Override
    public void errorOccurred(Connection conn, String error) {
        log.info("NATS connection error occurred: " + error);
    }
}
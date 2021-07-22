package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.TransferTaskConfigProperties;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.messaging.NatsConnectionListener;
import org.agaveplatform.service.transfers.messaging.NatsErrorListener;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.util.Slug;
import org.iplantc.service.common.uuid.UUIDType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public class AbstractNatsListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractNatsListener.class);
    /**
     * Connection to NATS server. Should use value from properties insteadd
     * @deprecated
     * @see TransferTaskConfigProperties#NATS_URL
     */
    protected static final String CONNECTION = "nats://nats:4222";
    protected NatsJetstreamMessageClient messageClient;
    protected String streamName;
    protected String consumerName;

    public AbstractNatsListener(Vertx vertx) throws IOException, InterruptedException {
        this(vertx, null);
    }
    public AbstractNatsListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        this();
        setVertx(vertx);
        setEventChannel(eventChannel);
        setStreamName("AGAVE_" + config().getString(TransferTaskConfigProperties.AGAVE_ENVIRONMENT,"DEV"));
    }
    public AbstractNatsListener() throws IOException, InterruptedException {
        super();
        setStreamName("AGAVE_" + config().getString(TransferTaskConfigProperties.AGAVE_ENVIRONMENT,"DEV"));
    }


    public String getConnection(){
        return CONNECTION;
    }

    @Override
    public String getDefaultEventChannel() {
        return null;
    }

    /**
     * The stream name to set. Present for convenience.
     * @param streamName The stream name to set
     */
    private void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    /**
     * Constructs the stream name based on the AGAVE_ENVIRONMENT variable from the service config. Defaults
     * to "AGAVE_DEV"
     * @return the name of the stream to which this verticle will subscribe
     */
    protected String getStreamName() {
        return streamName;
    }

    /**
     * Gets an instance of the message client to use for messaging by this verticle
     * @return an instance of this message client, namespaced by the class name as consumer base name
     * @throws IOException if a networking issue occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    protected NatsJetstreamMessageClient getMessageClient() throws IOException, InterruptedException {
        if (this.messageClient == null) {
            if (getStreamName().isEmpty()) {
                Map<String, String> bashEnv = System.getenv();
                String streamName = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","AGAVE_DEV");
                setStreamName(streamName);
            }
            this.messageClient = new NatsJetstreamMessageClient(
                    config().getString(TransferTaskConfigProperties.NATS_URL, CONNECTION),
                    getStreamName(),
                    this.getClass().getSimpleName());
            
            this.messageClient.getOrCreateStream(UUIDType.TRANSFER.name().toLowerCase() + ".>" );
        }
        return this.messageClient;
    }

    /**
     * Creates a subject using the context of the event being sent. All values in this subject should be concrete
     * when pushing a message. When subscribing, they should leverage wildcards to the extent that a verticle serves
     * more than one tenant, owner, etc.
     * @param tenantId the id of the tenant
     * @param owner the subject to whom this message is attributed
     * @param sourceSystemId the id of the source system of the transfer event. TODO: remove this and disambiguate the system id in message subjects
     * @param eventName the event being thrown.
     * @return the derived subject with built in routing for downstream consumers.
     */
    public String createPushMessageSubject(String tenantId, String owner, String sourceSystemId, String eventName){
        String consumerName = "";
        try {
            consumerName = UUIDType.TRANSFER.name().toLowerCase() + "." + tenantId + "." + owner + "." + sourceSystemId + "." + eventName;
            consumerName = consumerName.replaceAll("\\.{2,}", ".");
            consumerName = StringUtils.stripEnd(consumerName, ".");
        } catch (Exception e) {
            return "";
        }
        return consumerName;
    }

    public String createConsumerName(String eventName, Class clazz){
        String consumerName = clazz.getSimpleName() + "-" + eventName;
        return Slug.toSlug(consumerName);
    }

    public void setConsumerName(String consumerName){
        this.consumerName = consumerName;
    }

    public String getConsumerName(){
        return this.consumerName;
    }
    /**
     * Convenience method to creates a subscription subject using the context of the event being sent. The
     * resource type will be {@link UUIDType#TRANSFER}, tenant, owner, and system will all be wildcards, "*".
     *
     * @param eventName the event being thrown.
     * @return the derived subject with built in routing for downstream consumers.
     * @see #createSubscriptionSubject(String, String, String, String, String)
     */
    public String createSubscriptionSubject(String eventName) {
        return createSubscriptionSubject(UUIDType.TRANSFER.name().toLowerCase(), null, null, null, eventName);
    }

    /**
     * Creates a subject using the context of the event being sent. All values in this subject should be concrete
     * when pushing a message. When subscribing, they should leverage wildcards to the extent that a verticle serves
     * more than one tenant, owner, etc.
     * @param agaveResourceType the type of agave resource for which this message is being created. ie. {@link UUIDType}
     * @param tenantId the id of the tenant
     * @param owner the subject to whom this message is attributed
     * @param sourceSystemId the id of the source system of the transfer event. TODO: remove this and disambiguate the system id in message subjects
     * @param eventName the event being thrown.
     * @return the derived subject with built in routing for downstream consumers.
     */
    public String createSubscriptionSubject(String agaveResourceType, String tenantId, String owner, String sourceSystemId, String eventName) {
        String subscriptionSubject = String.format("%s.%s.%s.%s.%s",
                                        agaveResourceType,
                                        StringUtils.isBlank(tenantId) ? "*" : tenantId,
                                        StringUtils.isBlank(owner) ? "*" : owner,
                                        StringUtils.isBlank(sourceSystemId) ? "*" : sourceSystemId,
                                        StringUtils.isBlank(eventName) ? "*" : eventName);

        subscriptionSubject = subscriptionSubject.replaceAll("\\.{2,}", ".");
        subscriptionSubject = StringUtils.strip(subscriptionSubject, ". ");
        //consumerName = Slug.toSlug(consumerName);
        return subscriptionSubject;
    }

    /**
     * Listen to the stream for push messages, knowing ony one member of the group will get the message.
     * @param messageType the type of message to send. This corresponds to a static MessageType value
     * @param callback the async handler to be resolved with an {@link Message} after receiving from the NATS stream.
     * @throws MessagingException if communication with the NATS server fails
     * @throws InterruptedException if the current thread is interrupted
     * @throws IOException if unable to connect to the NATS server
     */
    protected void subscribeToSubjectGroup(String messageType, Handler<Message> callback) throws IOException, InterruptedException, MessagingException {
        String conName = createConsumerName(messageType, this.getClass());  // returns "transfers.*.*.*.'EVENT_CHANEL'"
        String subject = createSubscriptionSubject("transfer","","","", messageType);
        String groupName = Slug.toSlug(this.getClass().getSimpleName() + "-" + messageType); // returns "transfertaskcreatedlistener-transfertask_created"

        getMessageClient().listen(subject, groupName, msg -> {

            // Q: should we handle the ack after processing the message so we can retry?
            // Current thinking is no since any failure should result in an error message being written instead
            // of automatically retrying the message.
            // A: I say yes.  We already have an error mechanism in vertx.  This would push the error and retry to the
            // Nats server.  Do we really want error handling in two places?  The Nats server has accomplished it task by
            // delivering the message regardless of how it is processed, or not processed.  ~Eric
            msg.ack();

            String body = new String(msg.getData(), StandardCharsets.UTF_8);
            callback.handle(new Message(msg.getSID(), body));
        });
    }

    /**
     * Listen to the stream for push messages, knowing it won't be the only one to get them.
     * @param messageType the type of message to send. This corresponds to a static MessageType value
     * @param callback the async handler to be resolved with an {@link Message} after receiving from the NATS stream.
     * @throws MessagingException if communication with the NATS server fails
     * @throws InterruptedException if the current thread is interrupted
     * @throws IOException if unable to connect to the NATS server
     */
    protected void subscribeToSubject(String messageType, Handler<Message> callback) throws IOException, InterruptedException, MessagingException {
        String subject = createSubscriptionSubject(messageType);

        getMessageClient().listen(subject, msg -> {

            // Q: should we handle the ack after processing the message so we can retry?
            // Current thinking is no since any failure should result in an error message being written instead
            // of automatically retrying the message.
            msg.ack();

            String body = new String(msg.getData(), StandardCharsets.UTF_8);
            callback.handle(new Message(msg.getSID(), body));
        });

    }

    /**
     * Handles event creation and delivery onto the NATS stream. Retry will be attempted within the messaging client.
     * The call is made asynchronously, so this method will returns almost immediately save for communication latency.
     *
     * @param eventName the name of the event. This doubles as the address in the request invocation.
     * @param body the message of the body. Currently only {@link JsonObject} are supported.
     */
    @Override
    public void _doPublishEvent(String eventName, JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        log.trace(this.getClass().getName() + ": _doPublishEvent({}, {})", eventName, body);
        try {
            URI target = null;
            String host = null;

            try { target = URI.create(body.getString("dest")); } catch (Exception ignored) {}

            if (target == null || target.getScheme().equalsIgnoreCase("file")) {
                try { target = URI.create(body.getString("source")); } catch (Exception ignored) {}
            }

            if (target == null || target.getScheme().equalsIgnoreCase("file")) {
                host = "internal";
            }
            else {
                host = target.getHost();
            }

            String subject = createPushMessageSubject(
                    body.getString("tenant_id"),
                    body.getString("owner", "agaveops"),
                    host,
                    eventName);

            getMessageClient().push(subject, body.toString());

            if (handler != null) {
                handler.handle(Future.succeededFuture(true));
            }
        } catch (Exception e) {
            log.error("Error with _doPublishEvent:  {}", e.getMessage(), e);
            if (handler != null) {
                handler.handle(Future.failedFuture(e));
            }
        }
    }

    /**
     * Creates a connection to the remote NATS server.
     * @param url the NATS server url
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @deprecated
     * @see NatsJetstreamMessageClient
     */
    public Connection _connect(String url) throws IOException, InterruptedException {
        Options.Builder builder = new Options.Builder()
                .server(url)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1)
                .connectionListener(new NatsConnectionListener())
                .errorListener(new NatsErrorListener());

        return Nats.connect(builder.build());
    }

    /**
     * If your verticle has simple synchronous clean-up tasks to complete then override this method and put your clean-up
     * code in here.
     *
     * @throws Exception when parent is unable to stop.
     */
    @Override
    public void stop() throws Exception {

        getMessageClient().stop();

        super.stop();
    }

    /**
     * Handles {@link MessageType#TRANSFERTASK_CANCELED_SYNC} messages by adding the task id to the list of tasks
     * being cancelled in this verticle.
     *
     * @param message the message to cancel
     */
    protected void handleCanceledSyncMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");

            log.info("Transfer task {} cancellation detected", uuid);
            if (uuid != null) {
                addCancelledTask(uuid);
            }
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing {} message {} body {}. {}",
                    MessageType.TRANSFERTASK_CANCELED_SYNC, message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Handles {@link MessageType#TRANSFERTASK_PAUSED_SYNC} messages by adding the task id to the list of tasks
     * being paused in this verticle.
     *
     * @param message the message to cancel
     */
    protected void handlePausedSyncMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused detected", uuid);
            if (uuid != null) {
                addPausedTask(uuid);
            }
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing {} message {} body {}. {}",
                    MessageType.TRANSFERTASK_PAUSED_SYNC, message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Handles {@link MessageType#TRANSFERTASK_PAUSED_COMPLETED} messages by removing the task id from the list of tasks
     * being paused in this verticle.
     *
     * @param message the message to cancel
     */
    protected void handlePausedCompletedMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused completed", uuid);
            if (uuid != null) {
                removePausedTask(uuid);
            }
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing {} message {} body {}. {}",
                    MessageType.TRANSFERTASK_PAUSED_COMPLETED, message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Handles {@link MessageType#TRANSFERTASK_CANCELED_COMPLETED} messages by removing the task id from the list of tasks
     * being canceled in this verticle.
     *
     * @param message the message to cancel
     */
    protected void handleCanceledCompletedMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");

            log.info("Transfer task {} canceled completed", uuid);
            if (uuid != null) {
                removeCancelledTask(uuid);
            }
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing {} message {} body {}. {}",
                    MessageType.TRANSFERTASK_CANCELED_COMPLETED, message.getId(), message.getMessage(), t.getMessage());
        }
    }
}

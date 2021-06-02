package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.TransferTaskConfigProperties;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.messaging.NatsConnectionListener;
import org.agaveplatform.service.transfers.messaging.NatsErrorListener;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.util.Slug;
import org.iplantc.service.common.uuid.UUIDType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_FAILED;

public class AbstractNatsListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractNatsListener.class);
    /**
     * Connection to NATS server. Should use value from properties insteadd
     * @deprecated
     * @see TransferTaskConfigProperties#NATS_URL
     */
    private static final String CONNECTION = "nats://nats:4222";
    protected NatsJetstreamMessageClient messageClient;
    protected String streamName;

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
        if (streamName.isEmpty()){

        }
        return streamName;
    }

    /**
     * Gets an instance of the message client to use for messaging by this verticle
     * @return an instance of this message client, namespaced by the class name as consumer base name
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
        }

        return this.messageClient;
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
    public String createPushMessageSubject(String agaveResourceType, String tenantId, String owner, String sourceSystemId, String eventName){
        String consumerName = "";
        try {
            consumerName = getStreamName() + "_" + agaveResourceType + "_" + tenantId + "_" + owner + "_" + sourceSystemId + "_" + eventName;
            consumerName = consumerName.replaceAll("\\.{2,}", ".");
            consumerName = StringUtils.stripEnd(consumerName, ".");

        }catch (Exception e){
            return "";
        }
        return consumerName;
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
        String consumerName = String.format("%s.%s.%s.%s.%s",
                                        agaveResourceType,
                                        StringUtils.isBlank(tenantId) ? "*" : tenantId,
                                        StringUtils.isBlank(owner) ? "*" : owner,
                                        StringUtils.isBlank(sourceSystemId) ? "*" : sourceSystemId,
                                        StringUtils.isBlank(eventName) ? "*" : eventName);

        consumerName = consumerName.replaceAll("\\.{2,}", ".");
        consumerName = StringUtils.strip(consumerName, ". ");
        //consumerName = Slug.toSlug(consumerName);
        return consumerName;
    }

    /**
     * Listen to the stream for push messages, knowing ony one member of the group will get the message.
     * @param messageType
     * @param callback
     * @throws IOException
     * @throws InterruptedException
     * @throws MessagingException
     * @throws MessageProcessingException
     */
    protected void subscribeToSubjectGroup(String messageType, Handler<Message> callback)
            throws IOException, InterruptedException, MessagingException, MessageProcessingException, JetStreamApiException {
        String subject = createSubscriptionSubject(messageType);  // returns "transfers.*.*.*.'EVENT_CHANEL'"
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
     * @param messageType
     * @param callback
     * @throws IOException
     * @throws InterruptedException
     * @throws MessagingException
     * @throws MessageProcessingException
     */
    protected void subscribeToBroadcastSubject(String messageType, Handler<Message> callback)
            throws IOException, InterruptedException, MessagingException, MessageProcessingException, JetStreamApiException {
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
     * Handles event creation and delivery across the existing event bus. Retry is handled by the
     * {@link RetryRequestManager} up to 3 times. The call will be made asynchronously, so this method
     * will return immediately.
     *
     * @param eventName the name of the event. This doubles as the address in the request invocation.
     * @param body the message of the body. Currently only {@link JsonObject} are supported.
     */
    public void _doPublishEvent(String eventName, JsonObject body) throws IOException, InterruptedException {
        _doPublishNatsJSEvent(eventName, body);
    }

    /**
     * Convenience method to handles generation of failed transfer messages, raising of failed event, and calling of handler with the
     * passed exception.
     * @param throwable the exception that was thrown
     * @param failureMessage the human readable message to send back
     * @param originalMessageBody the body of the original message that caused that failed
     * @param handler the callback to pass a {@link Future#failedFuture(Throwable)} with the {@code throwable}
     */
    protected void doHandleFailure(Throwable throwable, String failureMessage, JsonObject originalMessageBody, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        JsonObject json = new JsonObject()
                .put("cause", throwable.getClass().getName())
                .put("message", failureMessage)
                .mergeIn(originalMessageBody);

        _doPublishNatsJSEvent(TRANSFER_FAILED, json);

        // propagate the exception back to the calling method
        if (handler != null) {
            handler.handle(Future.failedFuture(throwable));
        }
    }

    /**
     * Convenience method to handles generation of errored out transfer messages, raising of error event, and calling of handler with the
     * passed exception.
     * @param throwable the exception that was thrown
     * @param failureMessage the human readable message to send back
     * @param originalMessageBody the body of the original message that caused that failed
     * @param handler the callback to pass a {@link Future#failedFuture(Throwable)} with the {@code throwable}
     */
    protected void doHandleError(Throwable throwable, String failureMessage, JsonObject originalMessageBody, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        JsonObject json = new JsonObject()
                .put("cause", throwable.getClass().getName())
                .put("message", failureMessage)
                .mergeIn(originalMessageBody);

        _doPublishNatsJSEvent(TRANSFERTASK_ERROR, json);

        // propagate the exception back to the calling method
        if (handler != null) {
            handler.handle(Future.failedFuture(throwable));
        }
    }

    /**
     *    This will publish a message @body on the messageAddress
     *    @param messageAddress - Address of the Nats message
     *    @param body - body of the message
     *    @return void
     */
    public void _doPublishNatsJSEvent(String messageAddress, JsonObject body) {
        log.info(this.getClass().getName() + ": _doPublishNatsEvent({}, {})", messageAddress, body);
        try {
            getMessageClient().push(messageAddress, body.toString());

        } catch (IOException | MessagingException | InterruptedException e) {
            log.debug("Error with _doPublishNatsJSEvent:  {}", e.getMessage());
        }
    }

    /**
     * @param url
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @deprecated
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
}

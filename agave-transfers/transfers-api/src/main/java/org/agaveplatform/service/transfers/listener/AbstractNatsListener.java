package org.agaveplatform.service.transfers.listener;

import io.nats.client.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.TransferTaskConfigProperties;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.uuid.UUIDType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

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
        return streamName;
    }

    /**
     * Gets an instance of the message client to use for messaging by this verticle
     * @return an instance of this message client, namespaced by the class name as consumer base name
     */
    protected NatsJetstreamMessageClient getMessageClient() throws IOException, InterruptedException {
        if (this.messageClient == null) {
            this.messageClient = new NatsJetstreamMessageClient(
                    config().getString(TransferTaskConfigProperties.NATS_URL),
                    getStreamName(),
                    this.getClass().getSimpleName());
        }

        return this.messageClient;
    }

    /**
     * Creates a subject using the context of the event being sent. All values in this subject should be concrete
     * when pushing a message. When subscribing, they should leverage wildcards to the extent that a verticle serves
     * more than one tenant, owner, etc.
     * @param streamName nam of stream. redundant. TODO: remove stream name from subject
     * @param agaveResourceType the type of agave resource for which this message is being created. ie. {@link UUIDType}
     * @param tenantId the id of the tenant
     * @param owner the subject to whom this message is attributed
     * @param sourceSystemId the id of the source system of the transfer event. TODO: remove this and disambiguate the system id in message subjects
     * @param eventName the event being thrown.
     * @return the derived subject with built in routing for downstream consumers.
     */
    public String createPushMessageSubject(String streamName, String agaveResourceType, String tenantId, String owner, String sourceSystemId, String eventName){
        String consumerName = "";
        try {
            consumerName = streamName + "_" + agaveResourceType + "_" + tenantId + "_" + owner + "_" + sourceSystemId + "_" + eventName;
            consumerName = consumerName.replaceAll("\\.{2,}", ".");
            consumerName = StringUtils.stripEnd(consumerName, ".");

        }catch (Exception e){
            return "";
        }
        return consumerName;
    }

    /**
     *    This will publish a message @body on the messageAddress
     *    @param messageAddress - Address of the Nats message
     *    @param body - body of the message
     *    @return void
     */
    public void _doPublishNatsJSEvent(String messageAddress, JsonObject body) throws IOException, InterruptedException, TimeoutException {
        log.info(super.getClass().getName() + ": _doPublishNatsEvent({}, {})", messageAddress, body);
        try {
            getMessageClient().push(getStreamName(), messageAddress, body.toString());

        } catch (IOException | MessagingException e) {
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
                .server("nats://nats:4222")
//                .server(Options.DEFAULT_URL)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1);
        builder = builder.connectionListener((conn, type) -> log.info("Status change {}", type));
        builder = builder.errorListener(new ErrorListener() {
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
                log.info("NATS connection error occurred " + error);
            }
        });

        log.debug("_connect");
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

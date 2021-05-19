package org.agaveplatform.service.transfers.listener;

import com.github.slugify.Slugify;
import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.util.Slug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AbstractNatsListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractNatsListener.class);
    //public final Connection nc = _connect();
    public String CONNECTION_URL = "nats://nats:4222";

    public AbstractNatsListener(Vertx vertx) throws IOException, InterruptedException {
        this(vertx, null);
    }
    public AbstractNatsListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        this();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }
    public AbstractNatsListener() throws IOException, InterruptedException {
            super();
        }

    @Override
    public String getDefaultEventChannel() {
        return null;
    }

    /*
    *    This will create a message name. It takes the following paramaters
    *    @param streamName
    *    @param tenantId - ID of the tenant
    *    @param eventName - this is the MessageType
    *    @return name
    *    The format for the messages is as follows:
    *        agaveType - the inital setting is "transfers" but other systems can use it, i.e. "files", "jobs"
    *        tenantId - the ID of the tenant
    *        uId - this it the owner of the request.  It can be found in the owner field of the TransferTask
    *        systemId - this is the system id for the source
    *        eventName - this is the MessgeType enumeration  NOTE this is two fields in the message string.
    */
    public String _createMessageName(String streamName, String tenantid, String uid, String systemId, String eventName){
        //transfers.$tenantid.transfer.$protocol
        String prefix = _getStreamPrefix();
        eventName = StringUtils.replaceChars(eventName,"_",".");
        String name = prefix + "." + streamName + "." + tenantid + "." + eventName;
        name = name.replaceAll("\\.{2,}",".");
        name = StringUtils.stripEnd(name, ".");

        name = Arrays.stream(name.split(".")).map(t -> Slug.toSlug(t)).collect(Collectors.joining("."));

        return name + "." + tenantid + "." + uid + "." + systemId + "." + eventName;
    }

    /**
     * This method will create the consumer name from the implementing class simple name, and provided {@code eventName}.
     *
     * @param eventName - this is the MessageType, which is enough for uniqueness. The subject is not needed
     * @return a unique consumerName for the event and implementing class
     */
    public String createConsumerNameForEvent(String eventName){
        return Slug.toSlug(String.format("%s-%s", this.getClass().getSimpleName(), eventName), true);

        //Slugify slg = new Slugify();
//        String consumerName = "";
//        String prefix = _getStreamPrefix();
//
//        consumerName = prefix + "." + streamName + "."+ eventName;
//        consumerName = consumerName.replaceAll("\\.{2,}",".");
//        consumerName = StringUtils.stripEnd(consumerName, ".");
//
//        AgaveUUID uuid = null;
//        try {
//            uuid = new AgaveUUID(UUIDType.SYSTEM.toString());
//        } catch (UUIDException e) {
//            log.debug(e.getMessage());
//        }
//        consumerName = consumerName + uuid.toString();
//
//        return consumerName;
    }

    /*
     *    This will create a NATS consumer. It takes the following paramaters
     *      @param jsm (JetStreamManagement
     *    @param stream - name of the stream
     *    @param consumerName name of the Nats Consumer
     *    @param subject - this is the fully qualified message address.  Can have wildcards.
     *    @return ConsumerInfo
     */
    private ConsumerInfo _createConsumer(JetStreamManagement jsm, String stream, String consumerName, String subject) throws JetStreamApiException, IOException {
        log.info("Configure And Add A Consumer {}: {}", consumerName, subject);
        ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder()
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofDays(-1))
                .durable(consumerName)
                .filterSubject(subject)
                .deliverPolicy(DeliverPolicy.All)
                .maxDeliver(-1)
                .rateLimit(-1)
                .replayPolicy(ReplayPolicy.Instant)
                .build();

        ConsumerInfo consumerInfo = jsm.addOrUpdateConsumer(stream, consumerConfiguration);
        log.info("Created Consumer Name: {} Desc: {}", consumerInfo.getStreamName(), consumerInfo.getDescription());
        return consumerInfo;
    }

    /*
     *    This will check if a consumer exists
     *    @param jsm (JetStreamManagement)
     *    @param stream - Name of the stream
     *    @param consumer - Nname of the Nats Consumer
     *    @return boolean
     */
    public boolean _checkConsumer(JetStreamManagement jsm, String stream, String consumer) throws JetStreamApiException, IOException {
        ConsumerInfo cI = jsm.getConsumerInfo(stream, consumer);

        return cI.getName() != null;
    }

    /*
     *    This will publish a message @body on the messageAddress
     *    @param messageAddress - Address of the Nats message
     *    @param body - body of the message
     *    @return void
     */
    public void _doPublishNatsJSEvent(String messageAddress, JsonObject body) throws IOException, InterruptedException, TimeoutException {
        log.info(super.getClass().getName() + ": _doPublishNatsEvent({}, {})", messageAddress, body);
        Connection nc;
        try{
            Options options = new Options.Builder().
                    server(CONNECTION_URL).
                    connectionTimeout(Duration.ofSeconds(30)). // Set timeout
                    build();
            nc = Nats.connect(options);
            //nc = _connect(CONNECTION_URL);

            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();
            String stream;
            if ( messageAddress.startsWith("notification")  ){
                stream = "NOTIFICATION";
            }else {
                stream = "TRANSFERTASK";
            }

            String consumerName="";
            // check to be sure the stream and the consumer exits.  If the consumer does not exist then first create it
            if (!_checkConsumer(jsm, stream, messageAddress)){
                String tenantid = body.getString("tenantId");
                String uid = body.getString("owner");
                AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(body.getString("source")));

                String systemid = srcUri.get().getHost();

                //transfers.$tenantid.$uid.$systemid.$messageAddress
                consumerName = createConsumerNameForEvent(messageAddress);

                _createConsumer(jsm, stream, consumerName, messageAddress);
            }

            // create a typical NATS message
            Message msg = NatsMessage.builder()
                    .subject(messageAddress)
                    .data(body.toString(), StandardCharsets.UTF_8)
                    .build();

            PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
                    .expectedStream(stream)
                    .messageId("latest");

//            nc.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8));
//            log.debug("Published using nc.publish to {}", messageAddress);
            // Publish a message and print the results of the publish acknowledgement.
            // An exception will be thrown if there is a failure.
            PublishAck pa = js.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8), pubOptsBuilder.build());
            log.debug("Published message to {} with a sequence no of {} and consumer name {}",pa.getStream(), pa.getSeqno(), consumerName);
            //nc.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8));
//            nc.flush(Duration.ofMillis(1000));
//            _closeConnection(nc);

        } catch (IOException | JetStreamApiException e) {
            log.debug("Error with _doPublishNatsJSEvent:  {}", e.getMessage());
            //nc = _connect();
        }
    }

    /*
    *       This will connect to Nats Jetstream
    *       @param url - url of the server
    *       @param stream
    */
    public JetStream _jsmConnect(String url){
        JetStream js = null;
        try {
            Connection nc = _connect(url);
                // Create our JetStream context to receive JetStream messages.
                js = nc.jetStream();

        } catch (IOException | InterruptedException e) {
            log.info(e.getMessage());
            log.info(e.getCause().toString());
        }
        return js;
    }

    /*
     *       This will connect to Nats
     *       It uses the CONNECTION_URL
     *       @returns Connection
     */
    public Connection _connect() throws IOException, InterruptedException {
        return _connect(CONNECTION_URL);
    }

    /*
     *      This will connect to Nats
     *      @param url - the url of the Nats server
     *      @returns Connection
     */
    public Connection _connect(String url) throws IOException, InterruptedException {
        Options.Builder builder = new Options.Builder()
                .server("nats://nats:4222")
//                .server(Options.DEFAULT_URL)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1)
                ;
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
        Connection nc = Nats.connect(builder.build());
        return nc;
    }

    /*
     *       This will disconnect from Nats
     *       @param nc - Connection to disconect from
     */
    public void _closeConnection(Connection nc){
        try {
            nc.close();
            log.debug("Nats closed connection {}", nc);
        } catch (InterruptedException e) {
            log.debug(e.getMessage());
        }
    }

    /**
     * This method will get the stream name from the {@code AGAVE_ENVIRONMENT} config value, or environment
     * variable. This will be a reflection of whether the service is running in dev or prod, but generally does not
     * matter too much since we will have a dedicated nats server per tenant/namespace.
     *
     * @return the name of the stream to which we will subscribe
     */
    public String getStreamName() {
        return "AGAVE_" + config().getString("AGAVE_ENVIRONMENT", "DEV").toUpperCase();
    }

    /*
     *       This will check to be sure the stream(s) are correct from Nats
     *       @param nc - Connection to disconect from
     *       @return boolean
     */
    public boolean _checkStreams(Connection nc) {
        //**********************************************************************************
        // Check to be sure all the streams are present
        //**********************************************************************************
        try {
            Map<String, String> bashEnv = System.getenv();
            String streamEnvironment = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","DEV");

            if (!streamExists(nc, streamEnvironment)) {
                log.info("Stopping program, stream does not exist: {}", streamEnvironment);
                return false;
            }else {
                String subjectPrefix = bashEnv.getOrDefault("AGAVE_MESSAGE_PREFIX","default.");
                subjectPrefix = subjectPrefix.replaceAll("\\.{2,}",".");
                subjectPrefix = StringUtils.stripEnd(subjectPrefix, ".");
                JetStreamManagement jsm = nc.jetStreamManagement();
                _createStream(jsm, streamEnvironment,  subjectPrefix + ".>");
            }
        } catch (JetStreamApiException | IOException e) {
            log.debug("TransferTaskCreatedListener - Error with subsription {}", e.getMessage());
        }
        return true;
    }

    /*
    *   This method returns the StreamInfo back from Nats
    *   @param jsm
    *   @param streamName
    *   @returns StreamInfo
     */
    public StreamInfo _getStreamInfo(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        try {
            return jsm.getStreamInfo(streamName);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getErrorCode() == 404) {
                return null;
            }
            throw jsae;
        }
    }

    /*
     *   This method returns the prefix for the address
     *   @returns String prefix
     *   This method pulls back the:
     *      envorment
     *      version
     *    and puts them into a string to be used by a Nats Jetstream stream.
     *  //$branch.$type.$owner.$systemid.transfer.protocol.sftp
     */
    public String _getStreamPrefix(){

        Slugify slg = new Slugify();
        String subjectPrefix ="";
        Map<String, String> bashEnv = System.getenv();
        String streamEnvironment = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","DEV");
        subjectPrefix = bashEnv.getOrDefault("AGAVE_MESSAGE_PREFIX","default");
        subjectPrefix = subjectPrefix.replaceAll("\\.{2,}",".");
        subjectPrefix = StringUtils.stripEnd(subjectPrefix, ".");
        // slugify each component
        subjectPrefix = Arrays.stream(subjectPrefix.split(".")).map(t -> Slug.toSlug(t)).collect(Collectors.joining("."));

        subjectPrefix = streamEnvironment + "." + subjectPrefix;
        return subjectPrefix;
    }

    /*
    *   Method that will create the stream in Nats
    *   @param jsm
    *   @param name - name of the stream
    *   @param subject - subject for that stream
     */
    private void _createStream(JetStreamManagement jsm, String name, String subject) {
        log.info("Configure And Add Streams.");
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(name)
                    .subjects(subject)
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .maxConsumers(-1)
                    .maxBytes(-1)
                    .maxAge(Duration.ofDays(30))
                    .maxMsgSize(-1)
                    .maxMessages(-1)
                    .storageType(StorageType.Memory)
                    .replicas(1)
                    .noAck(false)
                    // .template(...)
                    .discardPolicy(DiscardPolicy.Old)
                    .build();
            StreamInfo streamInfo = jsm.addStream(streamConfig);
            log.info("Create stream {}", jsm.getStreamInfo(streamConfig.getName()));
        }
        catch (JetStreamApiException | IOException e) {
            log.debug(e.getMessage());
        }
    }

    /*
    *   Method to return the streamInfo
    *   @param nc - Connection
    *   @param streamName
    *   @return boolean
     */
    public boolean streamExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        return _getStreamInfo(nc.jetStreamManagement(), streamName) != null;
    }

    /*
     *   Method to return the streamInfo
     *   @param jsm - JetStreamManagement
     *   @param streamName
     *   @return boolean
     */
    public boolean streamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        return _getStreamInfo(jsm, streamName) != null;
    }

}

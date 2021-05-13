package org.agaveplatform.service.transfers.listener;

import com.github.slugify.Slugify;
import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
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

import org.agaveplatform.service.transfers.nats.NatsArgs;
import org.agaveplatform.service.transfers.nats.NatsArgs.Builder;

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

    public String _createMessageName(String type, String tenantid, String uid, String systemid, String eventName){
        //transfers.$tenantid.$uid.$systemid.transfer.$protocol
        String prefix = _getStreamPrefix();
        eventName = StringUtils.replaceChars(eventName,"_",".");
        String name = prefix + "." + type + "." + tenantid + "." + uid + "." + systemid + "." + eventName;
        name = name.replaceAll("\\.{2,}",".");
        name = StringUtils.stripEnd(name, ".");
        Slugify slg = new Slugify();
        name = Arrays.stream(name.split(".")).map(t -> slg.slugify(t)).collect(Collectors.joining("."));

        return name;
    }
    public String _createConsumerName(String type, String tenantid, String uid, String systemid, String eventName){
        //Slugify slg = new Slugify();
        String consumerName = "";
        String prefix = _getStreamPrefix();

        consumerName = prefix + "." + type + "." + tenantid + "." + uid + "." + systemid + "." + eventName;
        consumerName = consumerName.replaceAll("\\.{2,}",".");
        consumerName = StringUtils.stripEnd(consumerName, ".");

        return consumerName;
    }
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
    public boolean _getConsumer(JetStreamManagement jsm, String stream, String consumer) throws JetStreamApiException, IOException {
        ConsumerInfo cI = jsm.getConsumerInfo(stream, consumer);

        if (cI.getName() != null){
            return true;
        } else {
            return false;
        }
    }

    public void _doPublishNatsJSEvent(String eventName, JsonObject body) throws IOException, InterruptedException, TimeoutException {
        log.info(super.getClass().getName() + ": _doPublishNatsEvent({}, {})", eventName, body);
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
            if ( eventName.startsWith("notification")  ){
                stream = "NOTIFICATION";
            }else {
                stream = "TRANSFERTASK";
            }

            String consumerName="";
            // check to be sure the stream and the consumer exits.  If the consumer does not exist then first create it
            if (!_getConsumer(jsm, stream, eventName)){
                String tenantid = body.getString("tenantId");
                String uid = body.getString("owner");
                AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(body.getString("source")));

                String systemid = srcUri.get().getHost();
//$branch.$type.$owner.$systemid.transfer.protocol.sftp
                consumerName = _createConsumerName( stream, tenantid, uid, systemid, eventName);
                _createConsumer(jsm, stream, consumerName, eventName);
            }

            // create a typical NATS message
            Message msg = NatsMessage.builder()
                    .subject(eventName)
                    .data(body.toString(), StandardCharsets.UTF_8)
                    .build();

            PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
                    .expectedStream(stream)
                    .messageId("latest");

//            nc.publish(eventName, body.toString().getBytes(StandardCharsets.UTF_8));
//            log.debug("Published using nc.publish to {}", eventName);
            // Publish a message and print the results of the publish acknowledgement.
            // An exception will be thrown if there is a failure.
            PublishAck pa = js.publish(eventName, body.toString().getBytes(StandardCharsets.UTF_8), pubOptsBuilder.build());
            log.debug("Published message to {} with a sequence no of {} and consumer name {}",pa.getStream(), pa.getSeqno(), consumerName);
            //nc.publish(eventName, body.toString().getBytes(StandardCharsets.UTF_8));
//            nc.flush(Duration.ofMillis(1000));
//            _closeConnection(nc);

        } catch (IOException | JetStreamApiException e) {
            log.debug("Error with _doPublishNatsJSEvent:  {}", e.getMessage());
            //nc = _connect();
        }
    }

//    public void _doPublishNatsJSEvent(String stream, String subject, JsonObject body) {
//        log.info(super.getClass().getName() + ": _doPublishNatsJSEvent({}, {}, {})", stream, subject, body);
//
//        try {
//            JetStream js = _jsmConnect(CONNECTION_URL, stream, subject);
//
//            // create a typical NATS message
//            Message msg = NatsMessage.builder()
//                    .subject(subject)
//                    .data(body.toString(), StandardCharsets.UTF_8)
//                    .build();
//
//            // Publish a message and print the results of the publish acknowledgement.
//            // An exception will be thrown if there is a failure.
//            PublishAck pa = js.publish(msg);
//        }catch (IOException e){
//            log.debug(e.getMessage());
//        }catch (JetStreamApiException e){
//            log.debug(e.getMessage());
//        }
//    }
//    public void _doPublishEvent(String stream, String eventName, JsonObject body) {
//            this._doPublishNatsJSEvent(stream, eventName, body);
//    }
    public JetStream _jsmConnect(String url, String stream, String subject){
        JetStream js = null;
        try {
            Connection nc = _connect(url);

            // check to see if the stream is there.
            if (!streamExists(nc, stream)) {
                log.info("Stopping program, stream does not exist: " + stream);
                return null;
            }else{
                // create a dispatcher without a default handler.
                Dispatcher dispatcher = nc.createDispatcher();

                // Create our JetStream context to receive JetStream messages.
                js = nc.jetStream();
            }
        } catch (IOException e) {
            log.info(e.getMessage());
            log.info(e.getCause().toString());
        } catch (InterruptedException e) {
            log.info(e.getMessage());
            log.info(e.getCause().toString());
        }catch (JetStreamApiException e){
            log.info(e.getMessage());
            log.info(e.getCause().toString());
        }
        return js;
    }

    public Connection _connect() throws IOException, InterruptedException {
        return _connect(CONNECTION_URL);
    }

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

    public void _closeConnection(Connection nc){
        try {
            nc.close();
            log.debug("Nats closed connection {}", nc.toString());
        } catch (InterruptedException e) {
            log.debug(e.getMessage());
        }
    }
    public boolean _checkStreams(Connection nc) {
        //**********************************************************************************
        // Check to be sure all the streams are present
        //**********************************************************************************
        try {
            Map<String, String> bashEnv = System.getenv();
            String streamEnvironment = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","DEV");
            String subjectPrefix = streamEnvironment + "." + bashEnv.getOrDefault("AGAVE_MESSAGE_PREFIX","default.");
            subjectPrefix = subjectPrefix.replaceAll("\\.{2,}",".");
            subjectPrefix = StringUtils.stripEnd(subjectPrefix, ".");
            if (!streamExists(nc, streamEnvironment)) {
                log.info("Stopping program, stream does not exist: {}", streamEnvironment);
                return false;
            }else {
                JetStreamManagement jsm = nc.jetStreamManagement();
                _createStream(jsm, streamEnvironment, streamEnvironment + "." + subjectPrefix + ".>");
            }
        } catch (JetStreamApiException | IOException e) {
            log.debug("TransferTaskCreatedListener - Error with subsription {}", e.getMessage());
        }
        return true;
    }
    public static boolean streamExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(nc.jetStreamManagement(), streamName) != null;
    }
    public static boolean streamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(jsm, streamName) != null;
    }

    public static StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
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

    public String _getStreamPrefix(){
        // This methon pull back the:
        //  envorment
        //  version
        // and puts them into a string to be used by a Nats Jetstream stream.
        ////$branch.$type.$owner.$systemid.transfer.protocol.sftp
        Slugify slg = new Slugify();
        String subjectPrefix ="";
        Map<String, String> bashEnv = System.getenv();
        String streamEnvironment = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","DEV");
        subjectPrefix = bashEnv.getOrDefault("AGAVE_MESSAGE_PREFIX","default");
        subjectPrefix = subjectPrefix.replaceAll("\\.{2,}",".");
        subjectPrefix = StringUtils.stripEnd(subjectPrefix, ".");
        // slugify each component
        subjectPrefix = Arrays.stream(subjectPrefix.split(".")).map(t -> slg.slugify(t)).collect(Collectors.joining("."));

        subjectPrefix = streamEnvironment + "." + subjectPrefix;
        return subjectPrefix;
    }

    private static void _createStream(JetStreamManagement jsm, String name, String subject) {
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
}

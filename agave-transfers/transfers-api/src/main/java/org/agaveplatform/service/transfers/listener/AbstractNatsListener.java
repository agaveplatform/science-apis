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

    public String streamName = config().getString("STREAM_NAME");

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
            this.getClass().getSimpleName();
        }

    @Override
    public String getDefaultEventChannel() {
        return null;
    }


    public String _getStreamName() {
        return streamName;
    }


    /**
     * This method will create the consumer name from the implementing class simple name, and provided {@code eventName}.
     *
     * @param eventName - this is the MessageType, which is enough for uniqueness. The subject is not needed
     * @return a unique consumerName for the event and implementing class
     */
//    public String createConsumerNameForEvent(String eventName){
//        return Slug.toSlug(String.format("%s-%s", this.getClass().getSimpleName(), eventName), true);
//
//        //Slugify slg = new Slugify();
////        String consumerName = "";
////        String prefix = _getStreamPrefix();
////
////        consumerName = prefix + "." + streamName + "."+ eventName;
////        consumerName = consumerName.replaceAll("\\.{2,}",".");
////        consumerName = StringUtils.stripEnd(consumerName, ".");
////
////        AgaveUUID uuid = null;
////        try {
////            uuid = new AgaveUUID(UUIDType.SYSTEM.toString());
////        } catch (UUIDException e) {
////            log.debug(e.getMessage());
////        }
////        consumerName = consumerName + uuid.toString();
////
////        return consumerName;
//        String consumerName = "";
//        String prefix = _getStreamPrefix();
//
//        consumerName =  streamName + "." + type + ".*.*.*." + eventName;
//        consumerName = consumerName.replaceAll("\\.{2,}",".");
//        consumerName = StringUtils.stripEnd(consumerName, ".");
//
//        return consumerName;
//    }

    //_createConsumerName(streamName,"transfers", tt.getTenantId(), tt.getOwner(), sourceClient.getHost().toString(), MessageType.TRANSFERTASK_CREATED);
    public String _createConsumerName(String streamName, String type, String tenantId, String owner, String host, String eventName){
        //Slugify slg = new Slugify();
        String consumerName = "";
        String prefix = _getStreamPrefix();

        consumerName =  streamName + "_" + type + "_"+ tenantId + "_" + owner + "_" + host + "_" + eventName;
        consumerName = consumerName.replaceAll("\\.{2,}",".");
        consumerName = StringUtils.stripEnd(consumerName, ".");

        return consumerName;
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

//    /*
//     *    This will publish a message @body on the messageAddress
//     *    @param messageAddress - Address of the Nats message
//     *    @param body - body of the message
//     *    @return void
//     */
//    public void _doPublishNatsJSEvent(String messageAddress, JsonObject body) throws IOException, InterruptedException, TimeoutException {
//        log.info(super.getClass().getName() + ": _doPublishNatsEvent({}, {})", messageAddress, body);
//        Connection nc;
//        try{
//            Options options = new Options.Builder().
//                    server(CONNECTION_URL).
//                    connectionTimeout(Duration.ofSeconds(30)). // Set timeout
//                    build();
//            nc = Nats.connect(options);
//            //nc = _connect(CONNECTION_URL);
//
//            JetStream js = nc.jetStream();
//            JetStreamManagement jsm = nc.jetStreamManagement();
//            String stream;
//            if ( messageAddress.startsWith("notification")  ){
//                stream = "NOTIFICATION";
//            }else {
//                stream = "TRANSFERTASK";
//            }
//
//            String consumerName="";
//            // check to be sure the stream and the consumer exits.  If the consumer does not exist then first create it
//            if (!_checkConsumer(jsm, stream, messageAddress)){
//                String tenantid = body.getString("tenantId");
//                String uid = body.getString("owner");
//                AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(body.getString("source")));
//
//                String systemid = srcUri.get().getHost();
//
//                //transfers.$tenantid.$uid.$systemid.$messageAddress
//                consumerName = createConsumerNameForEvent(messageAddress);
//
//                _createConsumer(jsm, stream, consumerName, messageAddress);
//            }
//
//            // create a typical NATS message
//            Message msg = NatsMessage.builder()
//                    .subject(messageAddress)
//                    .data(body.toString(), StandardCharsets.UTF_8)
//                    .build();
//
//            PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
//                    .expectedStream(stream)
//                    .messageId("latest");
//
////            nc.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8));
////            log.debug("Published using nc.publish to {}", messageAddress);
//            // Publish a message and print the results of the publish acknowledgement.
//            // An exception will be thrown if there is a failure.
//            PublishAck pa = js.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8), pubOptsBuilder.build());
//            log.debug("Published message to {} with a sequence no of {} and consumer name {}",pa.getStream(), pa.getSeqno(), consumerName);
//            //nc.publish(messageAddress, body.toString().getBytes(StandardCharsets.UTF_8));
////            nc.flush(Duration.ofMillis(1000));
////            _closeConnection(nc);
//
//        } catch (IOException | JetStreamApiException e) {
//            log.debug("Error with _doPublishNatsJSEvent:  {}", e.getMessage());
//            //nc = _connect();
//        }
//    }

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



}

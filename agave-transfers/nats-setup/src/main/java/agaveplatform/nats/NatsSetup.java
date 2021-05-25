package agaveplatform.nats;


import io.nats.client.*;
import io.nats.client.api.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class NatsSetup {
    private static final Logger log = LoggerFactory.getLogger(NatsSetup.class);

    public static void main(String[] args) {
        log.debug("starting the run");
        try (Connection nc = NatsSetup._connect()) {

            JetStream js = nc.jetStream();
            log.debug("Connected to jetstream");

            _createStreamAndConsumer();
            log.info("All streams and consumers have been created.");

            System. exit(0);

        }
        catch (Exception e) {
            log.debug(e.getMessage());
        } finally {
            System. exit(0);
        }
    }

    public static Connection _connect() throws IOException, InterruptedException {
        Options.Builder builder = new Options.Builder()
                .server("nats://nats:4222")
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
                exp.printStackTrace();
            }
            @Override
            public void errorOccurred(Connection conn, String error) {
                log.info("NATS connection error occurred {}", error);
            }
        });

        log.debug("_connected with NATS");
        Connection nc = Nats.connect(builder.build());
        return nc;
    }

    public static void _createStreamAndConsumer() throws JetStreamApiException, IOException {
        // Create a JetStreamManagement context.

        Connection nc = null;
        try {
            nc = _connect();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        JetStreamManagement jsm = null;
        try {
            jsm = nc.jetStreamManagement();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String, String> bashEnv = System.getenv();
        String streamEnvironment = bashEnv.getOrDefault("AGAVE_ENVIRONMENT","DEV");
        String subjectPrefix = streamEnvironment + "." + bashEnv.getOrDefault("AGAVE_MESSAGE_PREFIX","transfers.");
        subjectPrefix = subjectPrefix.replaceAll("\\.{2,}",".");
        subjectPrefix = StringUtils.stripEnd(subjectPrefix, ".");
        // slugify each component
//        Slugify slg = new Slugify();
//        subjectPrefix = Arrays.stream(subjectPrefix.split(".")).map(t -> slg.slugify(t)).collect(Collectors.joining("."));


        _createStream(jsm, streamEnvironment,  subjectPrefix + ".>");

        log.info("done creating streams");
        //$branch.$type.$owner.$systemid.transfer.protocol.sftp
        //branch_abc.notificatin.*
        log.info("Now creating consumers");
//        _createConsumer(jsm, "AGAVE_DEV", subjectPrefix +"_TRANSFERTASK_CREATED_ConsumerD", subjectPrefix + ".transfers.*.*.transfer.protocol.*");
        //_createConsumer(jsm, "DEV", "DEV_transfers_tenantId_owner_host_transfertask_created","DEV.transfers.a.b.c.transfertask_created");

        //ConsumerInfo cI = jsm.getConsumerInfo("DEV", "DEV_transfers_tenantId_owner_host_transfertask_created");
       // log.info(cI.getDescription());

        //log.info("All consumers have been created.");
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

    private static void _createConsumer(JetStreamManagement jsm, String stream, String consumerName, String subject){
        log.info("Configure And Add A Consumer {}: {}", consumerName, subject);
        try {
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
            log.info("Consumer Name: "+ consumerInfo.getStreamName() + "    Desc:" + consumerInfo.getDescription());
            log.info("Consumer Info: {}", consumerInfo.getConsumerConfiguration());

        } catch (IOException | JetStreamApiException e) {
            log.debug(e.getMessage() );
            log.debug(e.getCause().toString());
        }
    }
}
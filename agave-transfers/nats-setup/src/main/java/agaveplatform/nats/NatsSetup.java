package agaveplatform.nats;


import io.nats.client.*;
import io.nats.client.api.*;
//import io.nats.client.impl.NatsMessage;

import java.io.IOException;
//import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static void _createStreamAndConsumer()  {
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
        _createStream(jsm, "TRANSFERTASK", "transfertask.*");
        _createStream(jsm, "NOTIFICATION", "notification*");
        _createStream(jsm, "TRANSFER", "transfer.*");
        _createStream(jsm, "FILETRANSFER", "filetransfer.*");
        _createStream(jsm, "UrlCopy", "filetransfer.*");
        log.info("done creating streams");

        log.info("Now creating consumers");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CREATED_Consumer", "transfertask.created");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_ASSIGNED_Consumer","transfertask.assigned");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CANCELED_SYNC_Consumer", "transfertask.canceled.sync");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CANCELED_COMPLETED_Consumer","transfertask.canceled.completed");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CANCELED_ACK_Consumer","transfertask.canceled.ack");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CANCELED_Consumer","transfertask.canceled");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_CANCELED_PAUSED_Consumer","transfertask.canceled.paused");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PAUSED_Consumer","transfertask.paused");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PAUSED_SYNC_Consumer","transfertask.paused.sync");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PAUSED_COMPLETED_Consumer","transfertask.paused.completed");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PAUSED_ACK_Consumer","transfertask.paused.ack");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_COMPLETED_Consumer","transfer.completed");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_FINISHED_Consumer","transfertask.finished");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_ERROR_Consumer","transfertask.error");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PARENT_ERROR_Consumer","transfertask.parent.error");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_FAILED_Consumer","transfertask.failed");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_INTERUPTED_Consumer","transfertask.interupted");
        _createConsumer(jsm, "NOTIFICATION", "NOTIFICATION_Consumer","notification");
        _createConsumer(jsm, "NOTIFICATION", "NOTIFICATION_TRANSFERTASK_Consumer","notification.transfertask");
        _createConsumer(jsm, "NOTIFICATION", "NOTIFICATION_CANCELED_Consumer","notification.cancelled");
        _createConsumer(jsm, "NOTIFICATION", "NOTIFICATION_COMPLETED_Consumer","notification.completed");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_SFTP_Consumer","transfer.sftp");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_HTTP_Consumer","transfer.http");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_GRIDFTP_Consumer","transfer.gridftp");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_FTP_Consumer","transfer.ftp");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_IRODS_Consumer","transfer.irods");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_IRODS4_Consumer","transfer.irods4");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_LOCAL_Consumer","transfer.local");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_AZURE_Consumer","transfer.azure");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_S3_Consumer","transfer.s3");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_SWIFT_Consumer","transfer.swift");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_HTTPS_Consumer","transfer.https");
        _createConsumer(jsm, "FILETRANSFER", "FILETRANSFER_SFTP_Consumer","filetransfer.sftp");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_DB_QUEUE_Consumer","transfertask.db.queue");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_DELETED_Consumer","transfertask.deleted");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_DELETED_SYNC_Consumer","transfertask.deleted.sync");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_DELETED_COMPLETED_Consumer","transfertask.deleted.completed");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_DELETED_ACK_Consumer","transfertask.deleted.ack");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_UPDATED_Consumer","transfertask.updated");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_PROCESS_UNARY_Consumer","transfertask.process.unary");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_STREAMING_Consumer","transfer.streaming");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_UNARY_Consumer","transfer.unary");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_HEALTHCHECK_Consumer","transfertask.healthcheck");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_HEALTHCHECK_PARENT_Consumer","transfertask.healthcheck_parent");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_FAILED_Consumer","transfer.failed");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_RETRY_Consumer","transfer.retry");
        _createConsumer(jsm, "TRANSFER", "TRANSFER_ALL_Consumer","transfer.all");
        _createConsumer(jsm, "TRANSFERTASK", "TRANSFERTASK_NOTIFICATION_Consumer","transfertask.notification");
        _createConsumer(jsm, "TRANSFERTASK", "UrlCopy_Consumer","transfertask.UrlCopy");
        log.info("All consumers have been created.");
    }

    private static void _createStream(JetStreamManagement jsm, String name, String subject) {
        log.info("Configure And Add Streams.");
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(name)
                    .subjects(subject)
                    .retentionPolicy(RetentionPolicy.Limits)
                    .maxConsumers(-1)
                    .maxBytes(-1)
                    .maxAge(Duration.ofDays(60))
                    .maxMsgSize(-1)
                    .maxMessages(2000000)
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
                .ackPolicy(AckPolicy.None)
                .durable(consumerName) // durable name is required when creating consumers
                .filterSubject("transfertask.created")
                .deliverPolicy(DeliverPolicy.Last)
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
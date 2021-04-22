package main.java;


import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsSetup {
    private static final Logger log = LoggerFactory.getLogger(NatsSetup.class);

    public static void main(String[] args) {
        log.debug("starting the run");
        try (Connection nc = NatsSetup._connect()) {

            JetStream js = nc.jetStream();
            log.debug("connected to jetstream");

            _createStreamAndConsumer();
            log.info("All streams have been created.");
        }
        catch (Exception e) {
            log.debug(e.getMessage());
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
                System.out.println("NATS connection slow consumer detected");
            }
            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                System.out.println("NATS connection exception occurred");
                exp.printStackTrace();
            }
            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.println("NATS connection error occurred " + error);
            }
        });

        log.debug("_connect");
        Connection nc = Nats.connect(builder.build());
        return nc;
    }

    public static void _createStreamAndConsumer() throws IOException, InterruptedException, JetStreamApiException {
        // Create a JetStreamManagement context.
        Connection nc = _connect();
        JetStreamManagement jsm = nc.jetStreamManagement();
        _createStream(jsm, "TRANSFERTASK_CREATED", "transfertask.created");
        _createStream(jsm, "TRANSFERTASK_ASSIGNED", "transfertask.assigned");
        _createStream(jsm, "TRANSFERTASK_CANCELED_SYNC", "transfertask.canceled.sync");
        _createStream(jsm, "TRANSFERTASK_CANCELED_COMPLETED", "transfertask.canceled.completed");
        _createStream(jsm, "TRANSFERTASK_CANCELED_ACK", "transfertask.canceled.ack");
        _createStream(jsm, "TRANSFERTASK_CANCELED", "transfertask.canceled");
        _createStream(jsm, "TRANSFERTASK_PAUSED", "transfertask.paused");
        _createStream(jsm, "TRANSFERTASK_PAUSED_SYNC", "transfertask.paused.sync");
        _createStream(jsm, "TRANSFERTASK_PAUSED_COMPLETED", "transfertask.paused.completed");
        _createStream(jsm, "TRANSFERTASK_PAUSED_ACK", "transfertask.paused.ack");
        _createStream(jsm, "TRANSFER_COMPLETED", "transfer.completed");
        _createStream(jsm, "TRANSFERTASK_FINISHED", "transfertask.finished");
        _createStream(jsm, "TRANSFERTASK_ERROR", "transfertask.error");
        _createStream(jsm, "TRANSFERTASK_PARENT_ERROR", "transfertask.parent.error");
        _createStream(jsm, "TRANSFERTASK_FAILED", "transfertask.failed");
        _createStream(jsm, "TRANSFERTASK_INTERUPTED", "transfertask.interupted");
        _createStream(jsm, "NOTIFICATION", "notification");
        _createStream(jsm, "NOTIFICATION_TRANSFERTASK", "notification.transfertask");
        _createStream(jsm, "NOTIFICATION_CANCELED", "notification.cancelled");
        _createStream(jsm, "NOTIFICATION_COMPLETED", "notification.completed");
        _createStream(jsm, "TRANSFER_SFTP", "transfer.sftp");
        _createStream(jsm, "TRANSFER_HTTP", "transfer.http");
        _createStream(jsm, "TRANSFER_GRIDFTP", "transfer.gridftp");
        _createStream(jsm, "TRANSFER_FTP", "transfer.ftp");
        _createStream(jsm, "TRANSFER_IRODS", "transfer.irods");
        _createStream(jsm, "TRANSFER_IRODS4", "transfer.irods4");
        _createStream(jsm, "TRANSFER_LOCAL", "transfer.local");
        _createStream(jsm, "TRANSFER_AZURE", "transfer.azure");
        _createStream(jsm, "TRANSFER_S3", "transfer.s3");
        _createStream(jsm, "TRANSFER_SWIFT", "transfer.swift");
        _createStream(jsm, "TRANSFER_HTTPS", "transfer.https");
        _createStream(jsm, "FILETRANSFER_SFTP", "filetransfer.sftp");
        _createStream(jsm, "TRANSFERTASK_DB_QUEUE", "transfertask.db.queue");
        _createStream(jsm, "TRANSFERTASK_DELETED", "transfertask.deleted");
        _createStream(jsm, "TRANSFERTASK_DELETED_SYNC", "transfertask.deleted.sync");
        _createStream(jsm, "TRANSFERTASK_DELETED_COMPLETED", "transfertask.deleted.completed");
        _createStream(jsm, "TRANSFERTASK_DELETED_ACK", "transfertask.deleted.ack");
        _createStream(jsm, "TRANSFERTASK_UPDATED", "transfertask.updated");
        _createStream(jsm, "TRANSFERTASK_PROCESS_UNARY", "transfertask.process.unary");
        _createStream(jsm, "TRANSFER_STREAMING", "transfer.streaming");
        _createStream(jsm, "TRANSFER_UNARY", "transfer.unary");
        _createStream(jsm, "TRANSFERTASK_HEALTHCHECK", "transfertask.healthcheck");
        _createStream(jsm, "TRANSFERTASK_HEALTHCHECK_PARENT", "transfertask.healthcheck_parent");
        _createStream(jsm, "TRANSFER_FAILED", "transfer.failed");
        _createStream(jsm, "TRANSFER_RETRY", "transfer.retry");
        _createStream(jsm, "TRANSFER_ALL", "transfer.all");
        _createStream(jsm, "TRANSFERTASK_NOTIFICATION", "transfertask.notification");
        _createStream(jsm, "UrlCopy", "transfertask.UrlCopy");
        log.info("done creating streams");

        log.info("Now creating consumers");
        _createConsumer(jsm, "TRANSFERTASK_CREATED", "TRANSFERTASK_CREATED_Consumer", "transfertask.created");
        _createConsumer(jsm, "TRANSFERTASK_ASSIGNED", "TRANSFERTASK_ASSIGNED_Consumer","transfertask.assigned");
        _createConsumer(jsm, "TRANSFERTASK_CANCELED_SYNC", "TRANSFERTASK_CANCELED_SYNC_Consumer", "transfertask.canceled.sync");
        _createConsumer(jsm, "TRANSFERTASK_CANCELED_COMPLETED", "TRANSFERTASK_CANCELED_COMPLETED_Consumer","transfertask.canceled.completed");
        _createConsumer(jsm, "TRANSFERTASK_CANCELED_ACK", "TRANSFERTASK_CANCELED_ACK_Consumer","transfertask.canceled.ack");
        _createConsumer(jsm, "TRANSFERTASK_CANCELED", "TRANSFERTASK_CANCELED_Consumer","transfertask.canceled");
        _createConsumer(jsm, "TRANSFERTASK_PAUSED", "TRANSFERTASK_PAUSED_Consumer","transfertask.paused");
        _createConsumer(jsm, "TRANSFERTASK_PAUSED_SYNC", "TRANSFERTASK_PAUSED_SYNC_Consumer","transfertask.paused.sync");
        _createConsumer(jsm, "TRANSFERTASK_PAUSED_COMPLETED", "TRANSFERTASK_PAUSED_COMPLETED_Consumer","transfertask.paused.completed");
        _createConsumer(jsm, "TRANSFERTASK_PAUSED_ACK", "TRANSFERTASK_PAUSED_ACK_Consumer","transfertask.paused.ack");
        _createConsumer(jsm, "TRANSFER_COMPLETED", "TRANSFER_COMPLETED_Consumer","transfer.completed");
        _createConsumer(jsm, "TRANSFERTASK_FINISHED", "TRANSFERTASK_FINISHED_Consumer","transfertask.finished");
        _createConsumer(jsm, "TRANSFERTASK_ERROR", "TRANSFERTASK_ERROR_Consumer","transfertask.error");
        _createConsumer(jsm, "TRANSFERTASK_PARENT_ERROR", "TRANSFERTASK_PARENT_ERROR_Consumer","transfertask.parent.error");
        _createConsumer(jsm, "TRANSFERTASK_FAILED", "TRANSFERTASK_FAILED_Consumer","transfertask.failed");
        _createConsumer(jsm, "TRANSFERTASK_INTERUPTED", "TRANSFERTASK_INTERUPTED_Consumer","transfertask.interupted");
        _createConsumer(jsm, "NOTIFICATION", "NOTIFICATION_Consumer","notification");
        _createConsumer(jsm, "NOTIFICATION_TRANSFERTASK", "NOTIFICATION_TRANSFERTASK_Consumer","notification.transfertask");
        _createConsumer(jsm, "NOTIFICATION_CANCELED", "NOTIFICATION_CANCELED_Consumer","notification.cancelled");
        _createConsumer(jsm, "NOTIFICATION_COMPLETED", "NOTIFICATION_COMPLETED_Consumer","notification.completed");
        _createConsumer(jsm, "TRANSFER_SFTP", "TRANSFER_SFTP_Consumer","transfer.sftp");
        _createConsumer(jsm, "TRANSFER_HTTP", "TRANSFER_HTTP_Consumer","transfer.http");
        _createConsumer(jsm, "TRANSFER_GRIDFTP", "TRANSFER_GRIDFTP_Consumer","transfer.gridftp");
        _createConsumer(jsm, "TRANSFER_FTP", "TRANSFER_FTP_Consumer","transfer.ftp");
        _createConsumer(jsm, "TRANSFER_IRODS", "TRANSFER_IRODS_Consumer","transfer.irods");
        _createConsumer(jsm, "TRANSFER_IRODS4", "TRANSFER_IRODS4_Consumer","transfer.irods4");
        _createConsumer(jsm, "TRANSFER_LOCAL", "TRANSFER_LOCAL_Consumer","transfer.local");
        _createConsumer(jsm, "TRANSFER_AZURE", "TRANSFER_AZURE_Consumer","transfer.azure");
        _createConsumer(jsm, "TRANSFER_S3", "TRANSFER_S3_Consumer","transfer.s3");
        _createConsumer(jsm, "TRANSFER_SWIFT", "TRANSFER_SWIFT_Consumer","transfer.swift");
        _createConsumer(jsm, "TRANSFER_HTTPS", "TRANSFER_HTTPS_Consumer","transfer.https");
        _createConsumer(jsm, "FILETRANSFER_SFTP", "FILETRANSFER_SFTP_Consumer","filetransfer.sftp");
        _createConsumer(jsm, "TRANSFERTASK_DB_QUEUE", "TRANSFERTASK_DB_QUEUE_Consumer","transfertask.db.queue");
        _createConsumer(jsm, "TRANSFERTASK_DELETED", "TRANSFERTASK_DELETED_Consumer","transfertask.deleted");
        _createConsumer(jsm, "TRANSFERTASK_DELETED_SYNC", "TRANSFERTASK_DELETED_SYNC_Consumer","transfertask.deleted.sync");
        _createConsumer(jsm, "TRANSFERTASK_DELETED_COMPLETED", "TRANSFERTASK_DELETED_COMPLETED_Consumer","transfertask.deleted.completed");
        _createConsumer(jsm, "TRANSFERTASK_DELETED_ACK", "TRANSFERTASK_DELETED_ACK_Consumer","transfertask.deleted.ack");
        _createConsumer(jsm, "TRANSFERTASK_UPDATED", "TRANSFERTASK_UPDATED_Consumer","transfertask.updated");
        _createConsumer(jsm, "TRANSFERTASK_PROCESS_UNARY", "TRANSFERTASK_PROCESS_UNARY_Consumer","transfertask.process.unary");
        _createConsumer(jsm, "TRANSFER_STREAMING", "TRANSFER_STREAMING_Consumer","transfer.streaming");
        _createConsumer(jsm, "TRANSFER_UNARY", "TRANSFER_UNARY_Consumer","transfer.unary");
        _createConsumer(jsm, "TRANSFERTASK_HEALTHCHECK", "TRANSFERTASK_HEALTHCHECK_Consumer","transfertask.healthcheck");
        _createConsumer(jsm, "TRANSFERTASK_HEALTHCHECK_PARENT", "TRANSFERTASK_HEALTHCHECK_PARENT_Consumer","transfertask.healthcheck_parent");
        _createConsumer(jsm, "TRANSFER_FAILED", "TRANSFER_FAILED_Consumer","transfer.failed");
        _createConsumer(jsm, "TRANSFER_RETRY", "TRANSFER_RETRY_Consumer","transfer.retry");
        _createConsumer(jsm, "TRANSFER_ALL", "TRANSFER_ALL_Consumer","transfer.all");
        _createConsumer(jsm, "TRANSFERTASK_NOTIFICATION", "TRANSFERTASK_NOTIFICATION_Consumer","transfertask.notification");
        _createConsumer(jsm, "UrlCopy", "UrlCopy_Consumer","transfertask.UrlCopy");
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
                    .maxMessages(20000)
                    .storageType(StorageType.Memory)
                    .replicas(1)
                    .noAck(false)
                    // .template(...)
                    .discardPolicy(DiscardPolicy.Old)
                    .build();
            StreamInfo streamInfo = jsm.addStream(streamConfig);
            log.info("Create stream {}", jsm.getStreamInfo(streamConfig.getName()));
        }
        catch (JetStreamApiException e) {
            log.debug(e.getMessage());
        } catch (IOException e) {
            log.debug(e.getMessage());
        }
    }

    private static void _createConsumer(JetStreamManagement jsm, String stream, String consumerName, String subject){
        log.info("Configure And Add A Consumer {}", consumerName);
        try {
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(consumerName) // durable name is required when creating consumers
                    //.deliverSubject(subject)
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(Duration.ofSeconds(60))
                    .maxDeliver(5)
                    .sampleFrequency("100")
                    .maxAckPending(1000)
                    .build();
            ConsumerInfo ci = null;

            ci = jsm.addOrUpdateConsumer(stream, cc);
            log.info("Consumer Name: "+ci.getStreamName() + "    Desc:" + ci.getDescription());
        } catch (IOException e) {
            log.debug(e.getMessage() );
        } catch (JetStreamApiException e) {
            log.debug(e.getMessage() );
        }
    }
}
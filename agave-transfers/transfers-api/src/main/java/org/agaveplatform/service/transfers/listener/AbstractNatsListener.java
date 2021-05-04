package org.agaveplatform.service.transfers.listener;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
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

    public void _doPublishNatsEvent(String eventName, JsonObject body) throws IOException, InterruptedException, TimeoutException {
        log.info(super.getClass().getName() + ": _doPublishNatsEvent({}, {})", eventName, body);
        Connection nc;
        try{
            nc = _connect(CONNECTION_URL);
        } catch (IOException e) {
            nc = _connect();
        }
        nc.publish(eventName, body.toString().getBytes(StandardCharsets.UTF_8));
        nc.flush(Duration.ofMillis(1000));
        _closeConnection(nc);
    }

    public void _doPublishNatsJSEvent(String stream, String subject, JsonObject body) {
        log.info(super.getClass().getName() + ": _doPublishNatsJSEvent({}, {}, {})", stream, subject, body);

        try {
            JetStream js = _jsmConnect(CONNECTION_URL, stream, subject);

            // create a typical NATS message
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(body.toString(), StandardCharsets.UTF_8)
                    .build();

            // Publish a message and print the results of the publish acknowledgement.
            // An exception will be thrown if there is a failure.
            PublishAck pa = js.publish(msg);
        }catch (IOException e){
            log.debug(e.getMessage());
        }catch (JetStreamApiException e){
            log.debug(e.getMessage());
        }
    }
    public void _doPublishEvent(String stream, String eventName, JsonObject body) {
            this._doPublishNatsJSEvent(stream, eventName, body);
    }
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
            if (!streamExists(nc, "TRANSFERTASK")) {
                log.info("Stopping program, stream does not exist: TRANSFERTASK");
                return false;
            } else if (!streamExists(nc, "FILETRANSFER")) {
                log.info("Stopping program, stream does not exist: FILETRANSFER");
                return false;
            } else if (!streamExists(nc, "NOTIFICATION")) {
                log.info("Stopping program, stream does not exist: NOTIFICATION");
                return false;
            } else if (!streamExists(nc, "TRANSFER")) {
                log.info("Stopping program, stream does not exist: TRANSFER");
                return false;
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

}

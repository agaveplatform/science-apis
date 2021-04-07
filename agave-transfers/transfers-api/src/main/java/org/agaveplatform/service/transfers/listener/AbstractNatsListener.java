package org.agaveplatform.service.transfers.listener;

import io.nats.client.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

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

    @Override
    public void _doPublishEvent(String eventName, JsonObject body) throws IOException, InterruptedException {
        try {
            this._doPublishNatsEvent(eventName, body);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public Connection _connect() throws IOException, InterruptedException {
        return _connect(CONNECTION_URL);
    }

    public Connection _connect(String url) throws IOException, InterruptedException {
        Options.Builder builder = new Options.Builder()
                .server(url)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1)
                .verbose()
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
        //return builder.build();
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
}

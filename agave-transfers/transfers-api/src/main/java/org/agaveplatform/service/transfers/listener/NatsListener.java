package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK_PARENT;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.MAX_TIME_FOR_HEALTHCHECK;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.MAX_TIME_FOR_HEALTHCHECK_PARENT;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;


public class NatsListener extends  AbstractNatsListener {
    private final static Logger log = LoggerFactory.getLogger(NatsListener.class);

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList<>();

    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ASSIGNED;

    public NatsListener() {
        super();
    }
    public NatsListener(Vertx vertx) {
        super(vertx);
    }
    public NatsListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        Connection nc = _connect();

        Dispatcher d = nc.createDispatcher((msg) -> {});
        Subscription s = d.subscribe(MessageType.TRANSFERTASK_ASSIGNED, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            log.debug("response is {}", response);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            log.info("Transfer task {} nats test detected.", uuid);
            this.processEvent(body, result -> {
                //result should be true
            });
        });

        d.subscribe(getEventChannel());
        nc.flush(Duration.ofMillis(500));
    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        log.debug("Got into NatsListener.processEvent ");
        log.debug("body = {} ", body);
        String uuid = body.getString("uuid");
        String source = body.getString("source");
        log.debug( "uuid is {} and source is {}", uuid, source);
    }


    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


    public void processInterrupt(JsonObject body, Handler<AsyncResult<Boolean>> handler) {

    }
}

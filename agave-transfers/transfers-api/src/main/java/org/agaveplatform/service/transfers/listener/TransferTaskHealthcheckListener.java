package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.FLUSH_DELAY_NATS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskHealthcheckListener extends AbstractNatsListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferTaskHealthcheckListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK;

	public TransferTaskHealthcheckListener() {
		super();
	}

	public TransferTaskHealthcheckListener(Vertx vertx) {
		super(vertx);
	}

	public TransferTaskHealthcheckListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return this.EVENT_CHANNEL;
	}

	@Override
	public void start() throws IOException, InterruptedException, TimeoutException {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		setDbService(TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue));

		// listen for healthcheck events to determine if a task is complete
		// before its transfertask_completed event was received.
		//getVertx().eventBus().<JsonObject>consumer(TRANSFERTASK_HEALTHCHECK, msg -> {
		Connection nc = _connect();
		Dispatcher d = nc.createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			//msg.reply(TransferTaskHealthcheckListener.class.getName() + " received.");

			logger.info("Performing healthcheck on transfer task {}", uuid);

			this.processAllChildrenCanceledEvent(body);

		});
		d.subscribe(EVENT_CHANNEL);
		nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

	}




	public Future<Boolean> processAllChildrenCanceledEvent(JsonObject body) {
		logger.trace("Got into TransferTaskHealthcheckListener.processEvent");
		Promise<Boolean> promise = Promise.promise();

		String uuid = body.getString("uuid");
		String tenantId = (body.getString("tenant_id"));

		getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, reply -> {
			logger.trace("got into getDbService().allChildrenCancelledOrCompleted");
			if (reply.succeeded()) {
				logger.info("reply from getDBSerivce.allChildrenCancelledOrCompleted " + reply.toString());
				if (reply.result()) {
					getDbService().updateStatus(tenantId, uuid, COMPLETED.name(), updateStatus -> {
						logger.trace("Got into getDBService.updateStatus(complete) ");
						if (updateStatus.succeeded()) {
							logger.info("[{}] Transfer task {} updated to completed.", tenantId, uuid);
							//parentList.remove(uuid);
							_doPublishEvent(MessageType.TRANSFERTASK_FINISHED, updateStatus.result());
							promise.handle(Future.succeededFuture(Boolean.TRUE));
						} else {
							logger.error("[{}] Task {} completed, but unable to update status: {}",
									tenantId, uuid, reply.cause());
							JsonObject json = new JsonObject()
									.put("cause", updateStatus.cause().getClass().getName())
									.put("message", updateStatus.cause().getMessage())
									.mergeIn(body);
							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
							promise.handle(Future.failedFuture(updateStatus.cause()));
						}
					});
				} else {
					logger.info("allChildrenCancelledOrCompleted succeeded but the result is returns false.");
					logger.info("[{}] Transfer task {} is still active", tenantId, uuid);
					getDbService().updateStatus(tenantId, uuid, CANCELED_ERROR.name(), updateStatus -> {
						logger.trace("Got into getDBService.updateStatus(ERROR)");
						if (updateStatus.succeeded()){
							logger.info("[{}] Transfer task {} updated to CANCELED_ERROR.", tenantId, uuid);
							//_doPublishEvent(MessageType.TRANSFERTASK_ERROR, updateStatus.result());
							//promise.handle(Future.succeededFuture(Boolean.TRUE));
						}else{
							logger.error("[{}] Task {} completed, but unable to update status to CANCELED_ERROR: {}",
									tenantId, uuid, reply.cause());
							JsonObject json = new JsonObject()
									.put("cause", updateStatus.cause().getClass().getName())
									.put("message", updateStatus.cause().getMessage())
									.mergeIn(body);
							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
							//promise.handle(Future.failedFuture(updateStatus.cause()));
						}
					});

					promise.handle(Future.succeededFuture(Boolean.TRUE));
				}
			} else {
				logger.error("[{}] Failed to check child status of transfer task {}. Task remains active: {}",
						tenantId, uuid, reply.cause().getMessage());
				JsonObject json = new JsonObject()
						.put("cause", reply.cause().getClass().getName())
						.put("message", reply.cause().getMessage())
						.mergeIn(body);
				_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
				promise.handle(Future.failedFuture(reply.cause()));
			}
		});

		return promise.future();
	}


	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}


}

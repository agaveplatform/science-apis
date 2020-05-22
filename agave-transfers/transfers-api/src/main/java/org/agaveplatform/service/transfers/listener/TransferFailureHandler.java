package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;

public class TransferFailureHandler extends AbstractTransferTaskListener implements Handler<RoutingContext> {
	private final Logger log = LoggerFactory.getLogger(TransferFailureHandler.class);

	public TransferFailureHandler(Vertx vertx) {
		super(vertx);
	}

	public TransferFailureHandler(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	public void handle(RoutingContext context){
		Throwable thrown = context.failure();
		recordError(thrown);
		context.response().setStatusCode(500).end();
	}

	private void recordError(Throwable throwable){
		log.info("failed: {}", throwable.getMessage());
	}
	private TransferTaskDatabaseService dbService;

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			processFailure(body);

			log.error("Transfer task {} failed: {}: {}",
					body.getString("id"), body.getString("cause"), body.getString("message"));

			_doPublishEvent(MessageType.TRANSFERTASK_FAILED, body);
		});

	}


	protected Future<Boolean> processFailure(JsonObject body){

		Promise<Boolean> promise = Promise.promise();

		String id = body.getString("id");
		String uuid = body.getString("uuid");
		String tenantId = body.getString("tenantId");

		body.remove("cause");
		body.remove("message");
		body.remove("id");

		TransferTask bodyTask = new TransferTask(body);
		bodyTask.setStatus(TransferStatusType.FAILED);

		getDbService().update(tenantId, uuid, bodyTask, updateBody -> {
			if (updateBody.succeeded()) {
				//log.info("Transfer task  updated.");
				promise.handle(Future.succeededFuture(Boolean.TRUE));
			} else {
				JsonObject json = new JsonObject()
						.put("cause", updateBody.cause().getClass().getName())
						.put("message", updateBody.cause().getMessage())
						.mergeIn(body);
				_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
				promise.handle(Future.failedFuture(updateBody.cause()));
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

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
	private static final Logger log = LoggerFactory.getLogger(TransferFailureHandler.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	private TransferTaskDatabaseService dbService;

	public TransferFailureHandler() { super(); }

	public TransferFailureHandler(Vertx vertx) {
		super(vertx);
	}

	public TransferFailureHandler(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

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


	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			log.info("Transfer task {} failed: {}: {}",
					body.getString("uuid"), body.getString("cause"), body.getString("message"));

			processFailure(body);
		});

	}

	protected Future<Boolean> processFailure(JsonObject body) {
		Promise<Boolean> promise = Promise.promise();
		try {
//			String id = body.getValue("id");
			String uuid = body.getString("uuid");
			String tenantId = body.getString("tenantId");

			body.remove("cause");
			body.remove("message");
			body.remove("id");

			TransferTask bodyTask = new TransferTask(body);
			bodyTask.setStatus(TransferStatusType.FAILED);

			getDbService().update(tenantId, uuid, bodyTask, updateBody -> {
				if (updateBody.succeeded()) {
					log.info("Transfer task {} successfully marked as {}.", uuid,
							updateBody.result().getString("status"));
					promise.handle(Future.succeededFuture(Boolean.TRUE));
				} else {
					JsonObject json = new JsonObject()
							.put("cause", updateBody.cause().getClass().getName())
							.put("message", updateBody.cause().getMessage())
							.mergeIn(body);
					promise.handle(Future.failedFuture(updateBody.cause()));
					// infinite loop if we publish an error event here
					// TODO: we need a wrapper object around our event bodies so we can record things like the number
					// 	of attempts to process a given event, last event time, and whether to keep trying indefinitely
					//  or not.
				}
			});
		}
		catch (Throwable t) {
			log.error("Failed to process failure event for transfer task {}: {}",
					body.getValue("id"), t.getMessage());
			promise.handle(Future.failedFuture(t));
		}

		return promise.future();
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

}

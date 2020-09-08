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

public class TransferTaskErrorFailureHandler extends AbstractTransferTaskListener implements Handler<RoutingContext> {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskErrorFailureHandler.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	private TransferTaskDatabaseService dbService;

	public TransferTaskErrorFailureHandler() { super(); }

	public TransferTaskErrorFailureHandler(Vertx vertx) {
		super(vertx);
	}

	public TransferTaskErrorFailureHandler(Vertx vertx, String eventChannel) {
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

			processFailure(body, resp -> {
				if (resp.succeeded()) {
					log.debug("Completed processing {} event for transfer task {}", getEventChannel(), body.getString("uuid"));
				} else {
					log.error("Unable to process {} event for transfer task message: {}", getEventChannel(), body.encode(), resp.cause());
				}
			});
		});

	}

	protected void processFailure(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
		try {
			body.remove("cause");
			body.remove("message");
			body.remove("id");

			TransferTask failedTask = new TransferTask(body);
			failedTask.setStatus(TransferStatusType.FAILED);

			getDbService().update(failedTask.getTenantId(), failedTask.getUuid(), failedTask, updateBody -> {
				if (updateBody.succeeded()) {
					TransferTask savedTask = new TransferTask(updateBody.result());
					log.info("Transfer task {} successfully marked as {}.", failedTask.getUuid(), savedTask.getStatus().name());
					handler.handle(Future.succeededFuture(Boolean.TRUE));
				} else {
					String msg = String.format("Unable to update status of failed transfer task {}. {}",
							failedTask.getUuid(), updateBody.cause().getMessage());
					log.error(msg);
					JsonObject json = new JsonObject()
							.put("cause", updateBody.cause().getClass().getName())
							.put("message", msg)
							.mergeIn(body);
					handler.handle(Future.failedFuture(updateBody.cause()));

					// infinite loop if we publish an error event here
					// TODO: we need a wrapper object around our event bodies so we can record things like the number
					// 	of attempts to process a given event, last event time, and whether to keep trying indefinitely
					//  or not.

					//TODO check the parent for completeness and if not then check option and ether fail everything or fail this task or complete
				}
			});
		} catch (NullPointerException e) {
			log.error("Null Pointer Exception {}: {}", e.toString(), body.getValue("id"));
			handler.handle(Future.failedFuture(e));
		}
		catch (Throwable t) {
			log.error("Failed to process failure event for transfer task {}: {}",
					body.getValue("id"), t.getMessage());
			handler.handle(Future.failedFuture(t));
		}
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

}

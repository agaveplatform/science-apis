package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.*;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;

public class TransferTaskErrorFailureHandler extends AbstractNatsListener implements Handler<RoutingContext> {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskErrorFailureHandler.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	private TransferTaskDatabaseService dbService;
	public Connection nc;
	public TransferTaskErrorFailureHandler() throws IOException, InterruptedException {
		super();
	}

	public TransferTaskErrorFailureHandler(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
	}

	public TransferTaskErrorFailureHandler(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

//	public Connection getConnection(){return nc;}

	public void handle(RoutingContext context){
		Throwable thrown = context.failure();
		recordError(thrown);
		context.response().setStatusCode(500).end();
	}

	private void recordError(Throwable throwable){
		log.info("failed: {}", throwable.getMessage());
	}

	/**
	 * Mockable method to initialize connection to the database from the pool
	 * @return {@link TransferTaskDatabaseService} connection to the database
	 */
	public TransferTaskDatabaseService createDatabaseConnection(){
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		return TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);
	}


	@Override
	public void start(Promise<Void> startPromise) throws IOException, InterruptedException, TimeoutException {
		// init our db connection from the pool
		dbService = createDatabaseConnection();

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
		} catch (Exception e) {
			log.error("TRANSFER_FAILED - Exception {}", e.getMessage());
			startPromise.tryFail(e);
		}
	}

	protected void handleMessage(Message message) {
		try {
			JsonObject body = new JsonObject(message.getMessage());
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);
			getVertx().<Boolean>executeBlocking(
					promise -> {
						try {
							processFailure(body, repl -> {
								if (repl.succeeded()) {
									promise.complete(repl.result());
								} else {
									promise.fail(repl.cause());
								}
							});
						} catch (Exception e) {
							log.debug(e.getMessage());
						}
					},
					resp -> {
						if (resp.succeeded()) {
							log.debug("Finished processing transfer task failure {}", uuid);
						} else {
							log.debug("Failed  processing transfer task failure {}", uuid);
						}
					});
		} catch (DecodeException e) {
			log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
		} catch (Throwable t) {
			log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
		}
	}

	protected void processFailure(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
		try {
			body.remove("cause");
			body.remove("message");

			this.processBody(body, processBodyResult ->{
                log.debug(body.encode());
                TransferTask failedTask = new TransferTask(processBodyResult.result());
                failedTask.setStatus(TransferStatusType.FAILED);

                if (processBodyResult.succeeded()){
                    getDbService().update(failedTask.getTenantId(), failedTask.getUuid(), failedTask, updateBody -> {
                        if (updateBody.succeeded()) {
                            TransferTask savedTask = new TransferTask(updateBody.result());
                            log.info("Transfer task {} successfully marked as {}.", failedTask.getUuid(), savedTask.getStatus().name());
                            handler.handle(Future.succeededFuture(Boolean.TRUE));
                        } else {
                            String msg = String.format("Unable to update status of failed transfer task %s. %s",
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
                } else {
                    handler.handle(Future.failedFuture(processBodyResult.cause()));
                }
        });
    } catch (NullPointerException e) {
			log.error("Null Pointer Exception for transfer task {}: {}", body.getValue("id"), e);
			handler.handle(Future.failedFuture(e));
		}
		catch (Throwable t) {
			log.error("Failed to process failure event for transfer task {}: {}",
					body.getValue("id"), t.getMessage());
			handler.handle(Future.failedFuture(t));
		}
	}

	/**
	 * Process {@code body} to handle both partial and complete {@link TransferTask} objects
	 *
	 * @param body {@link JsonObject} containing either an ID or {@link TransferTask} object
	 * @param handler  the handler to resolve with {@link JsonObject} of a {@link TransferTask}
	 */
	public void processBody(JsonObject body, Handler<AsyncResult<JsonObject>> handler) {
        try {
            TransferTask transfer = new TransferTask(body);
            handler.handle(Future.succeededFuture(transfer.toJson()));
        } catch (Exception e) {
            getDbService().getById(body.getString("id"), result -> {
                if (result.succeeded()) {
                    handler.handle(Future.succeededFuture(result.result()));
                } else {
                    handler.handle((Future.failedFuture(result.cause())));
                }
            });
        }
    }

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

}

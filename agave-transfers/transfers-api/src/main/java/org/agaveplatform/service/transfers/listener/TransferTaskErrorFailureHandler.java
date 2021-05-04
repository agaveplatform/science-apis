package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;

public class TransferTaskErrorFailureHandler extends AbstractNatsListener implements Handler<RoutingContext> {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskErrorFailureHandler.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	private TransferTaskDatabaseService dbService;
	public Connection nc;
	public TransferTaskErrorFailureHandler() throws IOException, InterruptedException {
		super();
		setConnection();
	}

	public TransferTaskErrorFailureHandler(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
		setConnection();
	}

	public TransferTaskErrorFailureHandler(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
		setConnection();
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	public Connection getConnection(){return nc;}

	public void setConnection() throws IOException, InterruptedException {
		try {
			nc = _connect(CONNECTION_URL);
		} catch (IOException e) {
			//use default URL
			nc = _connect();
		}
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
	public void start() throws IOException, InterruptedException, TimeoutException {
		//EventBus bus = vertx.eventBus();
		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		//final String err ;
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		//Connection nc = _connect();
		Dispatcher d = getConnection().createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(MessageType.TRANSFER_FAILED, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;

			log.info("Transfer task {} failed: {}: {}",
					body.getString("uuid"), body.getString("cause"), body.getString("message"));

			processFailure(body, resp -> {
				if (resp.succeeded()) {
					log.debug("Completed processing {} event for transfer task {}", getEventChannel(), body.getString("uuid"));
					body.put("event", this.getClass().getName());
					body.put("type", getEventChannel());
					try {
						_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_NOTIFICATION, body);
					} catch (Exception e) {
						log.debug(e.getMessage());
					}
				} else {
					log.error("Unable to process {} event for transfer task message: {}", getEventChannel(), body.encode(), resp.cause());
				}
			});
		});
		d.subscribe(MessageType.TRANSFER_FAILED);
		getConnection().flush(Duration.ofMillis(500));
		//d.unsubscribe(EVENT_CHANNEL);
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
			log.error("Null Pointer Exception {}: {}", e.toString(), body.getValue("id"));
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

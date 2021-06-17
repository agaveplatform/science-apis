package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.iplantc.service.common.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.COMPLETED;

public class TransferTaskHealthcheckListener extends AbstractNatsListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferTaskHealthcheckListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();
	public Connection nc;
	protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK;

	public TransferTaskHealthcheckListener() throws IOException, InterruptedException {
		super();
	}

	public TransferTaskHealthcheckListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
	}

	public TransferTaskHealthcheckListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
	}

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

//	public Connection getConnection(){return nc;}

	@Override
	public void start() throws IOException, InterruptedException, TimeoutException {

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        setDbService(TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue));

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
		} catch (Exception e) {
			logger.error("TRANSFER_ALL - Exception {}", e.getMessage());
		}

//		// listen for healthcheck events to determine if a task is complete
//		// before its transfertask_completed event was received.
//		//getVertx().eventBus().<JsonObject>consumer(TRANSFERTASK_HEALTHCHECK, msg -> {
//		//Connection nc = _connect();
//		Dispatcher d = getConnection().createDispatcher((msg) -> {});
//		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
//		Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
//			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
//			String response = new String(msg.getData(), StandardCharsets.UTF_8);
//			JsonObject body = new JsonObject(response) ;
//			String uuid = body.getString("uuid");
//			String source = body.getString("source");
//			String dest = body.getString("dest");
//			//msg.reply(TransferTaskHealthcheckListener.class.getName() + " received.");
//
//			logger.info("Performing healthcheck on transfer task {}", uuid);
//
//            this.processAllChildrenCanceledEvent(body);
//
//		});
//		d.subscribe(EVENT_CHANNEL);
//		getConnection().flush(Duration.ofMillis(500));

	}

	protected void handleMessage(Message message) {
		try {
			JsonObject body = new JsonObject(message.getMessage());
			String uuid = body.getString("uuid");

			getVertx().<Boolean>executeBlocking(
					promise -> {
						processAllChildrenCanceledEvent(body, repl -> {
							if (repl.succeeded()) {
								promise.complete(repl.result());
							} else {
								promise.fail(repl.cause());
							}
						});
					},
					resp -> {
						if (resp.succeeded()) {
							logger.debug("Finished processing health check for transfer task {}", uuid);
						} else {
							logger.debug("Failed  processing health check for transfer task {}", uuid);
						}
					});


		} catch (DecodeException e) {
			logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
		} catch (Throwable t) {
			logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
		}
	}


	/**
	 * Handles processing of root transfer task health check. Children are looked up to find any still active. If found,
	 * then we leave the task be provided it is not over 30 days old. Expired tasks are killed and retried.  If no
	 * children are active, then this task is marked as completed and the proper event thrown. The handler will resolve
	 * true unless a task needs to be killed, or an update fails.
 	 * @param body the message body, should be a serialized transfer task
	 * @return promise with boolean success
	 */
    public void processAllChildrenCanceledEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        logger.trace("Got into TransferTaskHealthcheckListener.processEvent");
        String uuid = body.getString("uuid");
        String tenantId = body.getString("tenant_id");
        Instant lastUpdated = body.getInstant("last_updated");

        logger.debug("Checking child status of active transfer task {}", uuid);
		getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, reply -> {
			if (reply.succeeded()) {
				if (reply.result()) {
					logger.info("Transfer task {} has no active child tasks. Updating to {}.", uuid, COMPLETED.name());
					getDbService().updateStatus(tenantId, uuid, COMPLETED.name(), updateStatus -> {
						if (updateStatus.succeeded()) {
							logger.debug("Transfer task {} updated to {}", uuid, COMPLETED.name());
							_doPublishEvent(MessageType.TRANSFERTASK_FINISHED, updateStatus.result(), handler);
						} else {
							logger.debug("Transfer task {} found completed, but was unable to update its final status to {}.", uuid, COMPLETED.name());
							JsonObject json = new JsonObject()
									.put("cause", updateStatus.cause().getClass().getName())
									.put("message", updateStatus.cause().getMessage())
									.mergeIn(body);
							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, errorResp -> {
								handler.handle(Future.failedFuture(updateStatus.cause()));
							});
						}
					});
				}
				else {
					logger.info("Transfer task {} still has active child tasks.", uuid);
					// we should check for a timeout here and retry if the task has stalled for too long.
					// individual copy times may handle this for us, though, as any URLCopy#copy() operation
					// that takes too long to complete will timeout, fail, and be retried.
					Instant now = Instant.now();
					// over a month without an update, restart it
					if (lastUpdated.plus(30, ChronoUnit.DAYS).isBefore(now)) {
						logger.info("Transfer task {} has not been updated in over 1 month. The task will be errored and retried.", uuid);
						getDbService().updateStatus(tenantId, uuid, TRANSFERTASK_ERROR, updateStatus -> {
							if (updateStatus.succeeded()) {
								logger.debug("Transfer task {} updated to {}.", uuid, TRANSFERTASK_ERROR);
								_doPublishEvent(MessageType.TRANSFERTASK_ERROR, updateStatus.result(), handler);
							} else {
								logger.debug("Failed to update transfer task {} to {}.", uuid, TRANSFERTASK_ERROR);
								JsonObject json = new JsonObject()
										.put("cause", updateStatus.cause().getClass().getName())
										.put("message", updateStatus.cause().getMessage())
										.mergeIn(body);
								_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, publishEventResp -> {
									handler.handle(Future.failedFuture(updateStatus.cause()));
								});
							}
						});
					}
					// over a day, log and move on
					else {
						if (lastUpdated.plus(1, ChronoUnit.DAYS).isBefore(now)) {
							logger.info("Transfer task {} has not been updated in over 1 day. Consider restarting the task.", uuid);
						}
						// over an hour, log and move on
						else if (lastUpdated.plus(60, ChronoUnit.MINUTES).isBefore(now)) {
							logger.info("Transfer task {} has not been updated in over 60 minutes. Consider restarting the task.", uuid);
						}
						//
						else {
							logger.info("Transfer task {} has been running less than 60 minutes. No reason to be concerned at this point.", uuid);
						}
						// succeeded on tasks that are still valid
						handler.handle(Future.succeededFuture(true));
					}
				}
			} else {
				logger.error("Failed to check child status of transfer task {}. Task remains active: {}",
						uuid, reply.cause().getMessage());

				handler.handle(Future.failedFuture(reply.cause()));
			}
		});
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


}

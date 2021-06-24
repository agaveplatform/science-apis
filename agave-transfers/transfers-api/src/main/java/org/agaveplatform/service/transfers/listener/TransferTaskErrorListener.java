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
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.ObjectNotFoundException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_ACK;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;

public class TransferTaskErrorListener extends AbstractNatsListener {
	protected static final Logger log = LoggerFactory.getLogger(TransferTaskErrorListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ERROR;

	protected String eventChannel = MessageType.TRANSFERTASK_ERROR;
	private Connection nc;

	protected static final List<String> RECOVERABLE_EXCEPTION_CLASS_NAMES = List.of(
			RemoteDataException.class.getName(), // failure to connect to remote system warrants a retry
			IOException.class.getName(), // failure to read from remote or write to local warrants a retry
			InterruptedException.class.getName() // interrupted tasks warrant a retry as it may have been due to a worker shutdown.
	);

	public TransferTaskErrorListener() throws IOException, InterruptedException {
		super();
	}

	public TransferTaskErrorListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
	}

	public TransferTaskErrorListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

//	public Connection getConnection(){return nc;}


	private TransferTaskDatabaseService dbService;
	@Override
	public void start() throws IOException, InterruptedException, TimeoutException {
		//EventBus bus = vertx.eventBus();

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
		} catch (Exception e) {
			log.error("TRANSFER_ALL - Exception {}", e.getMessage());
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
							processError(body, repl -> {
								if (repl.succeeded()) {
									promise.complete(repl.result());
								} else {
									promise.fail(repl.cause());
								}
							});
						} catch (IOException | InterruptedException e) {
							log.debug(e.getMessage());
						}
					},
					resp -> {
						if (resp.succeeded()) {
							log.debug("Finished processing health check for transfer task {}", uuid);
						} else {
							log.debug("Failed  processing health check for transfer task {}", uuid);
						}
					});

		} catch (DecodeException e) {
			log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
		} catch (Throwable t) {
			log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
		}
	}
	protected void processError(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
		try {
			TransferTask tt = new TransferTask(body);
			log.debug(body.encode());
			String cause = body.getString("cause");
			String message = body.getString("message", "");
			int maxTries = config().getInteger(TRANSFERTASK_MAX_ATTEMPTS, Settings.MAX_STAGING_RETRIES);
			//String status = body.getString("COMPLETED", null);
			String tenantId = body.getString("tenant_id");
			String uuid = body.getString("uuid");

			log.error("Tenant ID is {}", tenantId);

            // update dt DB status here
            getDbService().getByUuid(tenantId, uuid, getByIdReply -> {
                if (getByIdReply.succeeded()) {
                    if (getByIdReply.result() != null) {
                        TransferTask errorTask = new TransferTask(getByIdReply.result());

							// check to see if the task was canceled so we don't retry an interrupted task
							if (taskIsNotInterrupted(tt)) {
								// see if the error in the event is recoverable or not
								if (cause != null && getRecoverableExceptionsClassNames().contains(cause)) {
									// now check its status
									if (tt.getStatus().isActive()) {
										// check the retry count on the transfer task. if it has not yet tapped out, examine the error to see if
										if (errorTask.getAttempts() <= maxTries) {
											getDbService().updateStatus(tenantId, uuid, TransferStatusType.ERROR.name(), updateStatusResult -> {
												if (updateStatusResult.succeeded()) {
													log.error("Transfer task {} experienced a non-terminal error and will be retried. The error was {}", tt.getUuid(), message);
													_doPublishEvent(TRANSFER_RETRY, updateStatusResult.result(), handler);
												} else {
													String msg = String.format("Error updating status of transfer task %s to ERROR. %s",
															uuid, updateStatusResult.cause().getMessage());
													// write to error queue. we can retry
													doHandleError(updateStatusResult.cause(), msg, body, handler);
												}
											});
										} else {
											log.info("Maximum attempts have been exceeded for transfer task {}. No further retries will be attempted.",
													tt.getUuid());
											getDbService().updateStatus(tenantId, uuid, TransferStatusType.FAILED.name(), updateStatusResult -> {
												if (updateStatusResult.succeeded()) {
													log.error("Updated status of transfer task {} to FAILED. No retires will be attempted.", tt.getUuid());
													handler.handle(Future.succeededFuture(true));
												} else {
													String msg = String.format("Error updating status of transfer task %s to FAILED. %s",
															uuid, updateStatusResult.cause().getMessage());
													// write to error queue. we can retry
													doHandleError(updateStatusResult.cause(), msg, body, handler);
												}
											});
										}
									} else {
										// skip any new message as the task was already done, so this was a redundant operation
										log.info("Skipping retry of transfer task {} as it is already in a terminal state.", uuid);
										handler.handle(Future.succeededFuture(false));
										return;
									}
								} else {
									log.info("Unrecoverable exception occurred while processing transfer task {}. " +
											"No further retries will be attempted.", tt.getUuid());
									// TODO: support failure policy so we can continue if some files/folders failed to transfer
									getDbService().updateStatus(tenantId, uuid, TransferStatusType.FAILED.name(), updateStatusResult -> {
										if (updateStatusResult.succeeded()) {
											log.error("Updated status of transfer task {} to FAILED. No retires will be attempted.", tt.getUuid());
											handler.handle(Future.succeededFuture(true));
										} else {
											String msg = String.format("Error updating status of transfer task %s to FAILED. %s",
													uuid, updateStatusResult.cause().getMessage());
											// write to error queue. we can retry
											doHandleError(updateStatusResult.cause(), msg, body, handler);
										}
									});
								}
							} else {
								// task was interrupted, so don't attempt a retry
								// TODO: handle pause and cancelled interupts independently here
								log.info("Skipping error processing of transfer task {} due to interrupt event.", tt.getUuid());
								getDbService().updateStatus(tenantId, uuid, TransferStatusType.CANCELLED.name(), updateStatusResult -> {
									if (updateStatusResult.succeeded()) {
										log.error("Updated status of transfer task {} to CANCELLED after interrupt received. No retires will be attempted.", uuid);
										_doPublishEvent(TRANSFERTASK_CANCELED_ACK, updateStatusResult.result(), handler);
									} else {
										String msg = String.format("Error updating status of transfer task %s to CANCELLED. %s",
												uuid, updateStatusResult.cause().getMessage());
										// write to error queue. we can retry
										doHandleError(updateStatusResult.cause(), msg, body, handler);
									}
								});
							}
					}
                    else {
						String msg = String.format("getByIdReply.result() was null %s ",
								uuid);
						log.error(msg);
						handler.handle(Future.failedFuture(new ObjectNotFoundException(msg)));
						//doHandleError(getByIdReply.cause(), msg, body, handler);
					}
				} else {
					String msg = String.format("Error fetching current state of transfer task %s   %s",
							uuid, getByIdReply.cause().getMessage());
					// write to error queue. we can retry, but we need a circuite breaker here at some point to avoid
					// an infinite loop.
					doHandleError(getByIdReply.cause(), msg, body, handler);
				}
			});
		} catch (Throwable t) {
			// fail if there are any issues
			doHandleFailure(t, t.getMessage(), body, handler);
		}
	}

    /**
     * Process {@code body} to handle both partial and complete {@link TransferTask} objects
     *
     * @param body {@link JsonObject} containing either an ID or {@link TransferTask} object
     * @param handler  the handler to resolve with {@link JsonObject} of a {@link TransferTask}
     */
    protected void processBody(JsonObject body, Handler<AsyncResult<JsonObject>> handler) {
        try {
			String cause = body.getString("cause");
			String message = body.getString("message", "");

            TransferTask transfer = new TransferTask(body);
            handler.handle(Future.succeededFuture(transfer.toJson().put("cause", cause).put("message", message)));
        } catch (Exception e) {
            getDbService().getById(body.getString("id"), result -> {
                if (result.succeeded()) {
                    handler.handle(Future.succeededFuture(result.result()));
                } else {
                    log.error("{} - unable to get by id body: {}", this.getClass().getName(), body);
                    handler.handle((Future.failedFuture(result.cause())));
                }
            });
        }
    }

    /**
     * Returns the class names of all the exceptions that could cause a transient failure and justify a retry of
     * the failed {@link TransferTask}.
     *
     * @return a list of class names
     */
    protected List<String> getRecoverableExceptionsClassNames() {
        return RECOVERABLE_EXCEPTION_CLASS_NAMES;
    }

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}


}

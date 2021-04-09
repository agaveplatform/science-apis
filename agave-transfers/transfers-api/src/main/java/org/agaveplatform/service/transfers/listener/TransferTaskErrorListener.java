package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

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
		setConnection();
	}

	public TransferTaskErrorListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
		setConnection();
	}

	public TransferTaskErrorListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
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
			nc = _connect(Options.DEFAULT_URL);
		}
	}

	private TransferTaskDatabaseService dbService;
	@Override
	public void start() throws IOException, InterruptedException, TimeoutException {
		//EventBus bus = vertx.eventBus();

		//final String err ;
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		//Connection nc = _connect();
		Dispatcher d = getConnection().createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(MessageType.TRANSFERTASK_ERROR, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");

			// init our db connection from the pool
			String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
			dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

            try {
                processBody(body, processBodyResult -> {
                    log.info("Transfer task {} error: {}: {}",
                            body.getString("uuid"), body.getString("cause"), body.getString("message"));

                    if (processBodyResult.succeeded()) {
						try {
							processError(body, resp -> {
								if (resp.succeeded()) {
									log.debug("Completed processing {} event for transfer task {}", getEventChannel(), body.getString("uuid"));
								} else {
									log.error("Unable to process {} event for transfer task (TTEL) message: {}", getEventChannel(), body.encode(), resp.cause());
									try {
										_doPublishEvent(TRANSFER_FAILED, body);
									} catch (IOException e) {
										log.debug(e.getMessage());
									} catch (InterruptedException e) {
										log.debug(e.getMessage());
									}
								}
							});
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
					} else {
                        log.error("Error with retrieving Transfer Task {}", body.getString("id"));
                    }
                });
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
		d.subscribe(MessageType.TRANSFERTASK_ERROR);
		getConnection().flush(Duration.ofMillis(500));

		//bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;

			log.error("Transfer task {} failed to check it's parent task {} for completion: {}: {}",
					body.getString("uuid"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));
		});
		d.subscribe(MessageType.TRANSFERTASK_PARENT_ERROR);
		getConnection().flush(Duration.ofMillis(500));
		//d.unsubscribe(MessageType.TRANSFERTASK_PARENT_ERROR);
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

							// check to see if the job was canceled so we don't retry an interrupted task
							if (taskIsNotInterrupted(tt)) {
								// see if the error in the event is recoverable or not
								if (getRecoverableExceptionsClassNames().contains(cause)) {
									// now check its status
									if (tt.getStatus().isActive()) {
										// check the retry count on the transfer task. if it has not yet tapped out, examine the error to see if
										if (errorTask.getAttempts() <= maxTries) {
											getDbService().updateStatus(tenantId, uuid, TransferStatusType.ERROR.name(), updateStatusResult -> {
												if (updateStatusResult.succeeded()) {
													try {
														log.error("Transfer task {} experienced a non-terminal error and will be retried. The error was {}", tt.getUuid(), message);
														_doPublishEvent(TRANSFER_RETRY, updateStatusResult.result());
														handler.handle(Future.succeededFuture(true));
													} catch (IOException e) {
														log.debug(e.getMessage());
													} catch (InterruptedException e) {
														log.debug(e.getMessage());
													}
												} else {
													String msg = String.format("Error updating status of transfer task %s to ERROR. %s",
															uuid, updateStatusResult.cause().getMessage());
													// write to error queue. we can retry
													try {
														doHandleError(updateStatusResult.cause(), msg, body, handler);
													} catch (IOException e) {
														e.printStackTrace();
													} catch (InterruptedException e) {
														e.printStackTrace();
													}
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
													try {
														doHandleError(updateStatusResult.cause(), msg, body, handler);
													} catch (IOException e) {
														e.printStackTrace();
													} catch (InterruptedException e) {
														e.printStackTrace();
													}
												}
											});
										}
									} else {
										// skip any new message as the task was already done, so this was a redundant operation
										log.info("Skipping retry of transfer task {} as the job was already in a terminal state.", uuid);
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
											try {
												doHandleError(updateStatusResult.cause(), msg, body, handler);
											} catch (IOException e) {
												e.printStackTrace();
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
										}
									});
								}
							} else {
								// task was interrupted, so don't attempt a retry
								// TODO: handle pause and cancelled interupts independently here
								log.info("Skipping error processing of transfer task {} due to interrupt event.", tt.getUuid());
								getDbService().updateStatus(tenantId, uuid, TransferStatusType.CANCELLED.name(), updateStatusResult -> {
									if (updateStatusResult.succeeded()) {
										try {
											log.error("Updated status of transfer task {} to CANCELLED after interrupt received. No retires will be attempted.", uuid);
											_doPublishEvent(TRANSFERTASK_CANCELED_ACK, updateStatusResult.result());
											handler.handle(Future.succeededFuture(true));
										} catch (IOException e) {
											log.debug(e.getMessage());
										} catch (InterruptedException e) {
											log.debug(e.getMessage());
										}
									} else {
										String msg = String.format("Error updating status of transfer task %s to CANCELLED. %s",
												uuid, updateStatusResult.cause().getMessage());
										// write to error queue. we can retry
										try {
											doHandleError(updateStatusResult.cause(), msg, body, handler);
										} catch (IOException e) {
											e.printStackTrace();
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
									}
								});
							}
					}else {
						String msg = String.format("getByIdReply.result() was null %s ",
								uuid);
						log.error(msg);
						//doHandleError(getByIdReply.cause(), msg, body, handler);
					}
				} else {
					String msg = String.format("Error fetching current state of transfer task %s   %s",
							uuid, getByIdReply.cause().getMessage());
					// write to error queue. we can retry, but we need a circuite breaker here at some point to avoid
					// an infinite loop.
					log.error(msg);
					try {
						doHandleError(getByIdReply.cause(), msg, body, handler);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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

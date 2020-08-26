package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
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
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferTaskErrorListener extends AbstractTransferTaskListener {
	protected static final Logger log = LoggerFactory.getLogger(TransferTaskErrorListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ERROR;

	protected String eventChannel = MessageType.TRANSFERTASK_ERROR;

	protected static final List<String> RECOVERABLE_EXCEPTION_CLASS_NAMES = List.of(
			RemoteDataException.class.getName(), // failure to connect to remote system warrants a retry
			IOException.class.getName(), // failure to read from remote or write to local warrants a retry
			InterruptedException.class.getName() // interrupted tasks warrant a retry as it may have been due to a worker shutdown.
	);

	public TransferTaskErrorListener() { super(); }

	public TransferTaskErrorListener(Vertx vertx) {
		super(vertx);
	}

	public TransferTaskErrorListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	private TransferTaskDatabaseService dbService;

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();

		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			// init our db connection from the pool
			String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
			dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

			log.info("Transfer task {} error: {}: {}",
					body.getString("uuid"), body.getString("cause"), body.getString("message"));

			try {
				processError(body, resp -> {
					if (resp.succeeded()) {
						log.debug("Completed processing {} event for transfer task {}", getEventChannel(), body.getString("uuid"));
					} else {
						log.error("Unable to process {} event for transfer task message: {}", getEventChannel(), body.encode(), resp.cause());
						_doPublishEvent(TRANSFER_FAILED, body);
					}
				});
			}catch (Exception e){
				log.error(e.getMessage());
			}
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
			JsonObject body = msg.body();

			log.error("Transfer task {} failed to check it's parent task {} for copmletion: {}: {}",
					body.getString("uuid"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));
		});
	}

	protected void processError(JsonObject body, Handler<AsyncResult<Boolean>> handler){
		try {
			TransferTask tt = new TransferTask(body);

			String cause = body.getString("cause");
			String message = body.getString("message", "");
			int maxTries = config().getInteger(TRANSFERTASK_MAX_ATTEMPTS, Settings.MAX_STAGING_RETRIES);
			//String status = body.getString("COMPLETED", null);
			String tenantId = body.getString("tenantId");
			String uuid = body.getString("uuid");

			// update dt DB status here
			getDbService().getById(tenantId, uuid, getByIdReply -> {
				if (getByIdReply.succeeded()) {
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
											log.error("Transfer task {} experienced a non-terminal error and will be retried. The error was {}", tt.getUuid(), message);
											_doPublishEvent(TRANSFER_RETRY, updateStatusResult.result());
											handler.handle(Future.succeededFuture(true));
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
								_doPublishEvent(TRANSFERTASK_CANCELED_ACK, updateStatusResult.result());

								handler.handle(Future.succeededFuture(true));
							} else {
								String msg = String.format("Error updating status of transfer task %s to CANCELLED. %s",
										uuid, updateStatusResult.cause().getMessage());
								// write to error queue. we can retry
								doHandleError(updateStatusResult.cause(), msg, body, handler);
							}
						});
					}

				} else {
					String msg = String.format("Error fetching current state of transfer task %s. %s",
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
	 * Returns the class names of all the exceptions that could cause a transient failure and justify a retry of
	 * the failed {@link TransferTask}.
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

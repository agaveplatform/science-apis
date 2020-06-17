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
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_TRIES;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferErrorListener extends AbstractTransferTaskListener {
	protected static final Logger log = LoggerFactory.getLogger(TransferErrorListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ERROR;

	protected String eventChannel = MessageType.TRANSFERTASK_ERROR;

	public TransferErrorListener() { super(); }

	public TransferErrorListener(Vertx vertx) {
		super(vertx);
	}

	public TransferErrorListener(Vertx vertx, String eventChannel) {
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

			String cause = body.getString("cause", null);
			String message = body.getString("message", "");
			int attempts = body.getInteger("attempts", Integer.MAX_VALUE);
			int maxTries = config().getInteger(TRANSFERTASK_MAX_TRIES, Settings.MAX_STAGING_RETRIES);
			//String status = body.getString("COMPLETED", null);
			String tenantId = body.getString("tenantId", null);
			String uuid = body.getString("uuid", null);

			// update dt DB status here
			getDbService().updateStatus(tenantId, uuid, TransferStatusType.ERROR.toString(), updateReply -> {
				if (updateReply.succeeded()) {
					Future.succeededFuture(Boolean.TRUE);
				} else {
					// update failed
					Future.succeededFuture(Boolean.FALSE);
				}
			});

			// check the retry count on the transfer task. if it has not yet tapped out, examine the error to see if
			// we should retry the transfer
			if (attempts <= maxTries) {
				if (List.of(RemoteDataException.class.getName(),
						IOException.class.getName(),
						InterruptedException.class.getName()).contains(cause)) {
					// check to see if the job was canceled so we don't retry a cancelled task
					if (taskIsNotInterrupted(tt)) {
						// now check its status
						if (tt.getStatus().isActive()) {
							log.error("Transfer task {} experienced a non-terminal error and will be retried. The error was {}", tt.getUuid(), message);
							_doPublishEvent(TRANSFER_RETRY, body);
							handler.handle(Future.succeededFuture(true));
							return;
						} else {
							// skip any new message as the task was already done, so this was a redundant operation
							log.info("Skipping retry of transfer task {} as the job was already in a terminal state.", tt.getUuid());
							handler.handle(Future.succeededFuture(false));
						}
					} else {
						// task was interrupted, so don't attempt a retry
						log.info("Skipping retry of transfer task {} due to interrupt event.", tt.getUuid());
					}
				} else {
					log.info("Unrecoverable exception occurred while processing transfer task {}. " +
							"No further retries will be attempted.", tt.getUuid());
				}
			} else {
				log.info("Maximum attempts have been exceeded for transfer task {}. No further retries will be attempted.",
						tt.getUuid());
			}
			// anything getting to this point will not be retried and was not already done. we fail the transfer task
			// at this point with no expectation of futher efort to recover.
			_doPublishEvent(TRANSFER_FAILED, body);
			handler.handle(Future.succeededFuture(false));
		} catch (Throwable t) {
			// fail the processing if there is any kind of issue
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

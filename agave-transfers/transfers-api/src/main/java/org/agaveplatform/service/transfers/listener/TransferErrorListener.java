package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_TRIES;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_FAILED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;

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

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			log.error("Transfer task {} failed: {}: {}",
					body.getString("uuid"), body.getString("cause"), body.getString("message"));

			_doPublishEvent(MessageType.NOTIFICATION_TRANSFERTASK, body);

			boolean result = processError(body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
			JsonObject body = msg.body();

			log.error("Transfer task {} failed to check it's parent task {} for copmletion: {}: {}",
					body.getString("uuid"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));

		});
	}

	protected boolean processError(JsonObject body){
		//String id = body.getString("id");
		String cause = body.getString("cause");
		String message = body.getString("message");
		int attempts = body.getInteger("attempts");

		TransferTask tt = new TransferTask();
		tt.setUuid(body.getString("uuid"));
		tt.setStatus(TransferStatusType.valueOf(body.getString("status")));
		tt.setParentTaskId(body.getString("parentTaskId"));
		tt.setRootTaskId(body.getString("rootTaskId"));

		//int maxTries = 3;
		int maxTries = config().getInteger(TRANSFERTASK_MAX_TRIES, 3);

		// check the retry count on the transfer task. if it has not yet tapped out, exame the error to see if
		// we should retry the transfer
		if ( attempts <= maxTries ) {
			if (body.getString("cause").equals(RemoteDataException.class.getName()) ||
					body.getString("cause").equals(IOException.class.getName()) ||
					body.getString("cause").equals(InterruptedException.class.getName())) {
				// check to see if the job was canceled so we don't retry a cancelled task
				if (taskIsNotInterrupted(tt)) {
					// now check its status
					if (tt.getStatus().isActive()) {
						log.error("Transfer task {} experienced a non-terminal error and will be retried. The error was {}", tt.getUuid(), message);
						_doPublishEvent(TRANSFER_RETRY, body);
						return true;
					}
				} else {
					// task was interrupted, so don't attempt a retry
					log.info("Skipping retry of transfer task {} due to interrupt event.", body.getString("uuid"));
				}
			} else {
				log.info("Unrecoverable exception occurred while processing transfer task {}. " +
								"No further retries will be attempted.", body.getString("uuid"));
			}
		} else {
			log.info("Maximum attempts have been exceeded for transfer task {}. No further retries will be attempted.",
				body.getString("uuid"));
		}

		_doPublishEvent(TRANSFER_FAILED, body);
		return false;
	}
}

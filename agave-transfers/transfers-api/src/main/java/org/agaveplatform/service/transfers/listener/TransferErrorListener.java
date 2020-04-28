package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.mvel2.templates.TemplateRuntimeError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_FAILED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferErrorListener extends AbstractTransferTaskListener {
	protected final Logger log = LoggerFactory.getLogger(TransferErrorListener.class);
	//private TransferTaskDatabaseService dbService;
	protected String eventChannel = MessageType.TRANSFERTASK_ERROR;

	public TransferErrorListener(Vertx vertx) {
		super(vertx);
	}

	public TransferErrorListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ERROR;

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
					body.getString("id"), body.getString("cause"), body.getString("message"));

			_doPublishEvent(MessageType.NOTIFICATION_TRANSFERTASK, body);

			boolean result = processError(body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
			JsonObject body = msg.body();

			log.error("Transfer task {} failed to check it's parent task {} for copmletion: {}: {}",
					body.getString("id"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));

			//boolean result = processError(body);
		});
	}

	protected boolean processError(JsonObject body){
		//String id = body.getString("id");
		String cause = body.getString("cause");
		String message = body.getString("message");
		String status = body.getString("status");
		int attempts = body.getInteger("attempts");

		if ( attempts <= config().getInteger("transfertask.max.tries") ) {
			if (body.getString("cause").equals(RemoteDataException.class.getName()) ||
					body.getString("cause").equals(IOException.class.getName()) ||
					body.getString("cause").equals(InterruptedException.class.getName())) {

				if (getRetryStatus(status)) {
					//log.error("TransformErrorListener will retry this error {} processing an error.  The error was {}", cause, message);
					_doPublishEvent(TRANSFER_RETRY, body);
					return true;
				}
			}
		}

		_doPublishEvent(TRANSFER_FAILED, body);
		return false;
	}

	protected boolean getRetryStatus(String status){
		boolean statusBool = true;
		switch (status) {
			case "CANCELLED":
				statusBool = false;
				break;
			case "COMPLETED":
				statusBool = false;
				break;
			case "FAILED":
				statusBool = false;
				break;
		}
		return statusBool;
	}
}

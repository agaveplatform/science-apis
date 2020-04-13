package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;

import java.util.HashMap;

public class FileTransferCreateServiceImpl extends AbstractTransferTaskListener implements FileTransferService {

	private final HashMap<String, Double> lastValues = new HashMap<>();

	public FileTransferCreateServiceImpl(Vertx vertx) {
		super(vertx);
	}

	public FileTransferCreateServiceImpl(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.FILETRANSFER_SFTP.getEventChannel();

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}


	@Override
	public void start() {
		vertx.eventBus().<JsonObject>consumer(getEventChannel(), message -> {
			JsonObject json = message.body();
			lastValues.put(json.getString("id"), json.getDouble("temp"));
		});
	}

	@Override
	public void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		//TODO put create dir commands here
	}

	@Override
	public void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {

		// TODO put creat file command here and copy file
	}

}

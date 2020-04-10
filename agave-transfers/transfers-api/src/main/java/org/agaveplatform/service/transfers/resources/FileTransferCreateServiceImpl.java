package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;

public class FileTransferCreateServiceImpl extends AbstractVerticle implements FileTransferService {

	private final HashMap<String, Double> lastValues = new HashMap<>();
	private String eventChannel = "transfertask.sftp";

	public FileTransferCreateServiceImpl(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	public FileTransferCreateServiceImpl(Vertx vertx) {
		vertx.eventBus().<JsonObject>consumer("filetransfer.sftp", message -> {
			JsonObject json = message.body();
			lastValues.put(json.getString("id"), json.getDouble("temp"));
		});
	}

	@Override
	public void start() {
		System.out.println("hi");
	}
	@Override
	public void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		//TODO put create dir commands here
	}

	@Override
	public void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {

		// TODO put creat file command here and copy file
	}

	/**
	 * Sets the vertx instance for this listener
	 *
	 * @param vertx the current instance of vertx
	 */
	private void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getEventChannel() {
		return eventChannel;
	}

	/**
	 * Sets the message type for which to listen
	 *
	 * @param eventChannel
	 */
	public void setEventChannel(String eventChannel) {
		this.eventChannel = eventChannel;
	}
}

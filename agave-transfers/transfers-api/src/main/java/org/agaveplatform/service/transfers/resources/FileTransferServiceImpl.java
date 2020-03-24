package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.stream.Collectors;

public class FileTransferServiceImpl  implements FileTransferService {

	private final HashMap<String, Double> lastValues = new HashMap<>();

	public FileTransferServiceImpl(Vertx vertx) {
		vertx.eventBus().<JsonObject>consumer("filetransfer.sftp", message -> {
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

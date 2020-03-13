package main.java.org.agaveplatform.service.transfers.streaming;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.iplantc.vertx.fileTransfer.FileTrasferService;

import java.util.HashMap;
import java.util.stream.Collectors;

public class StramingFileTaskImpl  implements FileTrasferService {

	private final HashMap<String, Double> lastValues = new HashMap<>();

	public StramingFileTaskImpl(Vertx vertx) {
		vertx.eventBus().<JsonObject>consumer("filetransfer.sftp", message -> {
			JsonObject json = message.body();
			lastValues.put(json.getString("id"), json.getDouble("temp"));
		});
	}

	@Override
	public void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		double avg = lastValues.values().stream()
				.collect(Collectors.averagingDouble(Double::doubleValue));
		jsonObject = new JsonObject().put("average", avg);
		handler.handle(Future.succeededFuture(jsonObject));
	}

	@Override
	public void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		double avg = lastValues.values().stream()
				.collect(Collectors.averagingDouble(Double::doubleValue));
		// todo
//		Long logicalFileId = dataMap.getLong("logicalFileId");
//		cachedFile = dataMap.getString("cachedFile");
//		owner = dataMap.getString("owner");
//		createdBy = dataMap.getString("createdBy");
//		String tenantCode = dataMap.getString("tenantId");
//
//		String uploadSource = dataMap.getString("sourceUrl");
//		String uploadDest = dataMap.getString("destUrl");
//		boolean isRangeCopyOperation = dataMap.getBooleanFromString("isRangeCopyOperation");
//		long rangeIndex = dataMap.getLong("rangeIndex");
//		long rangeSize = dataMap.getLong("rangeSize");



		//=========================================================
		JsonObject data = new JsonObject().put("average", avg);
		handler.handle(Future.succeededFuture(data));
	}

}

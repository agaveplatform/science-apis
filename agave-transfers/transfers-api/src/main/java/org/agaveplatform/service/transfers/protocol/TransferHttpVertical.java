package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferHttpVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferHttpVertical.class);

	public TransferHttpVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferHttpVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = "transfer.http";

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}


	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			TransferTask tt = new TransferTask(body);

			logger.info("Transfer task HTTP {} for source {} and dest {}", uuid, source, dest);
//			bytesTrasfered = this.sftp(tt, source, dest );
//			if (bytesTrasfered > 1) {
//				body.put("total_size", bytesTrasfered);
//				TransferTask bodyTask = new TransferTask(body);
//				vertx.eventBus().publish("transfertask.sftp.complete", bodyTask);
//			}
			processEvent(body);

			});
	}

	public void processEvent(JsonObject body) {
		_doPublishEvent("transfer.completed", body);
//		org.iplantc.service.transfer.model.TransferTask agaveTransferTask = new org.iplantc.service.transfer.model.TransferTask();
		//agaveTransferTask.setId(bodyTask.getId());

//		List<String> exclusions = new ArrayList<String>();
//		logger.info("Cal to URLCopy made here. Source {} Dest {}", srcPath, destPath);
//		TransferTask sftpTask;
//		try {
//			URLCopy urlCopy;
//			Long urlCopy.copy(srcPath, destPath, agaveTransferTask, exclusions);
//		} catch (RemoteDataException e) {
//			logger.error(exclusions.toString());
//			vertx.eventBus().publish("transfertask.error", e.toString());
//		}catch (IOException e){
//			logger.error(e.getStackTrace().toString());
//			vertx.eventBus().publish("transfertask.error", e.toString());
//		}catch (TransferException e){
//			logger.error(e.toString());
//			vertx.eventBus().publish("transfertask.error", e.toString());
//		}catch (ClosedByInterruptException e) {
//			logger.error(e.toString());
//			vertx.eventBus().publish("transfertask.error", e.toString());
//		}
//		if (sftpTask.getTotalSize()!= 0){
//			vertx.eventBus().publish("transfer");
//		}
	}

}

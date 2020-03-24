package org.agaveplatform.service.trasfers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.DefaultTransferTaskAssignedListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.iplantc.service.transfer.URLCopy;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TransferSftpVertical extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(DefaultTransferTaskAssignedListener.class);
	private String eventChannel = "transfertask.sftp.get.*.*";

	public TransferSftpVertical(Vertx vertx) {
		this(vertx, null);
	}

	public TransferSftpVertical(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("id");
			String source = body.getString("source");
			String dest = body.getString("dest");

			TransferTask tt = new TransferTask(body);

			logger.info("Transfer task SFTP {} for source {} and dest {}", uuid, source, dest);
			Long bytesTrasfered = this.sftp(tt, source, dest );
			if (bytesTrasfered > 1) {
				body.put("total_size", bytesTrasfered);
				TransferTask bodyTask = new TransferTask(body);
				vertx.eventBus().publish("transfertask.sftp.complete", bodyTask);
			}

			});
	}

	public Long sftp(TransferTask bodyTask, String srcPath, String destPath) {
		org.iplantc.service.transfer.model.TransferTask agaveTransferTask = new org.iplantc.service.transfer.model.TransferTask();
		//agaveTransferTask.setId(bodyTask.getId());

		List<String> exclusions = new ArrayList<String>();
		logger.info("Cal to URLCopy made here. Source {} Dest {}", srcPath, destPath);
		TransferTask sftpTask;
		try {
			sftpTask = URLCopy.copy(srcPath, destPath, agaveTransferTask, exclusions);
		} catch (RemoteDataException e) {
			logger.error(exclusions.toString());
			vertx.eventBus().publish("transfertask.error", e.toString());
		}catch (IOException e){
			logger.error(e.getStackTrace().toString());
			vertx.eventBus().publish("transfertask.error", e.toString());
		}catch (TransferException e){
			logger.error(e.toString());
			vertx.eventBus().publish("transfertask.error", e.toString());
		}catch (ClosedByInterruptException e) {
			logger.error(e.toString());
			vertx.eventBus().publish("transfertask.error", e.toString());
		}
		if (sftpTask.getTotalSize()!= 0){
			vertx.eventBus().publish("transfer")
		}
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
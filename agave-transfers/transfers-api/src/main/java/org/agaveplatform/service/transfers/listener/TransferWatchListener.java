package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferWatchListener extends AbstractTransferTaskListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferWatchListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_COMPLETED;

	public TransferWatchListener(Vertx vertx) {}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		getVertx().eventBus().<JsonObject>consumer(MessageType.TRANSFERTASK_HEALTHCHECK, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			logger.info("Performing healthcheck on transfer task {}", uuid);

			this.processEvent(body);
		});

		while(true) {
			try {
				getDbService().getActiveRootTaskIds(getActiveRootTaskIds -> {
					if (getActiveRootTaskIds.succeeded()) {
						getActiveRootTaskIds.result().getList().forEach(parentTask -> {
							_doPublishEvent("transfertask.healthcheck", parentTask);
						});
					}
				});
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public Future<Boolean> processEvent(JsonObject body) {
		Promise<Boolean> promise = Promise.promise();

		String uuid = body.getString("uuid");
		String tenantId = body.getString("tenantId");

		getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, reply -> {
			if (reply.succeeded()) {
				if (reply.result()) {
					getDbService().updateStatus(tenantId, uuid, COMPLETED.name(), updateStatus -> {
						if (updateStatus.succeeded()) {
							logger.info("[{}] Transfer task {} updated to completed.", tenantId, uuid);
							_doPublishEvent(MessageType.TRANSFERTASK_COMPLETED, updateStatus.result());
							promise.handle(Future.succeededFuture(Boolean.TRUE));
						} else {
							logger.error("[{}] Task {} completed, but unable to update status: {}",
									tenantId, uuid, reply.cause());
							JsonObject json = new JsonObject()
									.put("cause", updateStatus.cause().getClass().getName())
									.put("message", updateStatus.cause().getMessage())
									.mergeIn(body);
							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
							promise.handle(Future.failedFuture(updateStatus.cause()));
						}
					});
				} else {
					logger.info("[{}] Transfer task {} is still active", tenantId, uuid);
					promise.handle(Future.succeededFuture(Boolean.TRUE));
				}
			} else {
				logger.error("[{}] Failed to check child status of transfer task {}. Task remains active: {}",
						tenantId, uuid, reply.cause().getMessage());
				JsonObject json = new JsonObject()
						.put("cause", reply.cause().getClass().getName())
						.put("message", reply.cause().getMessage())
						.mergeIn(body);
				_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
				promise.handle(Future.failedFuture(reply.cause()));
			}
		});

		return promise.future();
	}


	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}


}

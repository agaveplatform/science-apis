package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.COMPLETED;

public class TransferTaskHealthcheckListener extends AbstractTransferTaskListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferTaskHealthcheckListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK;

	public TransferTaskHealthcheckListener() {
		super();
	}

	public TransferTaskHealthcheckListener(Vertx vertx) {
		super(vertx);
	}

	public TransferTaskHealthcheckListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return this.EVENT_CHANNEL;
	}

	@Override
	public void start() {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		setDbService(TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue));

		// listen for healthcheck events to determine if a task is complete
		// before its transfertask_completed event was received.
		getVertx().eventBus().<JsonObject>consumer(TRANSFERTASK_HEALTHCHECK, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			logger.info("Performing healthcheck on transfer task {}", uuid);

			this.processEvent(body);
		});
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

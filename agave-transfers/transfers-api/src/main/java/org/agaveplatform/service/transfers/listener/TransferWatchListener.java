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
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;


public class TransferWatchListener extends AbstractTransferTaskListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferWatchListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_HEALTHCHECK;

	public TransferWatchListener(Vertx vertx) {
		super(vertx);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		getVertx().setPeriodic(10000, resp -> {
			processEvent();
		});
	}

	public Future<Boolean> processEvent() {
		Promise<Boolean> promise = Promise.promise();

		getDbService().getActiveRootTaskIds(getActiveRootTaskIds -> {
			if (getActiveRootTaskIds.succeeded()) {
				logger.info("Found {} active transfer tasks", getActiveRootTaskIds.result().size());
				getActiveRootTaskIds.result().getList().forEach(parentTask -> {
					logger.debug("[{}] Running health check on transfer task {}",
							((JsonObject)parentTask).getString("tenantId"),
							((JsonObject)parentTask).getString("uuid"));
					_doPublishEvent(TRANSFERTASK_HEALTHCHECK, parentTask);
				});
				promise.handle(Future.succeededFuture(Boolean.TRUE));
			}
			else {
				logger.error("Unable to retrieve list of active transfer tasks: {}", getActiveRootTaskIds.cause());
				promise.handle(Future.failedFuture(getActiveRootTaskIds.cause()));
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

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

	public TransferWatchListener() {
		super();
	}
	public TransferWatchListener(Vertx vertx) {
		super(vertx);
	}
	public TransferWatchListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
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

	/**
	 * Handles generation of health check events for each active transfer task every 10 seconds.
	 * @return future with boolean result of the batch scheduling operation
	 */
	public Future<Boolean> processEvent() {
		Promise<Boolean> promise = Promise.promise();

		logger.debug("Looking up active transfer tasks...");
		getDbService().getActiveRootTaskIds(reply -> {
			if (reply.succeeded()) {
				logger.info("Found {} active transfer tasks", reply.result().size());
				reply.result().getList().forEach(rootTask -> {
					try {
						logger.debug("Scheduling health check on transfer task {}",
								((JsonObject) rootTask).getString("uuid"));
						_doPublishEvent(TRANSFERTASK_HEALTHCHECK, rootTask);
					} catch (Throwable t) {
						logger.error("Failed to schedule health check for transfer task {}", rootTask);
					}
				});
				promise.handle(Future.succeededFuture(Boolean.TRUE));
			}
			else {
				logger.error("Unable to retrieve list of active transfer tasks: {}", reply.cause().getMessage());
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


	public void processInterrupt(JsonObject body, Handler<AsyncResult<Boolean>> handler) {

	}
}

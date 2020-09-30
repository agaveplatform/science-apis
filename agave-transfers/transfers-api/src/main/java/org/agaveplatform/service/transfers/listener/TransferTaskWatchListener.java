package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;


public class TransferTaskWatchListener extends AbstractTransferTaskListener {
	private final static Logger log = LoggerFactory.getLogger(TransferTaskWatchListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<>();

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_HEALTHCHECK;

	public TransferTaskWatchListener() {
		super();
	}
	public TransferTaskWatchListener(Vertx vertx) {
		super(vertx);
	}
	public TransferTaskWatchListener(Vertx vertx, String eventChannel) {
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
			processEvent(batchResp -> {
				if (batchResp.succeeded()) {
					log.debug("Periodic transfer task watch starting");
				} else {
					log.error("Failed to execute the periodic transfer watch task. {}", batchResp.cause().getMessage(), batchResp.cause());
				}
			});
		});
	}

	/**
	 * Handles generation of health check events for each active transfer task every 10 seconds.
	 * @return future with boolean result of the batch scheduling operation
	 */
	public void processEvent(Handler<AsyncResult<Boolean>> handler) {
		log.info("Got into TransferTaskWatchListener.processEvent ");
		try {
			log.debug("Looking up active transfer tasks...");
			getDbService().getActiveRootTaskIds(reply -> {
				if (reply.succeeded()) {
					log.info("Found {} active transfer tasks", reply.result().size());
					reply.result().stream().forEach(jsonResult -> {
						try {
							log.debug("Scheduling health check on transfer task {}",
									((JsonObject)jsonResult).getString("uuid"));
							_doPublishEvent(TRANSFERTASK_HEALTHCHECK, ((JsonObject)jsonResult));
						} catch (Throwable t) {
							log.error("Failed to schedule health check for transfer task {}", jsonResult);
						}
					});
					handler.handle(Future.succeededFuture(Boolean.TRUE));
				} else {
					log.error("Unable to retrieve list of active transfer tasks: {}", reply.cause().getMessage());
					handler.handle(Future.failedFuture(reply.cause()));
				}
			});
		} catch (Exception e) {
			if (e.toString().contains("no null address accepted")){
				log.info("Error with TransferTaskWatchListener processEvent  error ={} }", e.toString());
				handler.handle(Future.succeededFuture(Boolean.TRUE));
			}else
				{
				log.error("Error with TransferTaskWatchListener processEvent  error ={} }",  e.toString());
				handler.handle(Future.failedFuture(e));
			}
		}
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

package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK_PARENT;


public class TransferTaskWatchListener extends AbstractNatsListener {
	private final static Logger log = LoggerFactory.getLogger(TransferTaskWatchListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<>();

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_HEALTHCHECK;

	public TransferTaskWatchListener() throws IOException, InterruptedException {
		super();
	}
	public TransferTaskWatchListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
	}
	public TransferTaskWatchListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
	}
	public Connection nc;
	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}
	public Connection getConnection(){return nc;}

	public void setConnection() throws IOException, InterruptedException {
		try {
			nc = _connect(CONNECTION_URL);
		} catch (IOException e) {
			//use default URL
			nc = _connect(Options.DEFAULT_URL);
		}
	}

	@Override
	public void start() {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		int healthTimer = config().getInteger(MAX_TIME_FOR_HEALTHCHECK, 600000);
		int healthParentTimer = config().getInteger(MAX_TIME_FOR_HEALTHCHECK_PARENT, 600000);

		getVertx().setPeriodic( healthTimer, resp -> {
			processEvent(batchResp -> {
				if (batchResp.succeeded()) {
					log.trace("Periodic transfer task watch starting");
				} else {
					log.error("Failed to execute the periodic transfer watch task. {}", batchResp.cause().getMessage(), batchResp.cause());
				}
			});
		});


		getVertx().setPeriodic(healthParentTimer, resp -> {
			processParentEvent(batchResp -> {
				if (batchResp.succeeded()) {
					log.trace("Periodic transfer task watch starting");
				} else {
					log.error("Failed to execute the periodic transfer watch task. {}", batchResp.cause().getMessage(), batchResp.cause());
				}
			});
		});

	}

	public void processParentEvent(Handler<AsyncResult<Boolean>> handler) {
		log.trace("Got into TransferTaskWatchListener.processParentEvent ");
		try {
			log.trace("Looking up inactive parent transfer tasks...");
			getDbService().getAllParentsCanceledOrCompleted(reply -> {
				if (reply.succeeded()) {
					log.debug("Found {} active transfer tasks", reply.result().size());
					reply.result().stream().forEach(jsonResult -> {
						try {
							log.debug("Scheduling health check on transfer task {}",
									((JsonObject)jsonResult).getString("uuid"));
							_doPublishEvent(TRANSFERTASK_HEALTHCHECK_PARENT, ((JsonObject)jsonResult));
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

	/**
	 * Handles generation of health check events for each active transfer task every 10 seconds.
	 * @return future with boolean result of the batch scheduling operation
	 */
	public void processEvent(Handler<AsyncResult<Boolean>> handler) {
		log.trace("Got into TransferTaskWatchListener.processEvent ");
		try {
			log.trace("Looking up active transfer tasks...");
			getDbService().getActiveRootTaskIds(reply -> {
				if (reply.succeeded()) {
					log.debug("Found {} active transfer tasks", reply.result().size());
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

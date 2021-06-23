package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
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

	protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK;

	public TransferTaskWatchListener() throws IOException, InterruptedException {
		super();
	}
	public TransferTaskWatchListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
	}
	public TransferTaskWatchListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
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

		int healthTimer = config().getInteger(MAX_TIME_FOR_HEALTHCHECK_MILLIS, 600000);
		int healthParentTimer = config().getInteger(MAX_TIME_FOR_HEALTHCHECK_PARENT_MILLIS, 600000);

		getVertx().setPeriodic( healthTimer, resp -> {
			processRootTaskEvent(batchResp -> {
				if (batchResp.succeeded()) {
					log.trace("Periodic transfer task watch starting");
				} else {
					log.error("Failed to execute the periodic transfer watch task. {}", batchResp.cause().getMessage(), batchResp.cause());
				}
			});
		});

		// Disabling because the query cannot work and this seems to be an expensive, redundant operation
//		getVertx().setPeriodic(healthParentTimer, resp -> {
//			processParentEvent(batchResp -> {
//				if (batchResp.succeeded()) {
//					log.trace("Periodic transfer task watch starting");
//				} else {
//					log.error("Failed to execute the periodic transfer watch task. {}", batchResp.cause().getMessage(), batchResp.cause());
//				}
//			});
//		});

	}

//	/**
//	 * Searches the db for all non-root directory transfer tasks and creates a {@link MessageType#TRANSFERTASK_HEALTHCHECK_PARENT}
//	 * event to check and cleanup. The handler is resolved with the result of creating events for all the transfer
//	 * tasks discovered. False if any fail. A failed delivery does not mean no tasks were processed, just that not all
//	 * were created.
//	 * @param handler the result handler
//	 */
//	public void processParentEvent(Handler<AsyncResult<Boolean>> handler) {
//		log.trace("Got into TransferTaskWatchListener.processParentEvent ");
//		try {
//			log.trace("Looking up inactive parent transfer tasks...");
//			getDbService().getAllParentsCanceledOrCompleted(reply -> {
//				if (reply.succeeded()) {
//					log.debug("Found {} active transfer tasks", reply.result().size());
//
//					List<Future> activeTaskFutures = new ArrayList<>();
//					// iterate over the results from the db, adding the publishing of each to a list of Futures that
//					// we can resolve as a compositefuture and retain individual results.
//					reply.result().stream().forEach(jsonTask -> {
//						Promise<Boolean> promise = Promise.promise();
//						_doPublishEvent(TRANSFERTASK_HEALTHCHECK_PARENT, ((JsonObject)jsonTask), publishEventResp -> {
//							if (publishEventResp.failed()) {
//								log.error("Failed to schedule health check for transfer task {}", ((JsonObject)jsonTask).getString("uuid"));
//								promise.fail(publishEventResp.cause());
//							} else {
//								log.debug("Scheduled health check for transfer task {}",
//										((JsonObject)jsonTask).getString("uuid"));
//								promise.complete(publishEventResp.result());
//							}
//						});
//						activeTaskFutures.add(promise.future());
//					});
//
//					// this gets the collective result of all the futures and passes the overall result to the
//					// handler.
//					CompositeFuture.all(activeTaskFutures).onComplete(watchTasks -> {
//						if (watchTasks.succeeded()) {
//							handler.handle(Future.succeededFuture(Boolean.TRUE));
//						} else {
//							handler.handle(Future.failedFuture(watchTasks.cause()));
//						}
//					});
//				} else {
//					log.error("Unable to retrieve list of active transfer tasks: {}", reply.cause().getMessage());
//					handler.handle(Future.failedFuture(reply.cause()));
//				}
//			});
//		} catch (Exception e) {
//			if (e.toString().contains("no null address accepted")){
//				log.info("Error with TransferTaskWatchListener processEvent  error ={} }", e.toString());
//				handler.handle(Future.succeededFuture(Boolean.TRUE));
//			}else
//			{
//				log.error("Error with TransferTaskWatchListener processEvent  error ={} }",  e.toString());
//				handler.handle(Future.failedFuture(e));
//			}
//		}
//	}

	/**
	 * Handles generation of health check events for each active transfer task every 10 seconds.
	 * Result to handler is boolean result of the batch scheduling operation.
	 */
	public void processRootTaskEvent(Handler<AsyncResult<Boolean>> handler) {
		log.trace("Got into TransferTaskWatchListener.processEvent ");
		try {
			log.trace("Looking up active transfer tasks...");
			getDbService().getActiveRootTaskIds(reply -> {
				if (reply.succeeded()) {
					log.debug("Found {} active transfer tasks", reply.result().size());

					List<Future> activeTaskFutures = new ArrayList<>();
					// iterate over the results from the db, adding the publishing of each to a list of Futures that
					// we can resolve as a compositefuture and retain individual results.
					reply.result().stream().forEach(jsonTask -> {
						Promise<Boolean> promise = Promise.promise();
						_doPublishEvent(TRANSFERTASK_HEALTHCHECK, ((JsonObject)jsonTask), publishEventResp -> {
							if (publishEventResp.failed()) {
								log.error("Failed to schedule health check for transfer task {}", ((JsonObject)jsonTask).getString("uuid"));
								promise.fail(publishEventResp.cause());
							} else {
								log.debug("Scheduled health check for transfer task {}",
										((JsonObject)jsonTask).getString("uuid"));
								promise.complete(publishEventResp.result());
							}
						});
						activeTaskFutures.add(promise.future());
					});

					// this gets the collective result of all the futures and passes the overall result to the
					// handler.
					CompositeFuture.all(activeTaskFutures).onComplete(watchTasks -> {
						if (watchTasks.succeeded()) {
							handler.handle(Future.succeededFuture(Boolean.TRUE));
						} else {
							handler.handle(Future.failedFuture(watchTasks.cause()));
						}
					});

				} else {
					log.error("Unable to retrieve list of active transfer tasks: {}", reply.cause().getMessage());
					handler.handle(Future.failedFuture(reply.cause()));
				}
			});
		} catch (Exception e) {
			if (e.toString().contains("no null address accepted")){
				log.info("Error with TransferTaskWatchListener processEvent  error {} }", e.toString());
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

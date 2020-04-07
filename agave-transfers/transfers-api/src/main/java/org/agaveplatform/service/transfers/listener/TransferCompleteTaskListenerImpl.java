package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLOptions;

import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.SqlQuery;
import org.agaveplatform.service.transfers.model.TransferTask;
//import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
//import org.agaveplatform.service.transfers.dao.TransferTaskDao;
//import org.agaveplatform.service.transfers.model.TransferUpdate;
//import org.agaveplatform.service.transfers.util.TransferRateHelper;
//
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.util.ActionHelper.created;
import static org.agaveplatform.service.transfers.util.ActionHelper.ok;

public class TransferCompleteTaskListenerImpl extends AbstractVerticle implements TransferCompleteTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferCompleteTaskListenerImpl.class);
	private String address = "transfer.complete";

	private String eventChannel = "transfertask.db.queue";

	protected List<String>  parrentList = new ArrayList<String>();

	public TransferCompleteTaskListenerImpl(Vertx vertx) {
		this(vertx,null);
	}

	public TransferCompleteTaskListenerImpl(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setAddress(address);
	}
	private JDBCClient jdbc;
	private JsonObject config;
	private TransferTaskDatabaseService dbService;

	@Override
	public void start() {

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE, "transfertask.db.queue"); // <1>
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);


		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getAddress(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("id");
			String status = body.getString("status");
			logger.info("{} transfer task {} completed with status {}", uuid, status);

			this.processEvent(body);
		});
	}

	public Future<JsonObject> processEvent(JsonObject body) {
		Future<JsonObject> future = Future.future();
		// udpate transfer task status to completed

		//TransferTask bodyTask = new TransferTask(body);
		body.put("status", TransferStatusType.COMPLETED);
		String tenantId = body.getString("tenantId");
		String uuid = body.getString("uuid");
		String status = body.getString("status");
		String parentTaskId = body.getString("parentTaskId");

		try {
//			dbService.updateStatus(tenantId, uuid, status, reply -> this.handleUpdateStatus(reply, tenantId, parentTaskId));
			dbService.updateStatus(tenantId, uuid, status, reply -> {
				if (reply.succeeded()) {

					_doProcessEvent("transfertask.completed", body);

					processParentEvent(tenantId, parentTaskId).setHandler(tt -> {
						// TODO: send notification events? or should listeners listen to the existing events?
						//getVertx().eventBus().publish("transfertask.notification", body.toString());
						_doProcessEvent("transfertask.notification", body);
						future.complete(reply.result());
					} );
				}
				else {
					logger.error("Failed to set status of transfertask {} to completed. error: {}", uuid, reply.cause());
				}
			});
		} catch (Exception e) {
			logger.error(e.toString());
		} finally {
			System.out.println("got to  finally");
			return future;
		}
	}

	protected void _doProcessEvent(String event, Object body) {
		getVertx().eventBus().publish(event, body);
	}

	protected Future<JsonObject> processParentEvent(String tenantId, String parentTaskId) {
		Future<JsonObject> future = Future.future();
		dbService.getById(tenantId, parentTaskId, reply -> {
			if (reply.succeeded()) {
				TransferTask parentTask = new TransferTask(reply.result());
				if (! parentTask.getStatus().toString().isEmpty() ||
						parentTask.getStatus() != TransferStatusType.CANCELLED ||
						parentTask.getStatus() != TransferStatusType.COMPLETED ||
						parentTask.getStatus() != TransferStatusType.FAILED) {

					dbService.allChildrenCancelledOrCompleted(tenantId, parentTaskId, reply2 -> {
						if (reply2.succeeded()) {
							if (reply2.result()) {
								_doProcessEvent("transfertask.completed", reply.result());
								//getVertx().eventBus().publish("transfertask.completed", reply.result());
								future.complete(reply.result());
							} else {
								// parent has active children. let it run
							}
						} else {
							future.fail(reply2.cause());
						}
					});
				} else {
					// parent is already terminal. let it lay
				}
			} else {
				future.fail(reply.cause());
				logger.error("Failed to set status of transfertask {} to completed. error: {}", parentTaskId, reply.cause());
			}
		});

		return future;
	}

	/**
	 * Sets the vertx instance for this listener
	 * @param vertx the current instance of vertx
	 */
	private void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * Sets the message type for which to listen
	 * @param address
	 */
	public void setAddress(String address) {
		this.address = address;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getEventChannel() {
		return eventChannel;
	}
}

package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import org.agaveplatform.service.transfers.model.SqlQuery;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.agaveplatform.service.transfers.model.TransferTask;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.UpdateResult;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class TransferCompleteTaskListenerImpl extends AbstractVerticle implements TransferCompleteTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferCompleteTaskListenerImpl.class);
	private String address = "*.transfer.complete";

	protected List<String>  parrentList = new ArrayList<String>();

	public TransferCompleteTaskListenerImpl(Vertx vertx) {
		this(vertx,null);
	}

	public TransferCompleteTaskListenerImpl(Vertx vertx, String address) {
		super();
		setVertx(vertx);
		setAddress(address);
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getAddress(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("id");
			String status = body.getString("status");
			String source = body.getString("source");
			String username = body.getString("owner");
			String tenantId = body.getString("tenantId");
			logger.info("{} transfer task {} completed with status {}", uuid, status);


			// udpate transfer task status to completed
			TransferTask bodyTask = new TransferTask(body);
			body.put("status", TransferStatusType.COMPLETED);

			org.iplantc.service.transfer.model.TransferTask tt = new org.iplantc.service.transfer.model.TransferTask();
			tt.setId(bodyTask.getId());
			tt.setStatus(TransferStatusType.COMPLETED);
			try {
				TransferTaskDao.updateProgress(tt);
			}catch (TransferException e){
				logger.error(e.toString());
			} catch (Exception e) {
				logger.error(e.toString());
			}


			// TODO: check parent for outstanding tasks, update to completed if completed
			String parentId = body.getString("parentId");
			TransferTask parentTask = new TransferTask();

			parentTask = queryParent("", parentId);
			List<parentTask> = new ArrayList<parentTask>();


			if bodyTask.getStatus()
			bodyTask.getStatus()!= TransferStatusType.CANCELLED;


			// TODO: send notification events? or should listeners listen to the existing events?


		});
	}

	private Future<TransferTask> queryParent(SQLConnection connection, String uuid) {
		Future<TransferTask> future = Future.future();

		connection.queryWithParams(SqlQuery.GET_PARENT, new JsonArray().add(uuid), result -> {
			connection.close();
			future.handle(
					result.map(rs -> {
						List<JsonObject> rows = rs.getRows();
						if (rows.size() == 0) {
							throw new NoSuchElementException("No transferTask with id " + uuid);
						} else {
							JsonObject row = rows.get(0);
							return new TransferTask(row);
						}
					})
			);
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

}

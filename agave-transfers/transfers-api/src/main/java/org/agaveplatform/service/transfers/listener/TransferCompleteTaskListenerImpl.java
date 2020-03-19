package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.RoutingContext;
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
import java.util.stream.Collectors;

import static org.agaveplatform.service.transfers.util.ActionHelper.created;
import static org.agaveplatform.service.transfers.util.ActionHelper.ok;

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
	private JDBCClient jdbc;
	private JsonObject config;

	@Override
	public void start() {

		// init our db connection from the pool
		jdbc = JDBCClient.createShared(vertx, config, "agave_io");
		connect()
				.compose(connection -> {
					Future<Void> future = Future.future();
					createTableIfNeeded(connection)
							.compose(this::createSomeDataIfNone)
							.setHandler(x -> {
								connection.close();
								future.handle(x.mapEmpty());
							});
					return future;
				});


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


			// TODO: check parent for outstanding tasks, update to completed if not completed
			String parentId = body.getString("parentId");
			TransferTask parentTask = new TransferTask();
			SQLConnection connection = (SQLConnection) connect();

			parentTask = (TransferTask) queryParent(connection, parentId);
			org.iplantc.service.transfer.model.TransferTask transferTaskParent = new org.iplantc.service.transfer.model.TransferTask();

			if (! parentTask.getStatus().toString().isEmpty() ||
				parentTask.getStatus() != TransferStatusType.CANCELLED ||
				parentTask.getStatus() != TransferStatusType.COMPLETED ||
				parentTask.getStatus() != TransferStatusType.FAILED) {

				transferTaskParent.setId(bodyTask.getId());
				transferTaskParent.setStatus(TransferStatusType.COMPLETED);
				try {
					TransferTaskDao.updateProgress(transferTaskParent);
				}catch (TransferException e){
					logger.error(e.toString());
				} catch (Exception e) {
					logger.error(e.toString());
				}
			}

			// TODO: send notification events? or should listeners listen to the existing events?
			getVertx().eventBus().publish("transfertask.nofication", body.toString());

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

	private Future<SQLConnection> connect() {
		Future<SQLConnection> future = Future.future();
		jdbc.getConnection(ar ->
				future.handle(ar.map(c ->
								c.setOptions(new SQLOptions().setAutoGeneratedKeys(true))
						)
				)
		);
		return future;
	}

	/**
	 * Runs initial migration on the db
	 * @param connection the active db connection
	 * @return empty future for the sqlconnection
	 */
	private Future<SQLConnection> createTableIfNeeded(SQLConnection connection) {
		Future<SQLConnection> future = Future.future();
		vertx.fileSystem().readFile("tables.sql", ar -> {
			if (ar.failed()) {
				future.fail(ar.cause());
			} else {
				connection.execute(ar.result().toString(),
						ar2 -> future.handle(ar2.map(connection))
				);
			}
		});
		return future;
	}

	/**
	 * Populates the db with some sample data
	 * @param connection the active db connection
	 * @return empty future for the sqlconnection
	 */
	private Future<SQLConnection> createSomeDataIfNone(SQLConnection connection) {
		Future<SQLConnection> future = Future.future();
		connection.query("SELECT * FROM transfertasks", select -> {
			if (select.failed()) {
				future.fail(select.cause());
			} else {
				if (select.result().getResults().isEmpty()) {
					TransferTask transferTask1= new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts1", "testuser", null, null);
					TransferTask transferTask2 = new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts2", "testuser", null, null);
					TransferTask transferTask3 = new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts3", "testuser", null, null);
					Future<TransferTask> insertion1 = insert(connection, transferTask1, false);
					Future<TransferTask> insertion2 = insert(connection, transferTask2, false);
					Future<TransferTask> insertion3 = insert(connection, transferTask3, false);
					CompositeFuture.all(insertion1, insertion2)
							.setHandler(r -> future.handle(r.map(connection)));
				} else {
					future.complete(connection);
				}
			}
		});
		return future;
	}

	private Future<TransferTask> insert(SQLConnection connection, TransferTask transferTask, boolean closeConnection) {
		Future<TransferTask> future = Future.future();
		String sql = "INSERT INTO TransferTasks " +
				"(\"attempts\", \"bytes_transferred\", \"created\", \"dest\", \"end_time\", \"event_id\", \"last_updated\", \"owner\", \"source\", \"start_time\", \"status\", \"tenant_id\", \"total_size\", \"transfer_rate\", \"parent_task\", \"root_task\", \"uuid\", \"total_files\", \"total_skipped\") " +
				"VALUES " +
				"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		connection.updateWithParams(sql,
				new JsonArray()
						.add(transferTask.getAttempts())
						.add(transferTask.getBytesTransferred())
						.add(transferTask.getCreated())
						.add(transferTask.getDest())
						.add(transferTask.getEndTime())
						.add(transferTask.getEventId())
						.add(transferTask.getLastUpdated())
						.add(transferTask.getOwner())
						.add(transferTask.getSource())
						.add(transferTask.getStartTime())
						.add(transferTask.getStatus())
						.add(transferTask.getTenantId())
						.add(transferTask.getTotalSize())
						.add(transferTask.getTransferRate())
						.add(transferTask.getParentTaskId())
						.add(transferTask.getRootTaskId())
						.add(transferTask.getUuid())
						.add(transferTask.getTotalFiles())
						.add(transferTask.getTotalSkippedFiles()),
				ar -> {
					if (closeConnection) {
						connection.close();
					}
					future.handle(
							ar.map(res -> {
								TransferTask t = new TransferTask(transferTask.getSource(), transferTask.getDest());
								t.setId(res.getKeys().getLong(0));
								return t;
							})
					);
				}
		);
		return future;
	}
	/**
	 * Fetches all {@link TransferTask} from the db. Results are added to the routing context.
	 * TODO: add pagination and querying.
	 *
	 * @param routingContext the current rounting context for the request
	 */
	private void getAll(RoutingContext routingContext) {
		connect()
				.compose(this::query)
				.setHandler(ok(routingContext));
	}
	private Future<List<TransferTask>> query(SQLConnection connection) {
		Future<List<TransferTask>> future = Future.future();
		connection.query("SELECT * FROM transfertasks", result -> {
					connection.close();
					future.handle(
							result.map(rs -> rs.getRows().stream().map(TransferTask::new).collect(Collectors.toList()))
					);
				}
		);
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

package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
		import io.vertx.core.Vertx;
		import io.vertx.core.eventbus.EventBus;
		import io.vertx.core.json.JsonObject;
		import org.agaveplatform.service.transfers.model.TransferTask;
		import org.apache.commons.lang3.StringUtils;

		import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
		import org.slf4j.Logger;
		import org.slf4j.LoggerFactory;

		import java.util.ArrayList;
		import java.util.List;

public class TransferTaskPausedListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(TransferTaskCancelListener.class);
	private String eventChannel = "transfertask.paused";

	public TransferTaskPausedListener(Vertx vertx) {
		this(vertx, null);
	}

	public TransferTaskPausedListener(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("transfertask.paused", msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("id");
			logger.info("Transfer task {} cancel detected.", uuid);

			this.processPauseRequest(body);
		});

		bus.<JsonObject>consumer("transfertask.paused.ack", msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("id");
			String parentTaskId = body.getString("parentTaskId");
			String rootTaskId = body.getString("rootTaskId");

			logger.info("Transfer task {} ackowledged paused", uuid);

			// if this task has children and all are cancelled or completed, then we can
			// mark this task as cancelled.
			if (allChildrenCancelledOrCompleted(uuid)) {
				// update the status of the transfertask since the verticle handling it has
				// reported back that it completed the cancel request since this tree is
				// done cancelling
				setTransferTaskCancelledIfNotCompleted(uuid);

				// this task and all its children are done, so we can send a complete event
				// to safely clear out the uuid from all listener verticals' caches
				getVertx().eventBus().publish("transfertask.paused.complete", body);

				// we can now also check the parent, if present, for completion of its tree.
				// if the parent is empty, the root will be as well. For children of the root
				// transfer task, the root == parent
				if (!StringUtils.isEmpty(parentTaskId)) {
					// once all tasks in the parents tree are complete, we can update the status of the parent
					// and send out the completed event to clear its uuid out of any caches that may have it.
					if (allChildrenCancelledOrCompleted(parentTaskId)) {
						setTransferTaskCancelledIfNotCompleted(parentTaskId);
						TransferTask parentTask = getTransferTask(parentTaskId);
						getVertx().eventBus().publish("transfertask.paused.ack", parentTask.toJSON());
					}
				}
			}
		});
	}

	private boolean allChildrenCancelledOrCompleted(String parentTaskId) {
		//TODO: "select count(id) from transfertasks where (parentTask = {}) and status not in (('COMPLETED', 'CANCELLED','FAILED')"
		return false;
	}

	private void setTransferTaskCancelledIfNotCompleted(String uuid) {

		//TODO: "update transfertasks set status = CANCELLED, lastUpdated = now() where uuid = {} and status not in ('COMPLETED', 'CANCELLED','FAILED')"
	}

	protected void processPauseRequest(JsonObject body) {
		String uuid = body.getString("uuid");

		try {
			// lookup the transfer task from the db
			TransferTask tt = getTransferTask(uuid);
			// if it's not already in a terminal state, process the cancell requrest
			if (tt.getStatus() != TransferStatusType.CANCELLED ||
					tt.getStatus() != TransferStatusType.COMPLETED ||
					tt.getStatus() != TransferStatusType.FAILED) {

				// push the event transfer task onto the queue. this will cause all listening verticals
				// actively processing any of its children to cancel their existing work and ack
				getVertx().eventBus().publish("transfertask.paused.sync", body);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			getVertx().eventBus().publish("transfertask.error", json);
		}
	}

	protected List<TransferTask> getTransferTaskTree(String rootUuid) {
		// TODO: make db or api call to service
		return new ArrayList<TransferTask>();
	}

	protected TransferTask getTransferTask(String uuid) {
		// TODO: make db or api call to service
		return new TransferTask();
	}

	/**
	 * Sets the vertx instance for this listener
	 *
	 * @param vertx the current instance of vertx
	 */
	private void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getEventChannel() {
		return eventChannel;
	}

	/**
	 * Sets the message type for which to listen
	 *
	 * @param eventChannel
	 */
	public void setEventChannel(String eventChannel) {
		this.eventChannel = eventChannel;
	}
}

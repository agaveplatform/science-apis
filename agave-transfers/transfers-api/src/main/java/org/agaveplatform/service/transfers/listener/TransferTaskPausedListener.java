package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
		import io.vertx.core.Vertx;
		import io.vertx.core.eventbus.EventBus;
		import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
		import org.apache.commons.lang3.StringUtils;

		import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
		import org.slf4j.Logger;
		import org.slf4j.LoggerFactory;

		import java.util.ArrayList;
		import java.util.List;

public class TransferTaskPausedListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferTaskPausedListener.class);

	public TransferTaskPausedListener(Vertx vertx) {
		this(vertx, null);
	}

	public TransferTaskPausedListener(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_PAUSED;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			logger.info("Transfer task {} cancel detected.", uuid);

			this.processPauseRequest(body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_ACK, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
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
				_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_COMPLETED, body);

				// we can now also check the parent, if present, for completion of its tree.
				// if the parent is empty, the root will be as well. For children of the root
				// transfer task, the root == parent
				if (!StringUtils.isEmpty(parentTaskId)) {
					// once all tasks in the parents tree are complete, we can update the status of the parent
					// and send out the completed event to clear its uuid out of any caches that may have it.
					if (allChildrenCancelledOrCompleted(parentTaskId)) {
						setTransferTaskCancelledIfNotCompleted(parentTaskId);
						TransferTask parentTask = getTransferTask(parentTaskId);
						_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_ACK, parentTask.toJSON());
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
				_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_SYNC, body);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
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
}



























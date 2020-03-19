package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Transient;

public class NotificationListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer("transfertask.notification.*", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} notification: {}", body.getString("id"), body.getString("message"));
			TransferTask bodyTask = new TransferTask(body);
			notification(bodyTask);
		});

		bus.<JsonObject>consumer("transfertask.cancel.sync", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} notification: {}", body.getString("id"), body.getString("message"));
			TransferTask bodyTask = new TransferTask(body);
			String owner = bodyTask.getOwner();
			String event = "Notification";
			String uuid = bodyTask.getUuid();
			boolean persistent = true;
			String callback = "";
			notification(event, callback, persistent, owner, uuid);
		});



	}

	public void notification( String event, String callback, boolean persistent, String owner, String uuid){
		//TODO:  Write the nofication call here
		try {
			addNotification(event, callback, persistent, owner, uuid);
		}
		catch (Exception e){
			logger.error(e.toString());
		}
	}



	/**
	 * Convenience endpoint to add a notification.
	 *
	 * @param event String notification event that will trigger the callback
	 * @param callback A URL or email address that will be triggered by the event
	 * @param persistent Whether this notification should expire after the first successful trigger
	 */
	@Transient
	public void addNotification(String event, String callback, boolean persistent, String owner, String uuid)
			throws NotificationException
	{
		Notification notification = new Notification(event, callback);
		notification.setOwner(owner);
		notification.setAssociatedUuid(uuid);
		notification.setPersistent(persistent);

		addNotification(notification, owner, uuid);
	}
	/**
	 * Convenience endpoint to add a notification.
	 *
	 * @param notification A notification event to associate with this job. The current
	 * jobs owner and uuid will be added to the notification.
	 */
	@Transient
	public void addNotification(Notification notification, String owner, String uuid) throws NotificationException
	{
		notification.setOwner(owner);
		notification.setAssociatedUuid(uuid);
		new NotificationDao().persist(notification);
	}
}

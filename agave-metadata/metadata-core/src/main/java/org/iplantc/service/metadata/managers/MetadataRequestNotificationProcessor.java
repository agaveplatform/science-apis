package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dooley
 *
 */
public class MetadataRequestNotificationProcessor {

	private List<Notification> notifications;
	private String owner;
	private String uuid;
	private NotificationDao dao = new NotificationDao();


	public MetadataRequestNotificationProcessor(String owner, String uuid) {
		this.setNotifications(new ArrayList<Notification>());
		this.setOwner(owner);
		this.setUuid(uuid);
	}
	
	/**
	 * Processes a {@link JsonNode} passed in with a {@link MetadataItem} request as a notification configuration.
	 * Accepts an array of {@link Notification} request objects or a simple string.
	 *  
	 * @param jsonNotificationArray the array of json {@link Notification} objects to register to the new metadata item.
	 * @throws NotificationException if any of the notification objects are invalid
	 */
	public void process(ArrayNode jsonNotificationArray) throws NotificationException {
		
		getNotifications().clear();
		
		if (jsonNotificationArray != null && !jsonNotificationArray.isNull()) {
			for (int i=0; i<jsonNotificationArray.size(); i++)
			{
				JsonNode jsonNotif = jsonNotificationArray.get(i);
				if (!jsonNotif.isObject())
				{
					throw new NotificationException("Invalid notifications["+i+"] value given. "
						+ "Each notification objects should specify a "
						+ "valid url, event, and an optional boolean persistence attribute.");
				}
				else
				{
					// Here we reuse the validation built into the {@link Notification} model
					// itself to validate the embedded metadata schema notification subscriptions.
					Notification notification = new Notification();
					try {
						((ObjectNode)jsonNotif).put("associatedUuid", getUuid());

						notification = Notification.fromJSON(jsonNotif);
						notification.setOwner(getOwner());
					} 
					catch (NotificationException e) {
						throw e;
					} 
					catch (Throwable e) {
						throw new NotificationException("Unable to process notification.", e);
					}
					
					getDao().persist(notification);
					
					getNotifications().add(notification);
				}
			}
		}
		
	}

	/**
	 * @return the notifications
	 */
	public List<Notification> getNotifications() {
		return notifications;
	}

	/**
	 * @param notifications the notifications to set
	 */
	public void setNotifications(List<Notification> notifications) {
		this.notifications = notifications;
	}

	/**
	 * @return the owner
	 */
	public String getOwner() {
		return owner;
	}

	/**
	 * @param owner the owner to set
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @return the uuid
	 */
	public String getUuid() {
		return uuid;
	}

	/**
	 * @param uuid the uuid to set
	 */
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public NotificationDao getDao() {
		return dao;
	}

	public void setDao(NotificationDao dao) {
		this.dao = dao;
	}
}

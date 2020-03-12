package org.iplantc.service.notification.managers;

import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.uuid.UUIDEntityLookup;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.notification.model.enumerations.NotificationStatusType;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;

import java.net.URI;
import java.net.URL;
import java.util.UUID;

/**
 * Helper class to queue up all the registered notifications for a given
 * resource event.
 *
 * @author dooley
 *
 */
public class NotificationManager {
	private static final Logger log = Logger
			.getLogger(NotificationManager.class);

	/**
	 * Sends off all the notifications for a particular event and UUID for
	 * processing.
	 *
	 * @param associatedUuid
	 * @param notificationEvent
	 * @param affectedUser
	 * @return
	 */
	public static int process(String associatedUuid, String notificationEvent,
			String affectedUser) {
		return process(associatedUuid, notificationEvent, affectedUser, null);
	}

	/**
	 * Sends off all the notifications for a particular event and UUID for
	 * processing.
	 *
	 * @param associatedUuid
	 * @param notificationEvent
	 * @param affectedUser
	 * @param customData serialized custom data to be included in the message
	 * @return number of messages published for the given event
	 */
	public static int process(String associatedUuid, final String eventType, String affectedUser, String customData)
	{
		MessageQueueClient queue = null;
		int totalProcessed = 0;
		try {
			queue = MessageClientFactory.getMessageClient();
			NotificationDao dao = new NotificationDao();
			
			// find all messages that match the given event and uuid and throw
			// them into queue
			for (Notification n : dao.getActiveForAssociatedUuidAndEvent(associatedUuid, eventType)) {
				try {
					NotificationMessageContext payload = new NotificationMessageContext(
							eventType, customData, associatedUuid);

					// given
					final String eventId = UUID.randomUUID().toString();
					final URI src = URI.create("/trigger");

					UUIDType uuidType = UUIDType.getInstance(associatedUuid);
					String source = UUIDEntityLookup.getResourceUrl(uuidType, associatedUuid);
					URI sourceUrl = new URI(source);

					// passing in the given attributes
					final CloudEventImpl<NotificationMessageContext> cloudEvent =
							CloudEventBuilder.<NotificationMessageContext>builder()
									.withType(eventType)
									.withId(eventId)
									.withSource(sourceUrl)
									.withSubject(affectedUser)
									.withDataContentType("application/json")
									.withData(payload)
									.build();

					// marshalling as json
					final String json = Json.encode(cloudEvent);

					queue.push(Settings.NOTIFICATION_TOPIC,
							Settings.NOTIFICATION_QUEUE, json);
					totalProcessed++;
				} catch (Exception e) {
					log.error("Failed to queue up notification " + n.getUuid());
				}
			}			
		} catch (MessagingException e) {
			log.error(
					"Failed to connect to the messaging queue. No notifications will be sent for "
							+ associatedUuid + " on event " + eventType,
					e);
		} catch (NotificationException e) {
			log.error("Failed to process notifications for " + associatedUuid
					+ " on event " + eventType, e);
		} catch (Throwable e) {
			log.error(
					"Unknown messaging exception occurred. Failed to process notifications for "
							+ associatedUuid + " on event " + eventType,
					e);
		} finally {
			if (queue != null) {
				queue.stop();
			}
		}
		
		return totalProcessed;
	}

	/**
	 * Threadsafe update of a {@link Notification} object to set the new status.
	 *  
	 * @param notificationId
	 * @param status
	 * @throws NotificationException
	 */
	public static void updateStatus(String notificationId, NotificationStatusType status) 
	throws NotificationException
	{
		try {
			new NotificationDao().updateStatus(notificationId, status);
		}
		catch (Exception e) {
			throw new NotificationException("Failed to update status of notification " + 
					notificationId + " to " + status.name(), e);
		}
	}

//	/**
//	 * Returns true if the notification is either persistent or has not
//	 * succeeded within the maximum number of attempts.
//	 *
//	 * @param notification
//	 * @return true if the notification is active or has not filed enough yet
//	 * @throws NotificationException
//	 */
//	public static boolean isActive(Notification n) throws NotificationException {
//		if (n.isPersistent()
//				|| n.getAttempts() < Settings.MAX_NOTIFICATION_RETRIES
//				& !n.isSuccess()) {
//			return true;
//		} else {
//			return false;
//		}
//	}
}

/**
 * 
 */
package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobEventType;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;

import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.jobs.model.enumerations.JobEventType.*;

/**
 * @author dooley
 *
 */
public class JobRequestNotificationProcessor {

	private List<Notification> notifications;
	private String username;
	private Job job;
	
	public JobRequestNotificationProcessor(String jobRequestOwner, Job job) {
		this.setNotifications(new ArrayList<Notification>());
		this.setUsername(jobRequestOwner);
		this.job = job;
	}
	
	/**
	 * Processes a {@link String} passed in with a job request as a 
	 * notification configuration. Acceptable values are email addresses,
	 * URL, phone numbers, and agave uri. Anything else will throw 
	 * an exception.
	 *  
	 * @param callbackUrl the url callback when just strings are given as the notification property in url form submissions
	 * @throws NotificationException if an invalid callback URL value is given
	 */
	public void process(String callbackUrl) throws NotificationException {
		ObjectMapper mapper = new ObjectMapper();

		if (StringUtils.isNotBlank(callbackUrl)) {
			ArrayNode jsonNotifications = mapper.createArrayNode();
			for (JobEventType eventType : List.of(FINISHED, FAILED, STOPPED)) {
				jsonNotifications.addObject()
						.put("event", eventType.name())
						.put("owner", getUsername())
						.put("associatedUuid", getJob().getUuid())
						.put("url", callbackUrl)
						.put("persistent", false);
			}
			process(jsonNotifications);
		}
	}

	/**
	 * Processes a {@link JsonNode} passed in with a job request as a 
	 * notification configuration. Accepts an array of {@link Notification} 
	 * request objects or a simple string;
	 *  
	 * @param json a JSON representation of the notification to be added to the job
	 * @throws NotificationException if the notification cannot be validated
	 */
	public void process(JsonNode json) throws NotificationException {

		getNotifications().clear();

		if (json == null || json.isNull()) return;

		if (json.isValueNode()) {
			process(json.textValue());
		}
		else if (json.isArray()) {
			process((ArrayNode)json);
		} else {
			throw new NotificationException("Invalid notification value given. "
					+ "notifications must be an array of notification objects specifying a "
					+ "valid url, event, and an optional boolean persistence attribute.");
		}
	}

	/**
	 * Processes a {@link ArrayNode} of notification subscriptions represented as {@link ObjectNode}.
	 *
	 * @param jsonNotifications a JSON array of json notification objects
	 * @throws NotificationException if the notification cannot be validated
	 */
	public void process(ArrayNode jsonNotifications) throws NotificationException {

		getNotifications().clear();

		if (jsonNotifications == null) return;

		for (int i=0; i<jsonNotifications.size(); i++)
		{
			JsonNode jsonNotif = jsonNotifications.get(i);
			if (!jsonNotif.isObject())
			{
				throw new NotificationException("Invalid notifications["+i+"] value given. "
					+ "Each notification object should specify a "
					+ "valid url, event, and an optional boolean persistence attribute.");
			}
			else
			{
				// here we reuse the validation built into the {@link Notification} model
				// itself to validate the embedded job notification subscriptions.
				try {
					addNotification(((ObjectNode)jsonNotif).put("associatedUuid", getJob().getUuid()));
				}
				catch (NotificationException e) {
					throw e;
				}
				catch (Throwable e) {
					throw new NotificationException("Unable to process notification ["+i+"]" , e);
				}
			}
		}
	}

	/**
	 * Parses JsonObject into a {@link Notification} and adds the object to the queue. This is a light wrapper for the
	 * call to {@link Notification#fromJSON(JsonNode)} that allows us to mock out an otherwise static method
	 * call that would throw exceptions without a live database to query the job id. We already test the
	 * {@link Notification#fromJSON(JsonNode)} method in its module, so we can skip that here.
	 *
	 * @param jsonNotification notification subscription represented as an {@link ObjectNode}
	 */
	protected void addNotification(ObjectNode jsonNotification) throws NotificationException {
		Notification notification = Notification.fromJSON(jsonNotification);
		notification.setOwner(getUsername());
		getNotifications().add(notification);
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
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
}

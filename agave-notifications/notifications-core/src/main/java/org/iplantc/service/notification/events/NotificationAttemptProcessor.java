/**
 * 
 */
package org.iplantc.service.notification.events;

import java.time.Instant;
import java.util.Date;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.dao.FailedNotificationAttemptQueue;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.DisabledNotificationException;
import org.iplantc.service.notification.exceptions.MissingNotificationException;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.exceptions.NotificationPolicyViolationException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.notification.model.NotificationAttempt;
import org.iplantc.service.notification.model.NotificationAttemptBackoffCalculator;
import org.iplantc.service.notification.model.NotificationAttemptResponse;
import org.iplantc.service.notification.model.NotificationPolicy;
import org.iplantc.service.notification.model.enumerations.NotificationStatusType;
import org.iplantc.service.notification.providers.NotificationAttemptProvider;
import org.iplantc.service.notification.providers.NotificationAttemptProviderFactory;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author dooley
 *
 */
public class NotificationAttemptProcessor {
	
	private static final Logger	log	= Logger.getLogger(NotificationAttemptProcessor.class);

	private NotificationAttempt attempt;
	private Notification notification;
	
	
	public NotificationAttemptProcessor(NotificationAttempt attempt) 
	{
		this.setAttempt(attempt);
	}
	
	
	/**
	 * Verifies the  parent {@link Notification} is still valid and has not 
	 * been cancelled.
	 * @return true if the notification is valid.
	 * @throws DisabledNotificationException if the notification has been cancelled
	 * @throws MissingNotificationException if the notification cannot be found anymore
	 */
	public boolean isNotificationStillActive() 
	throws DisabledNotificationException, MissingNotificationException 
	{
		try {
			this.notification = new NotificationDao().findByUuidAcrossTenants(attempt.getNotificationId());
			
			if (this.notification == null) {
				throw new MissingNotificationException("Unable to locate parent notification.");
			}
			else if (this.notification.getStatus() != NotificationStatusType.ACTIVE) {
				throw new DisabledNotificationException("Notification " + attempt.getNotificationId() + 
						" has been disabled by upstream process. No further attempts will be made.");
			}
			else {
				return true;
			}
		} catch (DisabledNotificationException e) {
			throw e;
		} catch (Exception e){
			throw new DisabledNotificationException("Unable to locate parent notification.", e);
		}
	}

	/**
	 * Handles the processing and cleanup of a {@link NotificationAttempt}. A 
	 * {@link NotificationAttempt} will either be placed back into the retry message
	 * queue, or failed to the notification failure queue or abandonded if 
	 * {@link NotificationPolicy#isSaveOnFailure()} is false for the parent 
	 * {@link Notification}.
	 * 
	 * @return true if the attempt was successfully delivered, false otherwise.
	 */
	public boolean fire() 
	{
		try {
			
			if (!isNotificationStillActive()) {
				throw new DisabledNotificationException("Notification is not longer valid");
			}
			
			// process the attempt and save the timestamps
			getAttempt().setStartTime(Instant.now());
			NotificationAttemptResponse response = getNotificationAttemptProvider().publish();
			getAttempt().setEndTime(Instant.now());
			
			// save the response for the decision making step
			getAttempt().setResponse(response);
			
			if (getAttempt().isSuccess()) {
				handleSuccess();
				return true;
			} 
			// if there was a non-critical failure, retry immediately
			// if this is the first attempt at satisfying the attempt
			else if (getAttempt().getAttemptNumber() == 0) {
				getAttempt().setAttemptNumber(1);
				return fire();
			} 
			// if this is not the first attempt and the delivery failed,
			// then handle the failure according to the notification policy.
			else {
				handleFailure(getAttempt());
				return false;
			}
		}
		catch (DisabledNotificationException e) {
			handleCancelledNotification(null, getAttempt());
			return false;
		}
		catch (NotImplementedException e) {
			// can't process the notification due to unsupported provider fail immediately
			handlePolicyViolation(null, getAttempt());
			return false;
		}
		catch (Throwable e) {
			// failed to publish due to a system exception of some sort. 
			// we don't want to retry right away because this is something 
			// more than just a blip.
			handleFailure(getAttempt());
			return false;
		}
	}
		
	/**
	 * Returns a {@link NotificationAttemptProvider} capable of processing the
	 * {@link #attempt} being processed.
	 * @return
	 * @throws NotImplementedException
	 * @throws NotificationException
	 */
	protected NotificationAttemptProvider getNotificationAttemptProvider() 
	throws NotImplementedException, NotificationException 
	{
		// get a client to process the attempt
		return NotificationAttemptProviderFactory.getInstance(getAttempt());
	}


	/**
	 * Handles cleanup after a successful {@link NotificationAttempt}.
	 * 
	 * @throws NotificationPolicyViolationException
	 * @throws NotificationException
	 */
	protected void handleSuccess() 
	{	
		try {
			// remove the scheduled time.
			getAttempt().setScheduledTime(null);
			
			// increment the attempt count
			getAttempt().setAttemptNumber(getAttempt().getAttemptNumber()+1);
			
			// we need to lookup the parent notification and retire it if not persistent
			if (!getNotification().isPersistent()) {
					
				// if it's active, then set it to completed
				if (NotificationStatusType.ACTIVE == getNotification().getStatus()) {
					getNotification().setStatus(NotificationStatusType.COMPLETE);
					NotificationManager.updateStatus(getAttempt().getNotificationId(), 
							NotificationStatusType.COMPLETE);
				}
				// if not, then we're just going to retire the attempt. the notification
				// was updated during the notificatino processing, so we'll ignore it
				else {
					// carry on
				}
				
				// now delete the attempt since we're done with it
				//naDAO.delete(attempt);
			}
		} 
		catch (Exception e) {
			log.error("Failed to update parent notification upon successful delivery of attempt " + attempt.getUuid(), e);
		}
	}
	
	/**
	 * Handles cleanup after a {@link NotificationAttempt} fails to 
	 * deliver its message. This will check whether the
	 * 
	 * @throws NotificationPolicyViolationException
	 * @throws NotificationException
	 */
	protected void handleFailure(NotificationAttempt attempt) 
	{
		try {
			// calculate the next runttime
			NotificationAttemptBackoffCalculator calculator = new NotificationAttemptBackoffCalculator(getNotification().getPolicy(), attempt);
			
			// figure out the next time it should run based on the retry policy
			int secondsUntilNextScheduledAttempt = calculator.getNextScheduledTime();
			
			// increment the number of attempts
			attempt.setAttemptNumber(attempt.getAttemptNumber() + 1);
			
			// set the next scheduled attempt time.
			Instant nextScheduledAttempt = Instant.now().plusSeconds(secondsUntilNextScheduledAttempt);
			
			attempt.setScheduledTime(nextScheduledAttempt);
			setAttempt(attempt);
			
			pushNotificationAttemptToRetryQueue(getAttempt(), secondsUntilNextScheduledAttempt);
		} 
		catch (NotificationPolicyViolationException e) {
			// attempt violated policy. don't reschedule, handle directly
			handlePolicyViolation(getNotification().getPolicy(), getAttempt());
		}
		catch (Exception e) {
			log.debug("Failed to cleanup notification " + attempt.getNotificationId() + 
					" after attempt " + attempt.getUuid() + 
					". Attempt will be preserved in the failed notification queue.");
			
			// err on the side of caution and write the attempt to the failed notificaiton queue
			saveFailedAttempt(attempt);
		}
	}
	
	/**
	 * Handles cleanup after a {@link NotificationAttempt} has failed enough
	 * times to violate the {@link NotificationPolicy} of the parent {@link Notification}
	 * 
	 * @param policy
	 * @param attempt
	 */
	protected void handlePolicyViolation(NotificationPolicy policy, NotificationAttempt attempt) {
		
		log.debug("Notification attempts for " + attempt.getNotificationId() + " have exceeded the policy limits.");
		// attempts have exceeded policy in some way
		if (policy == null || policy.isSaveOnFailure()) {
			saveFailedAttempt(attempt);
		}
		else {
			log.debug("Skipping persistence of failed attempt " + attempt.getUuid() + 
					" due to notification " + attempt.getNotificationId() + " policy.");
		}

		// if it's active, then set it to completed
		if (NotificationStatusType.ACTIVE == getNotification().getStatus()) {
			try {
				getNotification().setStatus(NotificationStatusType.FAILED);
				NotificationManager.updateStatus(getAttempt().getNotificationId(),
						NotificationStatusType.FAILED);
			} catch (NotificationException e) {
				log.error("Failed to update status of notification " + attempt.getNotificationId() +
						" after failure of attempt " + attempt.getUuid());
			}
		}
	}
	
	/**
	 * Handles cleanup after a {@link NotificationAttempt} has failed enough
	 * times to violate the {@link NotificationPolicy} of the parent {@link Notification}
	 * 
	 * @param policy
	 * @param attempt
	 */
	protected void handleCancelledNotification(NotificationPolicy policy, NotificationAttempt attempt) {
		
		log.debug("Notification " + attempt.getNotificationId() + " has been disabled by upstream process. No further attempts will be made.");
		// attempts have exceeded policy in some way
		if (policy == null || policy.isSaveOnFailure()) {
			saveFailedAttempt(attempt);
		}
		else {
			log.debug("Skipping persistence of failed attempt " + attempt.getUuid() + 
					" due to notification " + attempt.getNotificationId() + " policy.");
		}
	}
	
	/**
	 * Writes a {@link NotificationAttempt} to the failed queue for the parent
	 * {@link Notification}
	 * 
	 * @param attempt
	 * @throws NotificationPolicyViolationException
	 * @throws NotificationException
	 */
	protected void saveFailedAttempt(NotificationAttempt attempt) {
		
		log.debug("Pushing failed notification attempt for " + attempt.getNotificationId() + " on to the failed notification stack.");
		try {
			FailedNotificationAttemptQueue.getInstance().push(attempt);
			log.debug("Failed notification attempt for " + attempt.getNotificationId() + " successfully written to the failed notification stack.");
		} catch (NotificationException e1) {
			// can't connect with mongo. The attempt has already failed and there's no
			// way for us to persist it otherwise. We need to log the error and move on. 
			// bummer. lost info.
			log.error(e1);
		}
	}
	
	/**
	 * Writes a {@link NotificationAttempt} to the retry message queue
	 * for execution after a delay of {@code secondsUntilNextScheduledAttempt} seconds.
	 * 
	 * @param attempt
	 * @param secondsUntilNextScheduledAttempt
	 */
	protected void pushNotificationAttemptToRetryQueue(NotificationAttempt attempt, int secondsUntilNextScheduledAttempt) {
		MessageQueueClient queue = null;

		try {
			ObjectMapper mapper = new ObjectMapper();
			queue = MessageClientFactory.getMessageClient();
			
			queue.push(Settings.NOTIFICATION_RETRY_TOPIC,
					Settings.NOTIFICATION_RETRY_QUEUE, mapper.writeValueAsString(attempt), 
					secondsUntilNextScheduledAttempt);

		} 
		catch (MessagingException e) {
			log.error("Failed to connect to the messaging queue. Unable to return " + 
					attempt.getAttemptNumber() + " for notification " + attempt.getNotificationId(),
					e);
			
			saveFailedAttempt(attempt);
		} 
		catch (Throwable e) {
			log.error("Unexpected failure returning notification attempt " + 
					attempt.getAttemptNumber() + " for notification " + attempt.getNotificationId() + 
					" to the retry attempt queue.",
					e);
			saveFailedAttempt(attempt);
		} 
		finally {
			if (queue != null) {
				queue.stop();
			}
		}
	}

	/**
	 * @return the attempt
	 */
	public NotificationAttempt getAttempt() {
		return attempt;
	}

	/**
	 * @param attempt the attempt to set
	 */
	public void setAttempt(NotificationAttempt attempt) {
		this.attempt = attempt;
	}


	/**
	 * @return the notification
	 */
	public Notification getNotification() {
		return notification;
	}


	/**
	 * @param notification the notification to set
	 */
	public void setNotification(Notification notification) {
		this.notification = notification;
	}


}

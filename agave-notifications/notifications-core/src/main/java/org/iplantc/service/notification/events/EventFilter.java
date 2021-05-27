package org.iplantc.service.notification.events;

import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;


public interface EventFilter {

	/**
	 * @return the notification
	 */
    Notification getNotification();

	/**
	 * @param notification the notification to set
	 */
    void setNotification(Notification notification);

	/**
	 * @return the dao
	 */
    NotificationDao getDao();

	/**
	 * @param dao the dao to set
	 */
    void setDao(NotificationDao dao);

	/**
	 * @return the event
	 */
    String getEvent();

	/**
	 * @param event the event to set
	 */
    void setEvent(String event);

	/**
	 * @return the owner
	 */
    String getOwner();

	/**
	 * @param owner the owner to set
	 */
    void setOwner(String owner);

	/**
	 * @return the associatedUuid
	 */
    AgaveUUID getAssociatedUuid();

	/**
	 * @param associatedUuid the associatedUuid to set
	 */
    void setAssociatedUuid(AgaveUUID associatedUuid);

	/**
	 * @return the responseCode
	 */
    int getResponseCode();

	/**
	 * @param responseCode the responseCode to set
	 */
    void setResponseCode(int responseCode);

	String resolveMacros(String text, boolean urlEncode);

	/**
	 * @return the customNotificationMessageContextData
	 */
    String getCustomNotificationMessageContextData();

	/**
	 * @param customNotificationMessageContextData the customNotificationMessageContextData to set
	 */
    void setCustomNotificationMessageContextData(
            String customNotificationMessageContextData);

	/**
	 * Creates an appropriate email subject for the given event. Notification event macros
	 * are resolved properly prior to returning.
	 * @return filtered email subject
	 */
    String getEmailSubject();
	
	/**
	 * Creates an appropriate email plain text body for the given event. Notification event macros
	 * are resolved properly prior to returning.
	 * @return filtered email body
	 */
    String getEmailBody();

	
	/**
	 * Handles default conversion of {@link #getEmailBody()} into HTML
	 * for templates who have not implemented the method by wrapping in a 
	 * <pre>&lt;div&gt;&lt;pre&gt;&lt;/pre&gt;%lt;/div&gt;</pre> 
	 * @return
	 */
    String getHtmlEmailBody();
	
}

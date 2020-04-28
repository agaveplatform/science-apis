package org.iplantc.service.notification.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.notification.model.Notification;

public class DefaultResourceEventFilter extends AbstractEventFilter {

    private static final Logger log = Logger.getLogger(DefaultResourceEventFilter.class);

    public DefaultResourceEventFilter(AgaveUUID associatedUuid, Notification notification, String event, String owner) {
        super(associatedUuid, notification, event, owner);
    }

    public DefaultResourceEventFilter(AgaveUUID associatedUuid, Notification notification, String event, String owner, String customNotificationMessageContextData) {
        super(associatedUuid, notification, event, owner, customNotificationMessageContextData);
    }

    /**
     * Creates an appropriate email subject for the given event. Notification event macros
     * are resolved properly prior to returning.
     *
     * @return filtered email subject
     */
    @Override
    public String getEmailSubject() {
        String resourceType = StringUtils.capitalize(getAssociatedUuid().getResourceType().name());
        String subject = null;
        if (StringUtils.equalsIgnoreCase(event, "created")) {
            subject = resourceType + " " + getAssociatedUuid().toString() + " was created";
        } else if (StringUtils.equalsIgnoreCase(event, "deleted")) {
            subject = resourceType + " " + getAssociatedUuid().toString() + " was deleted";
        } else if (StringUtils.equalsIgnoreCase(event, "updated")) {
            subject = resourceType + " " + getAssociatedUuid().toString() + " was updated";
        } else {
            subject = " " + getAssociatedUuid().toString() + " recieved a(n) ${EVENT} event";
        }

        return subject;
    }

    /**
     * Creates an appropriate email plain text body for the given event. Notification event macros
     * are resolved properly prior to returning.
     *
     * @return filtered email body
     */
    @Override
    public String getEmailBody() {
        String resourceType = StringUtils.capitalize(getAssociatedUuid().getResourceType().name());
        String body = resourceType + " ${UUID} published a ${EVENT} event with the following content:\n${RAW_JSON}";
        return resolveMacros(body, false);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.notification.events.EventFilter#getHtmlEmailBody()
     */
    @Override
    public String getHtmlEmailBody()
    {
        String resourceType = StringUtils.capitalize(getAssociatedUuid().getResourceType().name());
        String body = "<p>" + resourceType + " ${UUID} published a ${EVENT} event with "
                    + "the following content:</p>"
                    + "<br>"
                    + "<pre>${RAW_JSON}</pre>";

        return resolveMacros(body, false);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.notification.events.AbstractNotificationEvent#resolveMacros(java.lang.String, boolean)
     */
    @Override
    public String resolveMacros(String body, boolean urlEncode)
    {
        try
        {
            body = StringUtils.replace(body, "${UUID}", this.associatedUuid.toString());
            body = StringUtils.replace(body, "${EVENT}", this.event);
            body = StringUtils.replace(body, "${OWNER}", this.owner);

            if (StringUtils.isNotEmpty(getCustomNotificationMessageContextData())) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    JsonNode json = mapper.readTree(getCustomNotificationMessageContextData());
                    if (json.isObject()) {
                        body = StringUtils.replace(body, "${RAW_JSON}", mapper.writer().withDefaultPrettyPrinter().writeValueAsString(getCustomNotificationMessageContextData()));
                    }
                } catch (Exception e) {
                    body = StringUtils.replace(body, "${RAW_JSON}", getCustomNotificationMessageContextData());
                }
            }

            return body;
        }
        catch (Exception e) {
            log.error("Failed to create notification body", e);
            return "The status of postit with uuid " + associatedUuid.toString() +
                    " has changed to " + event;
        }
    }
}

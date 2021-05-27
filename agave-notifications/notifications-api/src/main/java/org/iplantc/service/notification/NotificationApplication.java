package org.iplantc.service.notification;

import org.iplantc.service.common.restlet.resource.QuartzUtilityResource;
import org.iplantc.service.notification.resources.impl.*;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;


public class NotificationApplication extends Application {

	/**
	 * @see javax.ws.rs.core.Application#getClasses()
	 */
	@Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> rrcs = new  HashSet<Class<?>>();
        // add all the resource beans
        rrcs.add(NotificationAttemptResourceImpl.class);
        rrcs.add(NotificationAttemptCollectionImpl.class);
        rrcs.add(NotificationResourceImpl.class);
        rrcs.add(NotificationCollectionImpl.class);
//        rrcs.add(FireNotificationResourceImpl.class);
        rrcs.add(QuartzResourceImpl.class);
        rrcs.add(QuartzUtilityResource.class);
        return rrcs;
    }
}

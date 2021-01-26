package org.iplantc.service.apps;

import org.iplantc.service.apps.resources.impl.*;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;


public class SoftwareApplication extends Application {

	/**
	 * @see javax.ws.rs.core.Application#getClasses()
	 */
	@Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> rrcs = new HashSet<Class<?>>();
        // add all the resource beans
        rrcs.add(SoftwareResourceImpl.class);
        rrcs.add(SoftwareCollectionImpl.class);
        rrcs.add(SoftwarePermissionResourceImpl.class);
        rrcs.add(SoftwarePermissionCollectionImpl.class);
        rrcs.add(SoftwareHistoryResourceImpl.class);
        rrcs.add(SoftwareHistoryCollectionImpl.class);
        rrcs.add(SoftwareFormResourceImpl.class);
        rrcs.add(QuartzResourceImpl.class);
        return rrcs;
    }
}

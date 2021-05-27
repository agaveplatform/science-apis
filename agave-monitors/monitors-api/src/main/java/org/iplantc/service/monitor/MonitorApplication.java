package org.iplantc.service.monitor;

import org.iplantc.service.monitor.resources.impl.*;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;


public class MonitorApplication extends Application {

	/**
	 * @see Application#getClasses()
	 */
	@Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> rrcs = new  HashSet<Class<?>>();
        // add all the resource beans
        rrcs.add(MonitorResourceImpl.class);
        rrcs.add(MonitorCheckResourceImpl.class);
        rrcs.add(MonitorCollectionImpl.class);
        rrcs.add(MonitorCheckCollectionImpl.class);
        rrcs.add(EntityEventCollectionImpl.class);
        rrcs.add(EntityEventResourceImpl.class);
        rrcs.add(QuartzResourceImpl.class);
        return rrcs;
    }
}

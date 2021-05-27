package org.iplantc.service.profile;

import org.iplantc.service.profile.resource.impl.InternalUserResourceImpl;
import org.iplantc.service.profile.resource.impl.ProfileResourceImpl;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

public class ProfileApplication extends Application 
{	
	/**
	 * @see javax.ws.rs.core.Application#getClasses()
	 */
	@Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> rrcs = new  HashSet<Class<?>>();
        // add all the resource beans
        rrcs.add(ProfileResourceImpl.class);
        rrcs.add(InternalUserResourceImpl.class);
        return rrcs;
    }
}


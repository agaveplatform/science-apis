package org.iplantc.service.tags;

import org.iplantc.service.tags.resource.impl.*;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;


public class TagsApplication extends Application {

	@Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> rrcs = new  HashSet<Class<?>>();
        // tag entities
        rrcs.add(TagResourceImpl.class);
        rrcs.add(TagsCollectionImpl.class);
        
        // tag associationIds
        rrcs.add(TagResourceResourceImpl.class);
        rrcs.add(TagResourcesCollectionImpl.class);
        
        // tag permissions
        rrcs.add(TagPermissionResourceImpl.class);
        rrcs.add(TagPermissionsCollectionImpl.class);
        
        // tag history
        rrcs.add(TagHistoryResourceImpl.class);
        rrcs.add(TagHistoryCollectionImpl.class);
        
        
        
        return rrcs;
    }
}

package org.iplantc.service.common.discovery.providers.sql;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * POJO for an API {@link DiscoverableService}. These represent the
 * core APIs and do not technically need to be discovered.
 * 
 * @author dooley
 *
 */
@Entity
@Table(name = "discoverableservices")
@DiscriminatorValue("API")
public class DiscoverableApi extends DiscoverableService
{	
	
}

package org.iplantc.service.common.discovery.providers.sql;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * POJO for an Worker {@link DiscoverableService}s. WorkerDiscoveryService
 * can be any elastically scalable process used across the platform. 
 * Workers are self-reporting and report their existence with a regular heartbeat.
 * 
 * @author dooley
 *
 */
@Entity
@Table(name = "discoverableservices")
@DiscriminatorValue("WORKER")
public class DiscoverableWorker extends DiscoverableService
{	
	
}

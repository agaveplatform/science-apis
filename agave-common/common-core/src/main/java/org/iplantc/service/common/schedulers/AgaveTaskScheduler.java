/**
 * 
 */
package org.iplantc.service.common.schedulers;

import org.iplantc.service.common.discovery.ServiceCapability;
import org.iplantc.service.common.exceptions.TaskSchedulerException;

import java.util.Set;

/**
 * General interface defining job schedulers.
 * 
 * @author dooley
 *
 */
public interface AgaveTaskScheduler
{
	String getNextTaskId(Set<ServiceCapability> capabilities) throws TaskSchedulerException;
}

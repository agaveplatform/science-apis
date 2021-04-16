/**
 * 
 */
package org.iplantc.service.jobs.managers;


import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.QuotaViolationException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * Generic quota check interface.
 * 
 * @author dooley
 *
 */
public interface QuotaCheck {

	public void check(Job job, ExecutionSystem executionSystem) throws QuotaViolationException, JobException;
}

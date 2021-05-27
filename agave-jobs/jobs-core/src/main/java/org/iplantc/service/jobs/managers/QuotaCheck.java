/**
 * 
 */
package org.iplantc.service.jobs.managers;


import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.QuotaViolationException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * Generic quota check interface.
 * 
 * @author dooley
 *
 */
public interface QuotaCheck {

	void check(Job job, ExecutionSystem executionSystem) throws QuotaViolationException, JobException;
}

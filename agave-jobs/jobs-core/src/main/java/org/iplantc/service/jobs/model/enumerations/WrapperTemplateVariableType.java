package org.iplantc.service.jobs.model.enumerations;

import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;

public interface WrapperTemplateVariableType
{

//	/**
//	 * Resolves a template variable into the actual runtime value
//	 * for a given job. Tenancy is honored with respect to the
//	 * job.
//	 *
//	 * @param job A valid job object
//	 * @return resolved value of the variable.
//	 * @throws JobMacroResolutionException when the macro cannot be resolved. Generally this is due to an unavailable execution system
//	 */
//	public abstract String resolveForJob(Job job) throws JobMacroResolutionException;
}
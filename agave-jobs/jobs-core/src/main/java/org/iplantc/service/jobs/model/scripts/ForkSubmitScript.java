/**
 * 
 */
package org.iplantc.service.jobs.model.scripts;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.enumerations.ParallelismType;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * @author dooley
 * 
 */
public class ForkSubmitScript extends AbstractSubmitScript {


	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public ForkSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		super(job, software, executionSystem);
	}

	/**
	 * Adds the bash header to the script, the process to capture the PID, and the 
	 * redirect of all stdout and stderr in the scrip to the named ${JOB_NAME}.err 
	 * and ${JOB_NAME}.out files.
	 */
	@Override
	public String getScriptText() throws JobMacroResolutionException
	{
		String jobSlug = Slug.toSlug(job.getName());
		return String.format("#!/bin/bash \n"
					+ "\n"
					+ "############################################################## \n"
					+ "# Agave Runtime IO Redirections \n"  
					+ "############################################################## \n\n"
					+ "# Capture the PID of this job on the system for remote monitoring \n"
					+ "echo $$ > %s.pid \n" 
					+ "\n"
					+ "# Pause for 2 seconds to avoid a callback race condition \n"
					+ "sleep 2 \n" 
					+ "\n"
					+ "# Redirect STDERR and STDOUT to the custom job output and error files \n"
					+ "exec 2>%s.err 1>%s.out \n\n\n",
				jobSlug, jobSlug, jobSlug);
	}
}

/**
 * 
 */
package org.iplantc.service.jobs.model.scripts;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.enumerations.ParallelismType;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * Concreate class for fully custom GridEngine batch submit scripts. This behaves 
 * similarly to the {@link SgeSubmitScript}, but does not attempt to 
 * set any info, rather deferring to the user to customize their scheduler
 * directives as they see fit.
 * 
 * @author dooley
 * 
 */
public class CustomGridEngineSubmitScript extends SgeSubmitScript 
{
	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public CustomGridEngineSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		super(job, software, executionSystem);
	}
	
	@Override
	public String getScriptText() throws JobMacroResolutionException
	{			
		if (StringUtils.isEmpty(queue.getCustomDirectives())) {
			return super.getScriptText();
		}
		else {
			String result = "#!/bin/bash\n\n" 
					+ SgeSubmitScript.DIRECTIVE_PREFIX + "-N " + name + "\n"
					+ ( inCurrentWorkingDirectory ? SgeSubmitScript.DIRECTIVE_PREFIX + "-cwd\n" : "" )
					+ SgeSubmitScript.DIRECTIVE_PREFIX + "-V\n" 
					+ SgeSubmitScript.DIRECTIVE_PREFIX + "-o " + standardOutputFile + "\n" 
					+ SgeSubmitScript.DIRECTIVE_PREFIX + "-e " + standardErrorFile + "\n";
			result += resolveMacros(queue.getCustomDirectives()) + "\n\n";
			
			return result;
		}
	}
}

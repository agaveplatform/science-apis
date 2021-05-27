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
 * Concreate class for SLURM batch submit scripts.
 * 
 * @author dooley
 * 
 */
public class SlurmSubmitScript extends AbstractSubmitScript {

	public static final String DIRECTIVE_PREFIX = "#SBATCH ";

	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public SlurmSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		super(job, software, executionSystem);
	}

	/**
	 * Serializes the object into a SLURM submit script. Assumptions made are
	 * that the number of nodes used will be the ceiling of the number of 
	 * processors requested divided by 16. For serial jobs, an entire node is requested.
	 */
	public String getScriptText() throws JobMacroResolutionException {
		String result = "#!/bin/bash\n" 
				+ DIRECTIVE_PREFIX + "-J " + name + "\n"
				+ DIRECTIVE_PREFIX + "-o " + standardOutputFile + "\n" 
				+ DIRECTIVE_PREFIX + "-e " + standardErrorFile + "\n" 
				+ DIRECTIVE_PREFIX + "-t " + time + "\n"
				+ DIRECTIVE_PREFIX + "-p " + queue.getEffectiveMappedName() + "\n"
				+ DIRECTIVE_PREFIX + "-N " + nodes + " -n " + processors + "\n";
				if (!StringUtils.isEmpty(queue.getCustomDirectives())) {
					result += DIRECTIVE_PREFIX + queue.getCustomDirectives() + "\n";
				}

		return result;
	}

}

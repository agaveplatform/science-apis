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
 * @author dooley
 * 
 */
public class LoadLevelerSubmitScript extends AbstractSubmitScript {

	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public LoadLevelerSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		super(job, software, executionSystem);
	}

	@Override
	public String getScriptText() throws JobMacroResolutionException {

		// #! /bin/bash -l
		// ## LoadLeveler script to submit 2 node, 4 task MPI program: hello
		// # @ job_type = MPICH
		// # @ class = LONG
		// # @ account_no = NONE
		// # @ node = 2
		// # @ tasks_per_node = 4
		// # @ wall_clock_limit = 10:00:00
		// # @ notification = always
		// # @ notify_user = <email_id>
		// # @ environment=COPY_ALL;
		// # @ output = hello.$(cluster).$(process).out
		// # @ error = hello.$(cluster).$(process).err
		// # @ queue

		// TODO: serialize script to pbs syntax
		// return null;

		String prefix = "#@ ";
		String result = "#! /bin/bash -l \n" 
				+ prefix + "- " + name + "\n"
				+ prefix + "environment = COPY_ALL\n" 
				+ prefix + "output = " + standardOutputFile + "\n" 
				+ prefix + "error = " + standardErrorFile + "\n" 
				+ prefix + "class = NORMAL\n"
				+ prefix + "account_no = NONE \n" 
				+ prefix + "wall_clock_limit = " + time + "\n";

		if (parallelismType.equals(ParallelismType.PTHREAD))
		{
			result += prefix + "job_type = MPICH\n";
			result += prefix + "node = 1\n";
			result += prefix + "tasks_per_node = " + processors + "\n";
		}
		else if (parallelismType.equals(ParallelismType.SERIAL))
		{
			result += prefix + "job_type = MPICH\n";
			result += prefix + "node_usage = not_shared\n";
			result += prefix + "nodes = 1\n";
			result += prefix + "tasks_per_node = 1\n";
		}
		else
		{
			result += prefix + "job_type = MPICH\n";
			result += prefix + "node_usage = not_shared\n";
			result += prefix + "nodes = " + nodes + "\n";
			result += prefix + "tasks_per_node = " + processors + "\n";
		}

		result += prefix + "queue " + queue.getEffectiveMappedName() + "\n";
		
		if (!StringUtils.isEmpty(queue.getCustomDirectives())) {
			result += prefix + queue.getCustomDirectives() + "\n";
		}
		
//		if (!StringUtils.isEmpty(system.getDefaultQueue().getCustomDirectives())) {
//			result += system.getDefaultQueue().getCustomDirectives() + "\n";
//		}
//		
////		for (String directive : system.getDefaultQueue().getCustomDirectives()) {
////			if (!StringUtils.isEmpty(directive)) {
////				result += prefix + directive + "\n";
////			}
////		}
		
		return result;
	}

}

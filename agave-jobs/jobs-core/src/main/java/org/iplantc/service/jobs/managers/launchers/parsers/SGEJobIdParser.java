package org.iplantc.service.jobs.managers.launchers.parsers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.RemoteJobIDParsingException;
import org.iplantc.service.jobs.exceptions.SchedulerException;

/**
 * Parses the output from a qsub command into a local job id 
 * that can be used for querying later on.
 * 
 * @author dooley
 *
 */
public class SGEJobIdParser implements RemoteJobIdParser {

	@Override
	public String getJobId(String output) throws RemoteJobIDParsingException, JobException, SchedulerException
	{
		Pattern pattern = Pattern.compile("Your job(?:-array)? (\\d+)(?:\\.\\d+-\\d+:\\d)? \\(\"([^\"]*)\"\\) has been submitted");
		Matcher matcher = pattern.matcher(output);
		
		// if the response matches the standard SGE qsub response, return the matching job id
		if (matcher.find())
		{
			return matcher.group(1);
		}
		else if (output.toLowerCase().contains("error")) {
			for (String line : output.split("\\r?\\n|\\r")) {
				if (line.contains("sbatch: error") ||
						line.contains("slurm_load_jobs error") ||
						line.contains("slurm_load_jobs error") ||
						line.contains("ERROR: Unknown project")) {
					throw new SchedulerException(output);
				}
			}
			// no obvious scheduler error was found. we fail with a generic job exception and continue to retry
			throw new JobException(output);
		}
		// otherwise, see what we can learn about the cause of the failure.
		else if (output.contains("You have exceeded the max job time limit.")) {
			throw new JobException(output);
		} 
		// likely the master server is down
		else if (output.contains("qmaster")) {
			throw new SchedulerException("Failed to submit job. Response from the scheduler was: " + output, new JobException());
		}
		else {
			throw new JobException("Failed to submit job. Response from the scheduler was: " + output); 
		}
	}

}

package org.iplantc.service.jobs.managers.launchers.parsers;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.RemoteJobIDParsingException;
import org.iplantc.service.jobs.exceptions.SchedulerException;

/**
 * Parses the output from a sbatcj command into a local job id 
 * that can be used for querying later on.
 * 
 * @author dooley
 *
 */
public class SlurmJobIdParser implements RemoteJobIdParser {

	@Override
	public String getJobId(String output) throws RemoteJobIDParsingException, JobException, SchedulerException
	{
		Pattern pattern = Pattern.compile("Submitted batch job \\d+");
		Matcher matcher = pattern.matcher(output);

		if (!matcher.find())
		{
			if (output.startsWith("sbatch: error:")) {
				output = output.substring("sbatch: error:".length());
			}
			throw new RemoteJobIDParsingException(output); 
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
		else
		{
			output = matcher.group();
			
			pattern = Pattern.compile("\\d+");
			matcher = pattern.matcher(output);
			matcher.find();
			
			return matcher.group();
		}
	}

}

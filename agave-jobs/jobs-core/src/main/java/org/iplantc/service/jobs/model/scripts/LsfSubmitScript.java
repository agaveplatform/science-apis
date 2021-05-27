/**
 * 
 */
package org.iplantc.service.jobs.model.scripts;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.enumerations.ParallelismType;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.joda.time.Duration;

/**
 * @author dooley
 * 
 */
public class LsfSubmitScript extends AbstractSubmitScript {

	public static final String DIRECTIVE_PREFIX = "#BSUB ";

	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public LsfSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		super(job, software, executionSystem);
	}

	/**
	 * Serializes the object to a bsub submit script.
	 */
	@Override
	public String getScriptText() throws JobMacroResolutionException {

		String result = "#!/bin/bash \n" 
			+ DIRECTIVE_PREFIX + "-J " + name + "\n"
			+ DIRECTIVE_PREFIX + "-o " + standardOutputFile + "\n" 
			+ DIRECTIVE_PREFIX + "-e " + standardErrorFile + "\n" 
			+ DIRECTIVE_PREFIX + "-W " + getTime() + "\n" // seconds not supported here
			+ DIRECTIVE_PREFIX + "-q " + queue.getEffectiveMappedName() + "\n"
			+ DIRECTIVE_PREFIX + "-L bash \n";
		
		if (parallelismType.equals(ParallelismType.PTHREAD))
		{
			result += DIRECTIVE_PREFIX + "-n " + nodes + "\n";
			result += DIRECTIVE_PREFIX + "-R 'span[ptile=1]'\n";
		}
		else if (parallelismType.equals(ParallelismType.SERIAL))
		{
			result += DIRECTIVE_PREFIX + "-n 1\n";
			result += DIRECTIVE_PREFIX + "-R 'span[ptile=1]'\n";
		}
		else
		{
			// assume parallel
			result += DIRECTIVE_PREFIX + "-n " + (nodes * processors) + "\n";
			result += DIRECTIVE_PREFIX + "-R 'span[ptile=" + processors + "]'\n";
		}
		
		if (!StringUtils.isEmpty(queue.getCustomDirectives())) {
			result += DIRECTIVE_PREFIX + queue.getCustomDirectives() + "\n";
		}
		
		return result;
	}
	
	/** 
	 * Formats the requested {@link Job#getMaxRunTime()} in integer minutes.
	 * If the overall duration is zero, a minimum run time of 1 minute is 
	 * returned. 
	 * 
	 * @returns the {@link Job#getMaxRunTime()} formatted into minutes.
	 */
	@Override
	public String getTime()
	{
		// convert the requested time from hhh:mm:ss format to milliseconds
		
		int maxRequestedTimeInMilliseconds = TimeUtils.getMillisecondsForMaxTimeValue(time);
		
		// LSF acceptes a minmum run time of 1 minute. Adjust 
		maxRequestedTimeInMilliseconds = Math.max(maxRequestedTimeInMilliseconds, 60000);
		Duration d = new Duration(maxRequestedTimeInMilliseconds);
		d.getStandardMinutes();
		return String.format("%d", d.getStandardMinutes());

//		// convert to a duration and print. we already pull in joda time
//		// so this saves us having to check for runtime ranges, rounding, etc.
//		Duration duration = new Duration(maxRequestedTimeInMilliseconds);
//
//		PeriodFormatter hm = new PeriodFormatterBuilder()
//		    .printZeroAlways()
//		    .minimumPrintedDigits(1) // gives the '01'
//		    .appendHours()
//		    .appendSeparator(":")
//		    .appendMinutes()
//		    .toFormatter();
//
//		return hm.print(duration.toPeriod());
		
	}


}

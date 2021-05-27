package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.util.Arrays;
import java.util.List;

public class LoadLevelerJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(LoadLevelerJobStatusResponseParser.class);

	/**
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of(SchedulerType.LOADLEVELER, SchedulerType.CUSTOM_LOADLEVELER);
	}

	/**
	 * The job status query to LoadLeveler was of the form {@code llq <job_id>}. That means the response should
	 * come back in a tabbed column with the following fields
	 *
	 * <pre>
	 * $ llq 114
	 * Id               Owner    Submitted    ST  PRI Class        Running On
	 * ---------------- -------- -----------  --  --- ------------ ----------
	 * 114.11.0         tesuser  7/28 10:05   R   50  data_stage   lltest
	 * </pre>
	 *
	 * We parse the status value and use it to generate the appropriate {@link LoadLevelerJobStatus} used in the response.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param schedulerResponseText the response text from the remote scheduler
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
	 * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
	 */
	@Override
	public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
		if (StringUtils.isBlank(schedulerResponseText)) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Empty response received from job status check on the remote system. Since the job was " +
							"successfully placed into queue, this is likely caused by a communication issue with the " +
							"scheduler. Retrying may clear up the issue.");

		} else if (schedulerResponseText.toLowerCase().contains("qstat: unknown job id")) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Error response from job status check on the remote system. This is likely due to the job " +
							"completing and being purged from the qstat job cache. Calling tracejob with sufficient " +
							"permissions or examining the job logs should provide more information about the job.");
		}
		else if (schedulerResponseText.toLowerCase().contains("job id")
				|| schedulerResponseText.toLowerCase().contains("error")
				|| schedulerResponseText.toLowerCase().contains("not ")) {
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		} else {
			String[] lines = StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+");
			// Iterate over each line, finding the one starting with the job id
			for (String line: lines) {
				// LoadLeveler status lines start with the job id
				if (line.startsWith(remoteJobId)) {
					try {
						// parse the line with the status info in it
						return parseLine(remoteJobId, line);
					} catch (RemoteJobMonitorResponseParsingException e) {
						throw new RemoteJobMonitorResponseParsingException(e.getMessage() + ": " + schedulerResponseText, e);
					}
				}
			}
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler");
		}
	}

	/**
	 * Parses a single status line from the remote server response. This should be the line containing the actual
	 * status info for the LoadLeveler job, trimmed of any headers, debug info, etc.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param statusLine the qstat job status line
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<LoadLevelerJobStatus> parseLine(String remoteJobId, String statusLine) throws RemoteJobMonitorResponseParsingException {

		List<String> tokens = Arrays.asList(StringUtils.split(statusLine));
		// output frsom {@code llq <job_id> } should be similar to one of the two following formats depending on LoadLeveler
		// <pre>Id               Owner    Submitted    ST  PRI Class        Running On</pre>

		// in case the response is customized, we start checking from the last column inward,
		// looking for single character values, which is what the status would be.
		String exitCode = "0";
		for (int i=tokens.size()-1; i > 0; i--) {
			if (tokens.get(i).matches("^([a-zA-Z]{1,2})$")) {
				try {
					LoadLevelerJobStatus remoteJobStatus = LoadLevelerJobStatus.valueOfCode(tokens.get(i));
					return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
				}
				catch (Throwable e) {
					throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
				}
			}
		}

		// The status line wasn't delimited like we thought and/or we couldn't find a valid status code, so we
		// check for the job id as the first token indicating that it was a status line, but that we could
		// not decipher the status. If found, we return a JobStatusResponse with LoadLevelerJobStatus.UNKNOWN
		// letting the scheduler know that the job was present, but no longer in a known running state.
		if (tokens.get(0).equals(remoteJobId)) {
			return new JobStatusResponse<>(remoteJobId, LoadLevelerJobStatus.UNKNOWN, exitCode);
		} else {
			// Otherwise, we don't really know what we have, so we reject the status line and throw a
			// RemoteJobMonitorResponseParsingException indicating we couldn't parse the response.
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + statusLine);
		}
	}
}

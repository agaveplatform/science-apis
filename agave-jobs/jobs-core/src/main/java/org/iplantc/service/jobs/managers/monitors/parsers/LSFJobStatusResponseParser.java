package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.util.Arrays;
import java.util.List;

public class LSFJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(LSFJobStatusResponseParser.class);

	/**
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of(SchedulerType.LSF, SchedulerType.CUSTOM_LSF);
	}

	/**
	 * The job status query to LSF was of the form {@code bjobs -w -noheader <job_id>}. That means the response should
	 * come back in a tabbed column with the following fields
	 *
	 * <pre>
	 * $ bjobs -w -noheader 114
	 * 114     testuser RUN   normal     fc0b07ca1fb9 fc0b07ca1fb9 test_job   May  9 02:59
	 * </pre>
	 *
	 * We parse the status value and use it to generate the appropriate {@link LSFJobStatus} used in the response.
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
		else if (schedulerResponseText.toLowerCase().contains("unknown")
				|| schedulerResponseText.toLowerCase().contains("error")
				|| schedulerResponseText.toLowerCase().contains("not ")) {
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		} else {
			List<String> lines = Arrays.asList(StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+"));
			// iterate over each line, finding the one starting with the job id
			for (String line: lines) {
				// PBS status lines start with the job id
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
	 * Parses a single status line from the remote server response. Job id, username, and status cannot contain
	 * spaces, so it is sufficient to split the status line and use the tokens. Expected format is:
	 * <pre>
	 * 114     testuser RUN   normal     fc0b07ca1fb9 fc0b07ca1fb9 test_job   May  9 02:59
	 * </pre>
	 * @param remoteJobId the remote job id to parse from the response
	 * @param statusLine the qstat job status line
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<PBSJobStatus> parseLine(String remoteJobId, String statusLine) throws RemoteJobMonitorResponseParsingException {

		List<String> tokens = Arrays.asList(StringUtils.split(statusLine));

		if (StringUtils.isNotBlank(tokens.get(0)) && remoteJobId.equals(tokens.get(0))) {
			try {
				PBSJobStatus remoteJobStatus = PBSJobStatus.valueOfCode(tokens.get(2));
				String exitCode = "0";
				return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
			}
			catch (Throwable e) {
				throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
			}
		}
		else {
			throw new RemoteJobMonitorResponseParsingException(
					"Unable to obtain job status in the response from the scheduler: " + statusLine);
		}
	}
}

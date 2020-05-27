package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Parses a standard Torque and TorquePro {@code qstat} job status checks when structured response data is not
 * The {@link TorqueXmlJobStatusResponseParser} class should be preferred whenever XML output is available from
 * the scheduler.
 * @deprecated
 * @see TorqueXmlJobStatusResponseParser
 */
public class TorqueJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(TorqueJobStatusResponseParser.class);

	/**
	 * The job status query to Torque was of the form {@code "qstat -a | grep ^<job_id>}. That means the response should
	 * come back in a tabbed column with the following fields.
	 *
	 * <pre>
	 * JobID                         Username        Queue           Jobname         SessID   NDS  TSK   ReqMemory ReqTime  S ElapsedTime
	 * </pre>
	 *
	 * A sample matching status like will look like:
	 * <pre>
	 * 0.33cf4118fced          testuser    debug    torque.submit       --    --     --     --   00:01:00 Q       --
	 * </pre>
	 *
	 * It is sufficient to split the string and look at the value in column 10 or 5.
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
		else if (schedulerResponseText.toLowerCase().contains("error")) {
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		} else {
			String[] lines = StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+");
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
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of(SchedulerType.TORQUE, SchedulerType.CUSTOM_TORQUE, SchedulerType.MOAB, SchedulerType.CUSTOM_MOAB);
	}

	/**
	 * Parses a single status line from the remote server response. This should be the line containing the actual
	 * status info for the PBS job, trimmed of any headers, debug info, etc.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param statusLine the qstat job status line
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<TorqueJobStatus> parseLine(String remoteJobId, String statusLine) throws RemoteJobMonitorResponseParsingException {

		JobStatusResponse<TorqueJobStatus> jobStatusResponse = null;
		List<String> tokens = Arrays.asList(StringUtils.split(statusLine));
		// output frsom {@code qstat -a <job_id> } should be similar to one of the two following formats depending on torque/pbspro
		// <pre>Job ID                         Username        Queue           Jobname         SessID   NDS  TSK   Memory Time  S Time</pre>

		if (tokens.size() == 11 || tokens.size() == 6) {
			try {
				TorqueJobStatus remoteJobStatus = TorqueJobStatus.valueOfCode(tokens.get(tokens.size() - 2));
				String exitCode = "0";

				jobStatusResponse = new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
			}
			catch (Throwable e) {
				throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
			}
		}
		// in case the response is customized, we start checking from the last column inward,
		// looking for single character values, which is what the status would be.
		else {
			String exitCode = "0";
			for (int i=tokens.size()-1; i > 0; i--) {
				if (tokens.get(i).matches("^([a-zA-Z]{1})$")) {
					try {
						TorqueJobStatus remoteJobStatus = TorqueJobStatus.valueOfCode(tokens.get(i));
						jobStatusResponse = new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
						break;
					}
					catch (Throwable e) {
						throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
					}
				}
			}

			// The status line wasn't delimited like we thought and/or we couldn't find a valid status code, so we
			// check for the job id as the first token indicating that it was a status line, but that we could
			// not decipher the status. If found, we return a JobStatusResponse with TorqueJobStatus.UNKNOWN
			// letting the scheduler know that the job was present, but no longer in a known running state.
			if (tokens.get(0).equals(remoteJobId)) {
				jobStatusResponse = new JobStatusResponse<>(remoteJobId, TorqueJobStatus.UNKNOWN, exitCode);
			} else {
				// Otherwise, we don't really know what we have, so we reject the status line and throw a
				// RemoteJobMonitorResponseParsingException indicating we couldn't parse the response.
				throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + statusLine);
			}
		}
		return jobStatusResponse;
	}

	/**
	 * Parses a job full status response from the remote server response. This should be the full multi-line response.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param schedulerResponseText the full job status response for the job id
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<TorqueJobStatus> parseFullStatus(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorResponseParsingException {

		Properties props = new Properties();
		TorqueJobStatus remoteJobStatus = TorqueJobStatus.UNKNOWN;
		String exitCode = null;
		try (InputStream bis = new ByteArrayInputStream(schedulerResponseText.getBytes())) {
			props.load(bis);

			if (props.containsKey("job_state")) {
				remoteJobStatus = TorqueJobStatus.valueOfCode(props.getProperty("job_state"));
			} else {
				throw new RemoteJobMonitorResponseParsingException("Unable to find job status in the response from the scheduler");
			}

			if (props.containsKey("Exit_status")) {
				exitCode = props.getProperty("Exit_status");
			}

			return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
		} catch (IOException e) {
			throw new RemoteJobMonitorResponseParsingException("Unable to parse the response from the scheduler");
		}
	}
}

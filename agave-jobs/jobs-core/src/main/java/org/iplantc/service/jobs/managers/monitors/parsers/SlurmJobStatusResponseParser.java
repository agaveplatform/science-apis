package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SlurmJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(SlurmJobStatusResponseParser.class);

	/**
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of(SchedulerType.SLURM, SchedulerType.CUSTOM_SLURM);
	}

	/**
	 * The request made was for
	 * <pre>
	 *     sacct -p -o 'JOBID,State,ExitCode' -n -j <job_id>
	 * </pre>.
	 *
	 * That means the response should come back in a pipe-delimited string as
	 * <pre>
	 *   <job_id>|<state>|<exit_code>|
	 * </pre>.
	 * It is sufficient to split the string and look at the second term
	 *
	 * @param remoteJobId           the remote job id to parse from the response
	 * @param schedulerResponseText the response text from the remote scheduler
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
	 * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
	 */
	@Override
	public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
		if (StringUtils.isBlank(schedulerResponseText)) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Empty response received from job status check on the remote system.");
		} else if (!schedulerResponseText.contains("|")) {
			throw new RemoteJobMonitorResponseParsingException(
					"Unexpected fields in the response from the scheduler: " + schedulerResponseText);
		} else {
			// split the response by newline
			List<String> lines = splitResponseByLine(schedulerResponseText);

			// iterate over each line looking for the job status line
			for (String line : lines) {
				// job id is the first token on the job status line, so if it doesn't start with that, pass
				// note: we do risk missing a match here due to truncated job id in the response
				if (!line.startsWith(remoteJobId)) continue;

				// split on pipe characters, removing the trailing pipe
				line = StringUtils.removeEnd(line, "|");
				List<String> tokens = Arrays.asList(StringUtils
						.splitByWholeSeparatorPreserveAllTokens(line, "|"));

				// there should be exactly 3 tokens. If more or less, throw an exception as we have already
				// identified this as the job status line and we cannot parse it.
				if (tokens.size() != 3) {
					throw new RemoteJobMonitorResponseParsingException(
							"Unexpected number of fields in the response from the scheduler: " + schedulerResponseText);
				} else {
					try {
						// we found our match. Now parse
						SlurmJobStatus statusType = SlurmJobStatus.valueOfCode(tokens.get(1));
						String exitCode = tokens.get(2);
						return new JobStatusResponse<>(remoteJobId, statusType, exitCode);
					} catch (Throwable e) {
						throw new RemoteJobMonitorResponseParsingException("Unexpected status found in the response from the scheduler: " + line);
					}
				}
			}
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		}
	}

	/**
	 * Splits the response by newline characters
	 * @param schedulerResponse the raw response from the remote scheduler
	 * @return a list of the {@code schedulerResponse} delimited by newlines and trimmed.
	 * @throws RemoteJobMonitorResponseParsingException if the response cannot be parsed
	 */
	protected List<String> splitResponseByLine(String schedulerResponse)
			throws RemoteJobMonitorResponseParsingException
	{
		BufferedReader rdr = null;
		List<String> lines = new ArrayList<String>();
		try {
			rdr = new BufferedReader(new StringReader(schedulerResponse));

			for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
				lines.add(StringUtils.trimToEmpty(line));
			}
		}
		catch (Exception e) {
			throw new RemoteJobMonitorResponseParsingException(
					"Unable to parse the response from the scheduler: " + schedulerResponse);
		}
		finally {
			try { if (rdr != null) rdr.close();} catch (Throwable ignored) {}
		}

		return lines;
	}

//
//	@Override
//	public boolean isJobRunning(String remoteServerRawResponse)
//	throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException, RemoteJobUnrecoverableStateException
//	{
//		SlurmJobStatusResponse statusResponse = new SlurmJobStatusResponse(remoteServerRawResponse);
//
//		try {
//			// if the state info is missing, job isn't running
//			if (StringUtils.isBlank(statusResponse.getStatus())) {
//				return false;
//			}
//			else {
//				SlurmJobStatus statusType = SlurmJobStatus.valueOf(statusResponse.getStatus().toUpperCase());
//
//				// if the job is in an unrecoverable state, throw the exception so the job is cleaned up
//				if (statusType == SlurmJobStatus.EQW) {
//					throw new RemoteJobUnrecoverableStateException(statusType.getDescription());
//				}
//				// is it in a known active state?
//				else if (statusType.isActiveStatus()) {
//					// raise a RemoteJobUnknownStateException exception here because the job
//					// will remain in that state without being killed by our monitors.
//					if (statusType == SlurmJobStatus.SUSPENDED) {
//						throw new RemoteJobUnknownStateException(statusResponse.getStatus(), statusType.getDescription());
//					}
//					else {
//						return true;
//					}
//				}
//				// if it's explicitly failed due to a runtime issue, denote that and throw an exception
//				else if (statusType == SlurmJobStatus.FAILED) {
//					throw new RemoteJobFailureDetectedException(statusType.getDescription() +
//							". Exit code was " + statusResponse.getExitCode());
//				}
//				else if (SlurmJobStatus.isFailureStatus(statusResponse.getStatus())) {
//					throw new RemoteJobFailureDetectedException(statusType.getDescription());
//				}
//				else {
//					return false;
//				}
//			}
//		}
//		catch (IllegalArgumentException e) {
//			throw new RemoteJobUnknownStateException(statusResponse.getStatus(), "Detected job in an unknown state ");
//		}
//	}
}

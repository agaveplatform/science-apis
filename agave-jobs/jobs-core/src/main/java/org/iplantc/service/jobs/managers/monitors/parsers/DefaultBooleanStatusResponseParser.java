package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.util.List;

public class DefaultBooleanStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(DefaultBooleanStatusResponseParser.class);

	/**
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of();
	}

	/**
	 * Parses the {@code schedulerResponseText} as a {@link Boolean} value. Truthy values resolve to
	 * {@link DefaultJobStatus#RUNNING} and non-truthy values resolve to {@link DefaultJobStatus#COMPLETED}.
	 *
	 * @param remoteJobId           the remote job id to parse from the response
	 * @param schedulerResponseText the response text from the remote scheduler
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorEmptyResponseException   if {@code schedulerResponseText} is blank
	 * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
	 */
	@Override
	public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
		if (StringUtils.isBlank(schedulerResponseText)) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Empty response received from job status check on the remote system.");
		} else {
			String exitCode = "0";
			DefaultJobStatus remoteJobStatus = null;
			if (Boolean.parseBoolean(schedulerResponseText)) {
				remoteJobStatus = DefaultJobStatus.RUNNING;
			} else {
				remoteJobStatus = DefaultJobStatus.COMPLETED;
			}

			return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
		}
	}
}

package org.iplantc.service.jobs.managers.monitors.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.io.IOException;
import java.util.List;

public class DefaultJsonStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(DefaultJsonStatusResponseParser.class);

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
	 * Parses the {@code schedulerResponseText} as a {@link ObjectNode} and checks the value of the "status" field
	 * in the object. Valid status values match any {@link DefaultJobStatus#getCode()} in a case insensitive way.
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
		} else if (!schedulerResponseText.contains("|")) {
			throw new RemoteJobMonitorResponseParsingException(
					"Unexpected fields in the response from the scheduler: " + schedulerResponseText);
		} else {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode json;
			try {
				json = mapper.readTree(schedulerResponseText);
				if (json.isObject()) {
					if (((ObjectNode)json).has("status")) {
						String statusValue = ((ObjectNode)json).get("status").asText();
						DefaultJobStatus remoteJobStatus = DefaultJobStatus.valueOfCode(statusValue);
						String exitCode = "0";
						return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
					} else {
						throw new RemoteJobMonitorResponseParsingException(
								"No status field found in JSON response from the scheduler: " + schedulerResponseText);
					}
				} else {
					throw new RemoteJobMonitorResponseParsingException(
							"JSON array found in response from the scheduler. " +
							"JSON object with status field required: " + schedulerResponseText);
				}
			} catch (IOException e) {
				throw new RemoteJobMonitorResponseParsingException("Invalid JSON response from the scheduler: " + schedulerResponseText, e);
			}
		}
	}
}

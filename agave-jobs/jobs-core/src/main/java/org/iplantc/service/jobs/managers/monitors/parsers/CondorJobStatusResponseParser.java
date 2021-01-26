package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Job status parser for CondorHT job log files. HTCondor does not have a descriptive queryable interface
 * for job status, and jobs are immediately purged from the {@code condor_q} response upon completion, so
 * we use the {@code runtime.log} file in the job directory to track job status. The log file is written
 * in "sections" with one section per job status update. The job status is given as a three digit code at
 * the start of a section header line.
 */
public class CondorJobStatusResponseParser implements JobStatusResponseParser {
    private static final String CONDOR_LOG_EVENT_EXIT_CODE_REGEX = "(?:.*\\(return value )(?<exitCode>[\\d]+)(?:\\).*)";
    private static final String CONDOR_LOG_EVENT_HEADER_REGEX = "(?<statusCode>[\\d]{3})\\s+\\((?<jobId>[\\d]+)(?:\\.[\\d]+\\.[\\d]+)\\)\\s+(?<eventDate>[0-9/]+)\\s+(?<eventTime>[0-9:]+)\\s+(?<eventMessage>.*)";
    private static final String CONDOR_LOG_EVENT_DELIMITER_REGEX = "\\.\\.\\.";
//    /**
//     * List of the {@link CondorLogJobStatus} codes found in the section headers of the condor log file.
//     * We use a list because order is important. Generally the last status is the one we care about,
//     * but we parse them all in the event there is a log race condition.
//     */
//    private ArrayList<CondorLogJobStatus> condorLogStatuses = new ArrayList<CondorLogJobStatus>();

    /**
     * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
     * can parse the job status query response.
     *
     * @return list of the {@link SchedulerType} supported buy this parser
     */
    @Override
    public List<SchedulerType> getSupportedSchedulerType() {
        return List.of(SchedulerType.CONDOR, SchedulerType.CUSTOM_CONDOR);
    }

    /**
     * Parses the file into sections based on predefined section headers and footers. A section header
     * follows the general pattern:
     *
     * <pre>
     * 000 (154.000.000) 01/31 14:27:43 Job submitted from host: <192.168.10.1:56091>
     * </pre>
     *
     * The First three digits of the header line mark the beginning of a section and indicate the condor status code
     * determining the job satus. A section break contains a single line with three periods, "...".
     *
     * A full log file for a test job will resemble the following:
     * <pre>
     * 000 (007.000.000) 05/27 11:27:01 Job submitted from host: <172.19.0.18:36901?addrs=172.19.0.18-36901>
     * ...
     * 001 (007.000.000) 05/27 11:27:04 Job executing on host: <172.19.0.18:49543?addrs=172.19.0.18-49543>
     * ...
     * 006 (007.000.000) 05/27 11:27:08 Image size of job updated: 1
     *         0  -  MemoryUsage of job (MB)
     *         0  -  ResidentSetSize of job (KB)
     * ...
     * 005 (007.000.000) 05/27 11:27:08 Job terminated.
     *         (1) Normal termination (return value 0)
     *                 Usr 0 00:00:00, Sys 0 00:00:00  -  Run Remote Usage
     *                 Usr 0 00:00:00, Sys 0 00:00:00  -  Run Local Usage
     *                 Usr 0 00:00:00, Sys 0 00:00:00  -  Total Remote Usage
     *                 Usr 0 00:00:00, Sys 0 00:00:00  -  Total Local Usage
     *         10325  -  Run Bytes Sent By Job
     *         6396  -  Run Bytes Received By Job
     *         10325  -  Total Bytes Sent By Job
     *         6396  -  Total Bytes Received By Job
     *         Partitionable Resources :    Usage  Request Allocated
     *            Cpus                 :                 1         1
     *            Disk (KB)            :       22       12  26480434
     *            Memory (MB)          :        0     1024      4004
     * ...
     * </pre>
     * @param remoteJobId           the remote job id to parse from the response
     * @param schedulerResponseText the response text from the remote scheduler
     * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
     * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
     * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
     *
     */
    public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
        schedulerResponseText = StringUtils.stripToEmpty(schedulerResponseText);
        if (schedulerResponseText.isEmpty()) {
            throw new RemoteJobMonitorEmptyResponseException(
                    "Empty response received from job status check on the remote system.");
        } else {
            // split the response into individual condor log events
            String[] logEventSections = schedulerResponseText.split(CONDOR_LOG_EVENT_DELIMITER_REGEX);

            // The string was not empty, so we know the split array will have at least one value,
            // thus no null check is required. We sanity check the first value to make sure the
            // file is an actual runtime log file.
            if (!isValidLogFile(logEventSections[0])) {
                throw new RemoteJobMonitorResponseParsingException("Invalid Condor log file format. " +
                        "No section header found on the first line.");
            }

            // parse the last section in the event log as that contains the current job status.
            return parseLogEventSection(remoteJobId, logEventSections[logEventSections.length - 1]);
        }
    }

    /**
     * Looks up the status of the last section header in the condor log file. If no section headers
     * were found, a status of {@link CondorJobStatus#UNKNOWN} is set.
     * @param condorLogEvent  the text of a single condor event log
     * @return status found in last section header
     * @throws RemoteJobMonitorResponseParsingException if the event log data cannot be parsed
     */
    protected JobStatusResponse parseLogEventSection(String remoteJobId, String condorLogEvent) throws RemoteJobMonitorResponseParsingException {
        // no section headers in the log file will result in an UNKNOWN status
        condorLogEvent = StringUtils.trimToEmpty(condorLogEvent);

        // empty event throws blank response exception as this should never happen
        if (StringUtils.isEmpty(condorLogEvent)) {
            throw new RemoteJobMonitorResponseParsingException("Empty event log found in condor runtime log file.");
        } // a three digit value followed by a space starting the event log section indicate a job status
        else {
            Matcher headerMatcher = Pattern.compile(CONDOR_LOG_EVENT_HEADER_REGEX).matcher(condorLogEvent);
            if (headerMatcher.find()) {
                // job id in the header will be left zero padded if the job id is less than 100. We strip rather than
                // parse as a Long because remote job ids are stored in the db as strings. This saves us a null check
                // and double conversion
                String strippedLogJobId = StringUtils.stripStart(headerMatcher.group("jobId"), "0");
                // condor job ids start at 1, so we don't need to worry about an job id of zero being empty and
                // triggering a false failure here
                if (StringUtils.isEmpty(strippedLogJobId)) {
                    throw new RemoteJobMonitorResponseParsingException(
                            "No job id found in current event for Condor runtime log file.");
                } else if ( !remoteJobId.equals(strippedLogJobId)) {
                    // if the job ids don't match up, throw an exception
                    throw new RemoteJobMonitorResponseParsingException(
                            "Mismatched job id found in current event for Condor runtime log file. " +
                                    strippedLogJobId + " != " + remoteJobId );
                } else {
                    // job ids match up, so we can parse the rest of the status line and return the interpreted response
                    String statusCode = headerMatcher.group("statusCode");
                    CondorLogJobStatus remoteJobStatus = CondorLogJobStatus.valueOfCode(statusCode);

                    // Use the condor event log message in leu of the default description, when possible, for more
                    // job-specific info about the job state.
                    String eventMessage = headerMatcher.group("eventMessage");
                    if (!StringUtils.isEmpty(eventMessage)) {
                        remoteJobStatus.setDescription(eventMessage);
                    }

                    // if an exit code was returned, we will parse that out here. We probably don't need to do this
                    // unless the job is in a terminal state, but we're only parsing a few lines of text and the
                    // complete condor state machine is not entirely clear, so we err on the side of certainty over
                    // the negligible performance savings.
                    Matcher matcher = Pattern.compile(CONDOR_LOG_EVENT_EXIT_CODE_REGEX).matcher(condorLogEvent);
                    String exitCode = "-1";
                    if (matcher.find()) {
                        exitCode = matcher.group("exitCode");
                    }

                    return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
                }
            } else {
                throw new RemoteJobMonitorResponseParsingException("Unexpected header format found when parsing current job " +
                        "event from Condor runtime log file: " + condorLogEvent );
            }
        }
    }

    /**
     * Checks the given section starts with a valid section header to start the file. If found and it has a
     * {@link CondorJobStatus#SUBMITTED} status code, we say it is valid. This should be safe as the first event in
     * every condor log file is a {@link CondorJobStatus#SUBMITTED} event.
     *
     * @param condorLogEvent A single section from the condor log file representing one event
     * @return true if the file begins with {@link CondorJobStatus#SUBMITTED} code.
     */
    protected boolean isValidLogFile(String condorLogEvent) {
        Matcher headerMatcher = Pattern.compile(CONDOR_LOG_EVENT_HEADER_REGEX).matcher(condorLogEvent);
        if (headerMatcher.find()) {
            CondorLogJobStatus actualStatus = CondorLogJobStatus.valueOfCode(headerMatcher.group("statusCode"));
            return actualStatus == CondorLogJobStatus.JOB_SUBMITTED;
        }
        return false;
    }
}


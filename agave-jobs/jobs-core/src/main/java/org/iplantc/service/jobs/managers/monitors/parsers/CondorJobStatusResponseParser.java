package org.iplantc.service.jobs.managers.monitors.parsers;

import java.util.*;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

/**
 * Job status parser for CondorHT job log files. CondorHT does not have a queryable interface
 * for job status, so we need to check the job log file for status info. The log file is written
 * in "sections" with one section per job status update. The job status is given as a three digit
 * code at the start of a section header line.
 */
public class CondorJobStatusResponseParser implements JobStatusResponseParser {
    /**
     * List of the {@link CondorJobStatus} codes found in the section headers of the condor log file.
     * We use a list because order is important. Generally the last status is the one we care about,
     * but we parse them all in the event there is a log race condition.
     */
    private ArrayList<CondorJobStatus> condorLogStatuses = new ArrayList<CondorJobStatus>();

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
     * <pre>000 (154.000.000) 01/31 14:27:43 Job submitted from host: <192.168.10.1:56091></pre>
     * <p>
     * The First three digits of the header line mark the beginning of a section and indicate the condor status code
     * determining the job satus. A section footer contains a single line with three periods, "...", and marks the
     * end of the section.
     *
     * @param remoteJobId           the remote job id to parse from the response
     * @param schedulerResponseText the response text from the remote scheduler
     * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
     * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
     * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
     */
    @Override
    public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
        if (org.apache.commons.lang.StringUtils.isBlank(schedulerResponseText)) {
            throw new RemoteJobMonitorEmptyResponseException(
                    "Empty response received from job status check on the remote system.");
        } else {
            String[] lines = org.apache.commons.lang.StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+");

            if (lines.length == 0) {
                throw new RemoteJobMonitorEmptyResponseException("Condor log file content was empty.");
            } else if (!isValidLogFile(lines[0])) {
                throw new RemoteJobMonitorResponseParsingException("Invalid Condor log file format. " +
                        "No section header found on the first line.");
            }

            // are there any lines to read
            int index_start = 0;
            while (index_start < lines.length) {
                // send index of section start
                index_start = parseLogFileSection(lines, index_start);
            }
            return parseLastLoggedStatus(remoteJobId);
        }
    }

    /**
     * Reads forward in the log file until the next section footer is reached. The header code and line are a
     * added to the {@link #condorLogStatuses} map and the line number representing the end of the section is
     * returned.
     * @param lines the newline split log file content
     * @param index the line number to start parsing the section.
     * @return the last line number of the parsed section.
     */
    protected int parseLogFileSection(String[] lines, int index){
        // this is the first line of the section
        String line = lines[index];
        // parse out the code to get the status info
        String code = line.substring(0,3);
        CondorJobStatus status = CondorJobStatus.valueOfCode(code);
//        condorLogSectionHeaders.put(status, line);  // add the first line of the section
        condorLogStatuses.add(status);  // add the first line of the section
        // read the next lines
        while (index < lines.length && !line.equals("...")) {
            line = lines[index++];
        }
        return index;
    }

    /**
     * Looks up the status of the last section header in the condor log file. If no section headers
     * were found, a status of {@link CondorJobStatus#UNKNOWN} is set.
     * @param remoteJobId           the remote job id to parse from the response
     * @return status found in last section header
     */
    protected JobStatusResponse parseLastLoggedStatus(String remoteJobId) {
        // no section headers in the log file will result in an UNKNOWN status
        if (condorLogStatuses.isEmpty()) {
            return new JobStatusResponse<>(remoteJobId, CondorJobStatus.UNKNOWN, "0");
        } else {
            // set to the status found in the last section header in the log file.
            CondorJobStatus statusType = condorLogStatuses.get(condorLogStatuses.size() - 1);
            return new JobStatusResponse<>(remoteJobId, statusType, "0");
        }
    }

    /**
     * Checks the given line for a valid section header to start the file. If found, it is treated as valid.
     * Generally, the first line of the log file should be provided here.
     * @return true if the file begins with {@link CondorJobStatus#SUBMITTED} code.
     */
    protected boolean isValidLogFile(String firstLine) {
        return firstLine != null && firstLine.contains(CondorJobStatus.SUBMITTED.getCode());
    }
}


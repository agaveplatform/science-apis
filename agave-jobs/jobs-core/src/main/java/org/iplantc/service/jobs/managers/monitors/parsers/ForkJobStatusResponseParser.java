package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.util.List;

public class ForkJobStatusResponseParser implements JobStatusResponseParser {

    private static final Logger log = Logger.getLogger(ForkJobStatusResponseParser.class);

    /**
     * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
     * can parse the job status query response.
     *
     * @return list of the {@link SchedulerType} supported buy this parser
     */
    @Override
    public List<SchedulerType> getSupportedSchedulerType() {
        return List.of(SchedulerType.FORK);
    }

    /**
     *  The job status query to LoadLeveler was of the form {@code ps -o pid= -o user= -o stat= -o time= -o comm= -p <job_id>}.
     * 	That means the response should come back in columns with the following fields:
     *
     *  <pre>
     * PID USER  STATUS  TIME COMMAND
     * </pre>
     * We parse the status value and use it to generate the appropriate {@link ForkJobStatus} used in the response.
     *
     * @param remoteJobId           the remote job id to parse from the response
     * @param schedulerResponseText the response text from the remote scheduler
     * @return a {@link JobStatusResponse containing remote status info about the job with {@code remoteJobId}
     * @throws RemoteJobMonitorEmptyResponseException   if {@code schedulerResponseText} is blank
     * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
     */
    @Override
    public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {

        if (StringUtils.isBlank(schedulerResponseText)) {
            throw new RemoteJobMonitorEmptyResponseException(
                    "Empty response received from job status check on the remote system. Since the job was " +
                            "successfully placed into queue, this is likely caused by a communication issue with the " +
                            "scheduler. Retrying may clear up the issue.");
        } else {
            String[] lines = StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+");
            for (String line : lines) {
                String trimmedLine = StringUtils.stripToEmpty(line);
                // bad syntax...possible on some OS, in theory
                if (StringUtils.isBlank(trimmedLine) || StringUtils.startsWithAny(trimmedLine, new String[]{"[", "usage", "ps"})) {
                    continue;
                }
                // pid is invalid
                else if (StringUtils.startsWith(trimmedLine, "ps: Invalid process id")) {
                    throw new RemoteJobMonitorResponseParsingException(schedulerResponseText);
                }
                // response isn't a show stopper out of the box. parse for more info
                else {
                    String[] tokens = StringUtils.split(trimmedLine);
                    String localId = StringUtils.stripToEmpty(tokens[0]);
                    if (!StringUtils.isNumeric(localId)) {
                        throw new RemoteJobMonitorResponseParsingException("Unexpected response format returned from ps: " + schedulerResponseText);
                    } else if (!StringUtils.equals(remoteJobId, localId)) {
                        // ignore any non-matching job ids
                        continue;
                    } else {
                        return new JobStatusResponse<>(remoteJobId, ForkJobStatus.valueOfCode(tokens[2]), "-1");
                    }
                }
            }
            // TODO: didn't find a line with the pid, that should mean the process is in a terminal state.
            return new JobStatusResponse<>(remoteJobId, ForkJobStatus.DONE, "-1");
        }
    }
}

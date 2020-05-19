package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.testng.annotations.Test;

public interface JobStatusResponseParserTest {
    JobStatusResponseParser getJobMonitorResponseParser();

    RemoteSchedulerJobStatus getUnknownStatus();

    @Test(dataProvider = "parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseExceptionProvider", expectedExceptions = RemoteJobMonitorEmptyResponseException.class)
    void parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException;

    @Test(dataProvider="parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingExceptionProvider", expectedExceptions = RemoteJobMonitorResponseParsingException.class)
    void parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse, String message) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException;

    @Test(dataProvider = "parseSchedulerResponseProvider")
    void parseSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus);

    @Test(dataProvider = "parseSchedulerResponseProvider")
    void parseCaseInsensitiveSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException;

    @Test(dataProvider = "parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider", expectedExceptions = RemoteJobMonitorResponseParsingException.class)
    void parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException;

    @Test(dataProvider = "parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatusProvider")
    void parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(String rawServerResponse) throws RemoteJobMonitorResponseParsingException;

    @Test(dataProvider = "parseMultilineSchedulerResponseProvider")
    void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus);
}

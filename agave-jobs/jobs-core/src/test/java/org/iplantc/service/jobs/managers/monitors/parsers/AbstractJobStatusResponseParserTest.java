package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class AbstractJobStatusResponseParserTest implements JobStatusResponseParserTest {

    /**
     * Returns a test job id appropriate for the scheduler
     * @return a test job id
     */
    public abstract String getTestSchedulerJobId();

    /**
     * Verifies that a parsing a known test scheduler response shoudl result in the proper
     * {@link RemoteSchedulerJobStatus}.
     *
     * @param remoteJobId the remote job id to pass to the remote system
     * @param schedulerOutput the remote scheduler output to parse and test for job status
     * @param expectedRemoteSchedulerJobStatus the expected {@link RemoteSchedulerJobStatus} after parsing
     * @param message the message to use in the assertion check
     */
    protected void _parseReturnsExpectedRemoteSchedulerJobStatus(String remoteJobId, String schedulerOutput, RemoteSchedulerJobStatus expectedRemoteSchedulerJobStatus, String message)
    {
        try {
            JobStatusResponseParser parser = getJobMonitorResponseParser();
            JobStatusResponse response = parser.parse(remoteJobId, schedulerOutput);
            Assert.assertEquals(response.getRemoteSchedulerJobStatus(), expectedRemoteSchedulerJobStatus, message);
        } catch (RemoteJobMonitorResponseParsingException | RemoteJobMonitorEmptyResponseException e) {
            Assert.fail("No exception should be thrown parsing scheduler output: " + schedulerOutput, e);
        }
    }

    /**
     * Verifies that a parsing throws an exception. The actual exception should bubble up to the calling method so
     * it can validate it using the {@link Test#expectedExceptions()} annotation field.
     *
     * @param remoteJobId the remote job id to pass to the remote system
     * @param rawServerResponse the remote scheduler output to parse and test for job status
     * @param message the message to use in the assertion check
     * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
     * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
     */
    protected void _parseThrowsException(String remoteJobId, String rawServerResponse, String message) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
        JobStatusResponseParser parser = getJobMonitorResponseParser();
        JobStatusResponse response = parser.parse(remoteJobId, rawServerResponse);
        Assert.fail(message);
    }

    @DataProvider
    protected abstract Object[][] parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseExceptionProvider();

    public void _parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
    {
        String message = "Empty response from the scheduler should throw RemoteJobMonitorEmptyResponseException";
        _parseThrowsException(getTestSchedulerJobId(), rawServerResponse, message);
    }

    @DataProvider
    protected abstract Object[][] parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingExceptionProvider();

    public void _parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse, String message) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
    {
        _parseThrowsException(getTestSchedulerJobId(), rawServerResponse, message);
    }

    @DataProvider
    protected abstract Object[][] parseSchedulerResponseProvider();

    public void _parseSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
    {
        String message = "Scheduler output should result in RemoteSchedulerJobStatus " + expectedStatus.toString();
        _parseReturnsExpectedRemoteSchedulerJobStatus(getTestSchedulerJobId(), rawServerResponse, expectedStatus, message);
    }

    public void _parseCaseInsensitiveSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
    {
        String message = "Parsed case insensitive status did not match expected status";
        _parseReturnsExpectedRemoteSchedulerJobStatus(getTestSchedulerJobId(), rawServerResponse.toLowerCase(), expectedStatus, message);
    }

    @DataProvider
    protected abstract Object[][] parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider();

    public void _parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
    {
        String message = "Parsing scheduler response with missing job id should throw a RemoteJobMonitorResponseParsingException";
        _parseThrowsException(getTestSchedulerJobId(), rawServerResponse, message);
    }

    @DataProvider
    protected abstract Object[][] parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatusProvider();

    public void _parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(String rawServerResponse) throws RemoteJobMonitorResponseParsingException
    {
        String message = "Empty status field in response should return UNKNOWN remote scheduler status";
        _parseReturnsExpectedRemoteSchedulerJobStatus(getTestSchedulerJobId(), rawServerResponse, getUnknownStatus(), message);
    }

    @DataProvider
    protected abstract Object[][] parseMultilineSchedulerResponseProvider();

   public void _parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
    {
        String message = "Multiline parsed status did not match expected status";
        _parseReturnsExpectedRemoteSchedulerJobStatus(getTestSchedulerJobId(), rawServerResponse, expectedStatus, message);
    }
}

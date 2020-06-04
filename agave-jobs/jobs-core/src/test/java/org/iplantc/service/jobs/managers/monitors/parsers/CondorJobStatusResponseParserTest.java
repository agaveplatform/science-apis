package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Test(groups={"unit"})
public class CondorJobStatusResponseParserTest extends AbstractJobStatusResponseParserTest {

	protected final static String TEST_SCHEDULER_JOB_ID = "124";
	protected final static String TEST_USER = "testuser";
	protected final static String TEST_STATUS_QUERY_RESPONSE_TEMPLATE_PATH = "schedulers/condor/runtime.log";
	protected String testSchedulerResponseTemplate;

	@BeforeClass
	public void beforeClass() {
		try (InputStream in = getClass().getClassLoader().getResourceAsStream(TEST_STATUS_QUERY_RESPONSE_TEMPLATE_PATH)) {
			testSchedulerResponseTemplate = Streams.asString(in);
		} catch (IOException e) {
			Assert.fail("Unable to load status query response template: " + TEST_STATUS_QUERY_RESPONSE_TEMPLATE_PATH);
		}
	}

    /**
     * Provides the instance of the {@link JobStatusResponseParser} under test
     * @return an instance of the concrete implementing class
     */
    @Override
    public JobStatusResponseParser getJobMonitorResponseParser()
    {
        return new CondorJobStatusResponseParser();
    }

    @Override
    public RemoteSchedulerJobStatus getUnknownStatus() {
        return CondorLogJobStatus.UNKNOWN;
    }

    /**
     * Returns a test job id appropriate for the scheduler
     * @return a test job id
     */
    @Override
    public String getTestSchedulerJobId() {
        return StringUtils.leftPad(TEST_SCHEDULER_JOB_ID, 3, "0");
    }

    /**
	 * Resolves the qstat status response template with the provided test values.
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatusCode the status code to set in the returned job status response
	 * @param testSchedulerJobExitCode the exit code to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, String testSchedulerJobStatusCode, String testSchedulerJobExitCode) {
		return testSchedulerResponseTemplate
				.replace("${JOB_ID}", StringUtils.leftPad(testSchedulerJobId, 3, "0"))
				.replace("${JOB_STATUS}", testSchedulerJobStatusCode)
				.replace("${JOB_USERNAME}", TEST_USER)
				.replace("${JOB_NODE}", "33cf4118fced")
				.replace("${JOB_EXIT_CODE}", testSchedulerJobExitCode);
	}

	/**
	 * Resolves the qstat status response template with the provided test values. Defaults exit code to "1".
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatusCode the status code to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, String testSchedulerJobStatusCode) {
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobStatusCode, "1");
	}

	/**
	 * Resolves the qstat status response template with the provided test values.
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatus the status to set in the returned job status response
	 * @param testSchedulerJobExitCode the exit code to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, CondorLogJobStatus testSchedulerJobStatus, String testSchedulerJobExitCode) {
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobStatus.getCode(), testSchedulerJobExitCode);
	}

	/**
	 * Resolves the qstat status response template with the provided test values. Defaults exit code to "0".
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatus the status to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 * @see #getSchedulerResponseString(String, CondorLogJobStatus, String)
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, CondorLogJobStatus testSchedulerJobStatus) {
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobStatus, "0");
	}


	@Override
	@DataProvider
	protected Object[][] parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseExceptionProvider()
	{
		String space = " ";
		String tab = "\t";
		String newline = "\n";
		String carriageReturn = "\r";

		List<Object[]> testCases = new ArrayList<Object[]>();
		testCases.add(new Object[]{ null });

		for (String c: List.of(space, tab, newline, carriageReturn)) {
			testCases.add(new Object[]{ c });
			testCases.add(new Object[]{ c + c });
			testCases.add(new Object[]{ c + space });
			testCases.add(new Object[]{ space + c });
			testCases.add(new Object[]{ space + c + space });
		}

		return testCases.toArray(new Object[][]{});
	}

	@Override
	@Test(dataProvider = "parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseExceptionProvider", expectedExceptions = RemoteJobMonitorEmptyResponseException.class)
	public void parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
		_parseBlankSchedulerResponseThrowsRemoteJobMonitorEmptyResponseException(rawServerResponse);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingExceptionProvider()
	{
		return new Object[][] {
				{ getSchedulerResponseString(getTestSchedulerJobId(), CondorLogJobStatus.JOB_SUBMITTED_TO_GRID_RESOURCE).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ getSchedulerResponseString(getTestSchedulerJobId(), CondorLogJobStatus.JOB_SUBMITTED_TO_GRID_RESOURCE).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ CondorLogJobStatus.JOB_SUBMITTED.getCode(), "Status only response should throw RemoteJobMonitorResponseParsingException" },
		};
	}

	@Override
	@Test(dataProvider="parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingExceptionProvider", expectedExceptions = RemoteJobMonitorResponseParsingException.class)
	public void parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse, String message) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
		_parseSchedulerResponseWithBadDelimiterThrowsRemoteJobMonitorResponseParsingException(rawServerResponse, message);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseProvider()
	{
		List<Object[]> testCases = new ArrayList<Object[]>();
		for (CondorLogJobStatus statusType: CondorLogJobStatus.values()) {
		    // skip unknown status as the empty value will cause an exception to be thrown
		    if (statusType != CondorLogJobStatus.UNKNOWN) {
                testCases.add(new Object[]{getSchedulerResponseString(getTestSchedulerJobId(), statusType), statusType});
            }
		}
		testCases.add(new Object[]{ getSchedulerResponseString(getTestSchedulerJobId(), "999"), CondorLogJobStatus.UNKNOWN });

		return testCases.toArray(new Object[][]{});
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider")
	public void parseSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseSchedulerResponse(rawServerResponse, expectedStatus);
	}

    /**
     * Creates the test cases for {@link #parseSchedulerResponseProvider()} with remote job id less than 3 digits in
     * length to validate the comparison of left zero padded job id in the log headers.
     * @return test cases for job id with 1 and 2 digits for every {@link CondorLogJobStatus} except UNKNOWN
     */
    @DataProvider
    protected Object[][] parseSchedulerResponseForSmallJobIdProvider()
    {
        List<Object[]> testCases = new ArrayList<Object[]>();
        for (String remoteJobId: List.of("1", "10", "1000", "10000", "1000000000")) {
            String altJobId = String.valueOf((long)Math.floor(Double.parseDouble(remoteJobId) * Math.PI));
            for (CondorLogJobStatus statusType : CondorLogJobStatus.values()) {
                // skip unknown status as the empty value will cause an exception to be thrown
                if (statusType != CondorLogJobStatus.UNKNOWN) {
                    testCases.add(new Object[]{remoteJobId, getSchedulerResponseString(remoteJobId, statusType), statusType});
                    testCases.add(new Object[]{altJobId, getSchedulerResponseString(altJobId, statusType), statusType});
                }
            }
            testCases.add(new Object[]{ remoteJobId, getSchedulerResponseString(remoteJobId, "999"), CondorLogJobStatus.UNKNOWN });
            testCases.add(new Object[]{ altJobId, getSchedulerResponseString(altJobId, "999"), CondorLogJobStatus.UNKNOWN });

        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "parseSchedulerResponseForSmallJobIdProvider")
    public void parseSchedulerResponseForSmallJobId(String remoteJobId, String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
    {
        String message = "Scheduler output for job id < 100 should result in RemoteSchedulerJobStatus " + expectedStatus.toString();
        _parseReturnsExpectedRemoteSchedulerJobStatus(remoteJobId, rawServerResponse, expectedStatus, message);
    }

	/**
	 * Case sensitivity matters in both the job status and the XML response, so we skip these tests
     *
	 * @param rawServerResponse tests server response
	 * @param expectedStatus expected output.
	 * @throws RemoteJobMonitorEmptyResponseException  if the {@code rawServerResponse} was empty. Should always be false
	 * @throws RemoteJobMonitorResponseParsingException if the {@code rawServerResponse} cannot be parsed.
	 */
	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider", enabled = false)
	public void parseCaseInsensitiveSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
		_parseCaseInsensitiveSchedulerResponse(rawServerResponse.toLowerCase(), expectedStatus);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider()
	{
		return new Object[][] {
				{getSchedulerResponseString("", CondorLogJobStatus.JOB_WAS_SUSPENDED)},
				{getSchedulerResponseString("foo-" + getTestSchedulerJobId() , CondorLogJobStatus.JOB_EXECUTING)},
				{getSchedulerResponseString(getTestSchedulerJobId() + "-foo" , CondorLogJobStatus.JOB_SUBMITTED)},
				{getSchedulerResponseString("foo-" + getTestSchedulerJobId() + "-foo" , CondorLogJobStatus.JOB_SUBMITTED)},
				{getSchedulerResponseString("", "")},
		};
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider", expectedExceptions = RemoteJobMonitorResponseParsingException.class)
	public void parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingException(String rawServerResponse) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
		_parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingException(rawServerResponse);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatusProvider()
	{
		return new Object[][] {};
	}

	/**
	 * This test checks that an empty status value will result in an unknown remote job status exception. However, for
	 * condor event log files, the status field appears first in the header line and its absence prevents the header
	 * from being parsed. Thus, we cover this particular use case in the {@link #parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingException(String)}
	 * test case rather than this one. This test remains a noop test for compliance with the interface.
	 * @param rawServerResponse the condor event log contents to test
	 */
	@Test(dataProvider = "parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatusProvider")
	public void parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(String rawServerResponse)
	{
//		_parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(rawServerResponse);
	}

	@Override
	@DataProvider
	protected Object[][] parseMultilineSchedulerResponseProvider()
	{
//		throw new SkipExc1ption("Multiline tests mean nothing when the response is multiline");
		return new Object[][] {};
	}

	@Override
	@Test(dataProvider = "parseMultilineSchedulerResponseProvider")
	public void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseMultilineSchedulerResponse(rawServerResponse, expectedStatus);
	}
}

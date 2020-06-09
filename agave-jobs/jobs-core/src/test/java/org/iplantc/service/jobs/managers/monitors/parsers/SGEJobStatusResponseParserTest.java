package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.fileupload.util.Streams;
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
public class SGEJobStatusResponseParserTest extends AbstractJobStatusResponseParserTest {

	protected final static String TEST_SCHEDULER_JOB_ID = "0";
	protected final static String TEST_USER = "testuser";
	protected final static String TEST_STATUS_QUERY_RESPONSE_TEMPLATE_PATH = "schedulers/sge/qstat.xml";
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
	 * Resolves the qstat status response template with the provided test values.
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatusCode the status code to set in the returned job status response
	 * @param testSchedulerJobExitCode the exit code to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, String testSchedulerJobStatusCode, String testSchedulerJobExitCode) {
		return testSchedulerResponseTemplate
				.replace("${JOB_ID}", testSchedulerJobId)
				.replace("${JOB_STATUS}", testSchedulerJobStatusCode)
				.replace("${JOB_USERNAME}", TEST_USER)
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
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobId, "1");
	}

	/**
	 * Resolves the qstat status response template with the provided test values.
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatus the status to set in the returned job status response
	 * @param testSchedulerJobExitCode the exit code to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, SGEJobStatus testSchedulerJobStatus, String testSchedulerJobExitCode) {
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobStatus.getCode(), testSchedulerJobExitCode);
	}

	/**
	 * Resolves the qstat status response template with the provided test values. Defaults exit code to "0".
	 *
	 * @param testSchedulerJobId the job id to set in the returned job status response
	 * @param testSchedulerJobStatus the status to set in the returned job status response
	 * @return a formatted job status response with the given values injected
	 * @see #getSchedulerResponseString(String, SGEJobStatus, String)
	 */
	private String getSchedulerResponseString(String testSchedulerJobId, SGEJobStatus testSchedulerJobStatus) {
		return getSchedulerResponseString(testSchedulerJobId, testSchedulerJobStatus, "0");
	}

	/**
	 * Returns a test job id appropriate for the scheduler
	 * @return a test job id
	 */
	@Override
	public String getTestSchedulerJobId() {
		return TEST_SCHEDULER_JOB_ID;
	}
	
	/**
	 * Provides the instance of the {@link JobStatusResponseParser} under test
	 * @return an instance of the concrete implementing class
	 */
	@Override
	public JobStatusResponseParser getJobMonitorResponseParser()
	{
		return new SGEJobStatusResponseParser();
	}

	@Override
	public RemoteSchedulerJobStatus getUnknownStatus() {
		return SGEJobStatus.UNKNOWN;
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
				{ getSchedulerResponseString(getTestSchedulerJobId(), SGEJobStatus.w).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ getSchedulerResponseString(getTestSchedulerJobId(), SGEJobStatus.w).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ getSchedulerResponseString(getTestSchedulerJobId(), SGEJobStatus.w).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ getSchedulerResponseString(getTestSchedulerJobId(), SGEJobStatus.w).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },

				{ SGEJobStatus.w.getCode(), "Status only response should throw RemoteJobMonitorResponseParsingException" },
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
		for (SGEJobStatus statusType: SGEJobStatus.values()) {
			testCases.add(new Object[]{getSchedulerResponseString(getTestSchedulerJobId(), statusType), statusType });
			testCases.add(new Object[]{getSchedulerResponseString(getTestSchedulerJobId(), statusType), statusType });
		}
		testCases.add(new Object[]{ getSchedulerResponseString(getTestSchedulerJobId(), "UNKNOWN"), SGEJobStatus.UNKNOWN });
		testCases.add(new Object[]{ getSchedulerResponseString(getTestSchedulerJobId(), "asdfasdfasdfadfa"), SGEJobStatus.UNKNOWN });

		return testCases.toArray(new Object[][]{});
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider")
	public void parseSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseSchedulerResponse(rawServerResponse, expectedStatus);
	}

	/**
	 * Case sensitivity matters in both the job status and the XML response, so we skip these tests
	 * @param rawServerResponse tests server response
	 * @param expectedStatus expected output.
	 * @throws RemoteJobMonitorEmptyResponseException  if the {@code rawServerResponse} was empty. Should always be false
	 * @throws RemoteJobMonitorResponseParsingException if the {@code rawServerResponse} cannot be parsed.
	 */
	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider", enabled = false)
	public void parseCaseInsensitiveSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
//		_parseCaseInsensitiveSchedulerResponse(rawServerResponse.toLowerCase(), expectedStatus);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider()
	{
		return new Object[][] {
				{getSchedulerResponseString("", SGEJobStatus.S)},
				{getSchedulerResponseString("foo-" + getTestSchedulerJobId() , SGEJobStatus.r)},
				{getSchedulerResponseString(getTestSchedulerJobId() + "-foo" , SGEJobStatus.w)},
				{getSchedulerResponseString("foo-" + getTestSchedulerJobId() + "-foo" , SGEJobStatus.w)},
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
		return new Object[][] {
				{ getSchedulerResponseString(getTestSchedulerJobId(), "") },
		};
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatusProvider")
	public void parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(String rawServerResponse) throws RemoteJobMonitorResponseParsingException
	{
		_parseSchedulerResponseWithEmptyStatusValueReturnsUnknownRemoteJobStatus(rawServerResponse);
	}

	@Override
	@DataProvider
	protected Object[][] parseMultilineSchedulerResponseProvider()
	{
//		throw new SkipException("Multiline tests mean nothing when the response is multiline");
		return new Object[][] {};
	}

	@Override
	@Test(dataProvider = "parseMultilineSchedulerResponseProvider")
	public void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseMultilineSchedulerResponse(rawServerResponse, expectedStatus);
	}

}

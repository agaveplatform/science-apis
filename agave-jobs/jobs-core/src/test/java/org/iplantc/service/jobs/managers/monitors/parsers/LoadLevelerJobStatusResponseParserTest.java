package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups={"unit"})
public class LoadLevelerJobStatusResponseParserTest extends AbstractJobStatusResponseParserTest {

	protected final static String TEST_SCHEDULER_JOB_ID = "114";
	protected final String TEST_USER = "testuser";

	/**
	 * Returns a test job id appropriate for the scheduler
	 * @return a test job id
	 */
	@Override
	public String getTestSchedulerJobId() {
		return TEST_SCHEDULER_JOB_ID;
	}

	/**
	 * The job status query to LoadLeveler is {@code "llq <job_id>}. That means the response should
	 * come back in a space-delimited line with the following fields:
	 *
	 * <pre>
	 * Id               Owner    Submitted    ST  PRI Class        Running On
	 * </pre>
	 * We can then resolve a valid status line from the template using the following command:
	 * <pre>
	 * String.format(TEST_VALID_STATUS_LINE_TEMPLATE, TEST_SCHEDULER_JOB_ID, TEST_USER, LoadLevelerJobStatus.PENDING.getCode());
	 * </pre>
	 */
	public final static String TEST_VALID_STATUS_LINE_TEMPLATE = "%s     %s   7/28 10:05  %s  50  data_stage   compute12f07n08";

	/**
	 * Provides the instance of the {@link JobStatusResponseParser} under test
	 * @return an instance of the concrete implementing class
	 */
	@Override
	public JobStatusResponseParser getJobMonitorResponseParser()
	{
		return new LoadLevelerJobStatusResponseParser();
	}

	@Override
	public RemoteSchedulerJobStatus getUnknownStatus() {
		return LoadLevelerJobStatus.UNKNOWN;
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
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.PENDING.getCode()).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.PENDING.getCode()).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },

				{ LoadLevelerJobStatus.PENDING.getCode(), "Status only response should throw RemoteJobMonitorResponseParsingException" },
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
		for (LoadLevelerJobStatus statusType: LoadLevelerJobStatus.values()) {
			testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, statusType.getCode()), statusType });
		}
		testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "UNKNOWN"), LoadLevelerJobStatus.UNKNOWN });
		testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "asdfasdfasdfadfa"), LoadLevelerJobStatus.UNKNOWN });

		return testCases.toArray(new Object[][]{});
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider")
	public void parseSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseSchedulerResponse(rawServerResponse, expectedStatus);
	}

	@Override
	@Test(dataProvider = "parseSchedulerResponseProvider")
	public void parseCaseInsensitiveSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException
	{
		_parseCaseInsensitiveSchedulerResponse(rawServerResponse.toLowerCase(), expectedStatus);
	}

	@Override
	@DataProvider
	protected Object[][] parseSchedulerResponseWithMissingJobIdThrowsRemoteJobMonitorResponseParsingExceptionProvider()
	{
		return new Object[][] {
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, "", TEST_USER, LoadLevelerJobStatus.IDLE.getCode()) },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, "", TEST_USER,"") },
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
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "") },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), "", "") },
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
		return new Object[][] {
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n",
						getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.NOT_RUN.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.COMPLETED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.PENDING.getCode()), LoadLevelerJobStatus.NOT_RUN },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n",
						"foo-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.NOT_RUN.getCode(),
						getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.COMPLETED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.PENDING.getCode()), LoadLevelerJobStatus.COMPLETED },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n",
						"bar-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.NOT_RUN.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.COMPLETED.getCode(),
						getTestSchedulerJobId(), TEST_USER, LoadLevelerJobStatus.PENDING.getCode()), LoadLevelerJobStatus.PENDING },
		};
	}

	@Override
	@Test(dataProvider = "parseMultilineSchedulerResponseProvider")
	public void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseMultilineSchedulerResponse(rawServerResponse, expectedStatus);
	}

}

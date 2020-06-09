package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups={"unit"})
public class TorqueJobStatusResponseParserTest extends AbstractJobStatusResponseParserTest {

	protected final static String TEST_SCHEDULER_JOB_ID = "1125";
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
	 * The job status query to Torque is {@code "qstat -a | grep ^<job_id>}. That means the response should
	 * come back in a space-delimited line with the following fields:
	 *
	 * <pre>
	 * JobID                         Username        Queue           Jobname         SessID   NDS  TSK   Req'dMemory Req'dTime  S ElapTime
	 * a
	 * </pre>
	 * We can then resolve a valid status line from the template using the following command:
	 * <pre>
	 * String.format(TEST_VALID_STATUS_LINE_TEMPLATE, TEST_SCHEDULER_JOB_ID, TEST_USER, TorqueJobStatus.QUEUED.getCode());
	 * </pre>
	 */
	public final static String TEST_VALID_STATUS_LINE_TEMPLATE = "%s          %s    debug    torque.submit       --    --     --     --   00:01:00 %s       -- ";

	/**
	 * In some cases, the short qstat format will be returned. This happens frequently for users wrappign the qstat
	 * commandfor use with alternative schedulers. The output resembles the response from {@code "qstat ^<job_id>}.
	 * That means the response should come back in a space-delimited line with the following fields:
	 *
	 * <pre>
	 * Job ID                    Name             User            Time Use S Queue
	 * a
	 * </pre>
	 * We can then resolve a valid status line from the template using the following command:
	 * <pre>
	 * String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, TEST_SCHEDULER_JOB_ID, TEST_USER, TorqueJobStatus.QUEUED.getCode());
	 * </pre>
	 */
	public final static String TEST_VALID_SHORT_STATUS_LINE_TEMPLATE = "%s             torque.submit    %s               0 %s debug";

	public final static String TEST_INVALID_STATUS_LINE_TEMPLATE = "";
	public final static String TEST_VALID_STATUS_MULTILINE_TEMPLATE = "";

	/**
	 * Provides the instance of the {@link JobStatusResponseParser} under test
	 * @return an instance of the concrete implementing class
	 */
	@Override
	public JobStatusResponseParser getJobMonitorResponseParser()
	{
		return new TorqueJobStatusResponseParser();
	}

	@Override
	public RemoteSchedulerJobStatus getUnknownStatus() {
		return TorqueJobStatus.UNKNOWN;
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
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()).replaceAll("\\s+", "|"), "Pipe delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()).replaceAll("\\s+", ","), "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },

				{ TorqueJobStatus.QUEUED.getCode(), "Status only response should throw RemoteJobMonitorResponseParsingException" },
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
		for (TorqueJobStatus statusType: TorqueJobStatus.values()) {
			testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, statusType.getCode()), statusType });
			testCases.add(new Object[]{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, statusType.getCode()), statusType });
		}
		testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "UNKNOWN"), TorqueJobStatus.UNKNOWN });
		testCases.add(new Object[]{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "asdfasdfasdfadfa"), TorqueJobStatus.UNKNOWN });
		testCases.add(new Object[]{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "UNKNOWN"), TorqueJobStatus.UNKNOWN });
		testCases.add(new Object[]{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "asdfasdfasdfadfa"), TorqueJobStatus.UNKNOWN });

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
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, "", TEST_USER, TorqueJobStatus.COMPLETED.getCode()) },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, "", TEST_USER, TorqueJobStatus.COMPLETED.getCode()) },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE, "", TEST_USER,"") },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, "", TEST_USER,"") },
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
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), TEST_USER, "") },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE, getTestSchedulerJobId(), "", "") },
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
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.COMPLETED },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n",
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.FINISHED },
				{ String.format(TEST_VALID_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_STATUS_LINE_TEMPLATE + "\n",
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.QUEUED },

				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n",
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.COMPLETED },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n",
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.FINISHED },
				{ String.format(TEST_VALID_SHORT_STATUS_LINE_TEMPLATE +"\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n" + TEST_VALID_SHORT_STATUS_LINE_TEMPLATE + "\n",
						"bar-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.COMPLETED.getCode(),
						"foo-" + getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.FINISHED.getCode(),
						getTestSchedulerJobId(), TEST_USER, TorqueJobStatus.QUEUED.getCode()), TorqueJobStatus.QUEUED },
		};
	}

	@Override
	@Test(dataProvider = "parseMultilineSchedulerResponseProvider")
	public void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseMultilineSchedulerResponse(rawServerResponse, expectedStatus);
	}

}

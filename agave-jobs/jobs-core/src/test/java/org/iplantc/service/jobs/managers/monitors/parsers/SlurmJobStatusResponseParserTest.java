package org.iplantc.service.jobs.managers.monitors.parsers;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups={"unit"})
public class SlurmJobStatusResponseParserTest extends AbstractJobStatusResponseParserTest {

	protected final static String TEST_SCHEDULER_JOB_ID = "10974959";

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
		return new SlurmJobStatusResponseParser();
	}

	/**
	 * Returns the unknown status instance for this scheduler type
	 * @return UNKNOWN {@link RemoteSchedulerJobStatus}
	 */
	@Override
	public RemoteSchedulerJobStatus getUnknownStatus() {
		return SlurmJobStatus.UNKNOWN;
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
				{ TEST_SCHEDULER_JOB_ID + " COMPLETED 0:0 2016-11-28T15:54:11 ", "Space delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ TEST_SCHEDULER_JOB_ID + ",COMPLETED,0:0,2016-11-28T15:54:11,00:36:31,", "Comma delimited server response should throw RemoteJobMonitorResponseParsingException" },
				{ "COMPLETED", "Status only response should throw RemoteJobMonitorResponseParsingException" },
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
		for (SlurmJobStatus statusType: SlurmJobStatus.values()) {
			testCases.add(new Object[]{ TEST_SCHEDULER_JOB_ID + "|" + statusType.getCode() + "|0:0|", statusType });
		}
		testCases.add(new Object[]{ TEST_SCHEDULER_JOB_ID + "|UNKNOWN|0:0|", SlurmJobStatus.UNKNOWN });
		testCases.add(new Object[]{ TEST_SCHEDULER_JOB_ID + "|asdfasdfasdfadfa|0:0|", SlurmJobStatus.UNKNOWN });

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
				{  "|"+SlurmJobStatus.COMPLETED.getCode()+"||" },
				{ "|"+SlurmJobStatus.COMPLETED.getCode()+"|0:0|" },
				{ "||0:0|" },
				{ "|||" },
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
				{ TEST_SCHEDULER_JOB_ID + "||0.0|" },
				{ TEST_SCHEDULER_JOB_ID + "|||" },
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
				{ String.format("%s|%s|0:0|\n%s.batch|%s|0.0|\n%s|%s|0.0|\n",
						TEST_SCHEDULER_JOB_ID, SlurmJobStatus.COMPLETED.getCode(),
						TEST_SCHEDULER_JOB_ID, SlurmJobStatus.FAILED.getCode(),
						TEST_SCHEDULER_JOB_ID, SlurmJobStatus.PENDING.getCode()), SlurmJobStatus.COMPLETED },
				{ String.format("10974|%s|1:0|\n%s.batch|%s|0.15|\n",
						SlurmJobStatus.TIMEOUT.getCode(),
						TEST_SCHEDULER_JOB_ID, SlurmJobStatus.CANCELLED.getCode()), SlurmJobStatus.CANCELLED },
		};
	}

	@Override
	@Test(dataProvider = "parseMultilineSchedulerResponseProvider")
	public void parseMultilineSchedulerResponse(String rawServerResponse, RemoteSchedulerJobStatus expectedStatus)
	{
		_parseMultilineSchedulerResponse(rawServerResponse, expectedStatus);
	}

}

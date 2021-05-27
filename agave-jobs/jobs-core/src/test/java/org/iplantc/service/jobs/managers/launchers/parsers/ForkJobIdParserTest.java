package org.iplantc.service.jobs.managers.launchers.parsers;


import org.iplantc.service.jobs.exceptions.RemoteJobIDParsingException;
import org.iplantc.service.jobs.managers.launchers.CLILauncher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

@Test(groups={"unit"})
public class ForkJobIdParserTest {
	
	private static final String EXPECTED_JOB_ID = "12345";
	private static final String UNEXPECTED_JOB_ID = "54321";
	private static final String SCRIPT_NAME = "foo.ipcexe";
	
	@DataProvider
	public Object[][] getJobIdProvider() {
		return new Object[][] { 
			new Object[] { EXPECTED_JOB_ID , EXPECTED_JOB_ID, "Valid process response from the server should return the process id" },
			new Object[] { "[abcde]\n" + EXPECTED_JOB_ID + "\n", EXPECTED_JOB_ID, "Multiline response from the server with valid pid line should return the process id" },
			new Object[] { EXPECTED_JOB_ID + "\n[abcde]", EXPECTED_JOB_ID, "Multiline response from the server with valid pid line should return the process id" },
			new Object[] { "[" + UNEXPECTED_JOB_ID + "]\n" + EXPECTED_JOB_ID, EXPECTED_JOB_ID, "Multiline response from the server with valid pid line should return the process id" },
			new Object[] { EXPECTED_JOB_ID+EXPECTED_JOB_ID, EXPECTED_JOB_ID+EXPECTED_JOB_ID, "Partial response from the server including process id should return the process id" },
		};
	}

	/**
	 * Tests the detection of job id in the output from a remote process execution. The way the {@link CLILauncher}
	 * starts jobs, the response will either be the local process id or a dump of the captured stderr. Here
	 * we make sure that we catch lines with valid natural numbers only.
	 * @param remoteOutput the remote output to test
	 * @param expectedJobId the expected job id to be found from the response
	 * @param message the assertion message on failure
	 */
	@Test(dataProvider = "getJobIdProvider")
	public void getJobId(String remoteOutput, String expectedJobId, String message)
	throws RemoteJobIDParsingException {
		ForkJobIdParser parser = new ForkJobIdParser();
		try {
			Assert.assertEquals(parser.getJobId(remoteOutput),expectedJobId, message);
		} catch (RemoteJobIDParsingException e) {
			Assert.fail(message, e);
		}

	}

	@DataProvider
	public Object[][] getJobIdThrowsRemoteJobIDParsingExceptionProvider() {
		return new Object[][] {
				new Object[] { SCRIPT_NAME, "Partial response from CLILauncher including only script name first should throw RemoteJobIDParsingException" },
				new Object[] { "", "empty CLILauncher output from the server should throw RemoteJobIDParsingException" },
				new Object[] { null, "null CLILauncher output  from the server should throw RemoteJobIDParsingException" },
				new Object[] { "[" + EXPECTED_JOB_ID + "]", "Singly bracked job id in response from the server should throw RemoteJobIDParsingException" },
				new Object[] { "[" + EXPECTED_JOB_ID + "] " + SCRIPT_NAME, "Background pid response from the CLILauncher should return the process id" },
				new Object[] { "Welcome ...\nThis is a test\nof the national broadcasting system\n\n", "Multiline response without a process id from the server should throw RemoteJobIDParsingException" },
		};
	}

	/**
	 * Tests that when a process id is not found in the remote response, a {@link RemoteJobIDParsingException}
	 * is thrown.
	 * @param remoteOutput the remote output to test
	 * @param message the assertion message on failure
	 */
	@Test(dataProvider = "getJobIdThrowsRemoteJobIDParsingExceptionProvider",
			expectedExceptions = RemoteJobIDParsingException.class)
	public void getJobIdThrowsRemoteJobIDParsingException(String remoteOutput, String message)
	throws RemoteJobIDParsingException {
		ForkJobIdParser parser = new ForkJobIdParser();
		parser.getJobId(remoteOutput);
		Assert.fail(message);
	}

	
	@DataProvider
	public Object[][] getJobIdIgnoresWhitespacePaddingProvider() {
		List<Object[]> testCases = new ArrayList<Object[]>();
		String singleSpace = " ";
		String doubleSpace = "  ";
		String tab = "\t";
		String newline = "\n";
		String carriageReturn = "\r";
		String newlineCarraigeReturn = newline + carriageReturn;
		
		for (String whitespace: Arrays.asList(singleSpace, doubleSpace, tab)) {
			testCases.add(new Object[] { whitespace + EXPECTED_JOB_ID , EXPECTED_JOB_ID, "Partial response with only the job id preceded by '"+whitespace+"' returns the process id" });
			testCases.add(new Object[] { EXPECTED_JOB_ID + whitespace , EXPECTED_JOB_ID, "Partial response with only the job id trailed by '"+whitespace+"' returns the process id" });
			testCases.add(new Object[] { whitespace + EXPECTED_JOB_ID + whitespace , EXPECTED_JOB_ID, "Partial response with only the job id bookended by '"+whitespace+"' returns the process id" });
		}

		for (String lineBreak: Arrays.asList(newline, carriageReturn, newlineCarraigeReturn)) {
			String quotedLineBreak = Matcher.quoteReplacement(lineBreak);
			testCases.add(new Object[]{EXPECTED_JOB_ID + lineBreak + SCRIPT_NAME, EXPECTED_JOB_ID, "Multiline response with process id and script name separated by '" + quotedLineBreak + "' should return process id"});
			testCases.add(new Object[]{lineBreak + EXPECTED_JOB_ID + lineBreak + SCRIPT_NAME, EXPECTED_JOB_ID, "Multiline response with process id and script name preceded and separated by '" + quotedLineBreak + "' should return process id"});
			testCases.add(new Object[]{EXPECTED_JOB_ID + lineBreak + SCRIPT_NAME + lineBreak, EXPECTED_JOB_ID, "Multiline response with process id and script name trailed and separated by '" + quotedLineBreak + "' should return process id"});
			testCases.add(new Object[]{lineBreak + EXPECTED_JOB_ID + lineBreak + SCRIPT_NAME + lineBreak, EXPECTED_JOB_ID, "Multiline response with process id and script name bookended and separated by '" + quotedLineBreak + "' should return process id"});

			testCases.add(new Object[]{EXPECTED_JOB_ID + lineBreak + UNEXPECTED_JOB_ID, EXPECTED_JOB_ID, "Multiline response with valid process id on multiple lines separated by " + quotedLineBreak + " should return process id"});
			testCases.add(new Object[]{lineBreak + EXPECTED_JOB_ID + lineBreak + UNEXPECTED_JOB_ID, EXPECTED_JOB_ID, "Multiline response with process id and script name preceded and separated by '" + quotedLineBreak + "' should return process id"});
			testCases.add(new Object[]{EXPECTED_JOB_ID + lineBreak + UNEXPECTED_JOB_ID + lineBreak, EXPECTED_JOB_ID, "Multiline response with process id and script name trailed and separated by '" + quotedLineBreak + "' should return process id"});
			testCases.add(new Object[]{lineBreak + EXPECTED_JOB_ID + lineBreak + UNEXPECTED_JOB_ID + lineBreak, EXPECTED_JOB_ID, "Multiline response with process id and script name bookended and separated by '" + quotedLineBreak + "' should return process id"});
		}

		return testCases.toArray(new Object[][]{}); 
	}

	/**
	 * Tests the detection of job id in the output from a remote process execution regardless of whitespace.
	 * The way the {@link CLILauncher} starts jobs, the response will either be the local process id or a
	 * dump of the captured stderr. Here we make sure that we catch lines with valid natural numbers and whitespace
	 * only.
	 * @param remoteOutput the remote output to test
	 * @param expectedJobId the expected job id to be found from the response
	 * @param message the assertion message on failure
	 */
	@Test(dataProvider = "getJobIdIgnoresWhitespacePaddingProvider")
	public void getJobIdIgnoresWhitespacePadding(String remoteOutput, String expectedJobId, String message) {
		
		ForkJobIdParser parser = new ForkJobIdParser();
		try {
			Assert.assertEquals(parser.getJobId(remoteOutput), expectedJobId, message);
		}
		catch (Exception e) {
			Assert.fail(message, e);
		}
	}

	@DataProvider
	public Object[][] getJobIdIgnoresWhitespaceThrowsRemoteJobIDParsingExceptionProvider() {
		List<Object[]> testCases = new ArrayList<Object[]>();
		String singleSpace = " ";
		String doubleSpace = "  ";
		String tab = "\t";
		String newline = "\n";
		String carriageReturn = "\r";
		String newlineCarraigeReturn = newline + carriageReturn;

		for (String whitespace: Arrays.asList(singleSpace, doubleSpace, tab)) {
			testCases.add(new Object[] { EXPECTED_JOB_ID + whitespace + SCRIPT_NAME , "Job id and process name separated by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { whitespace + EXPECTED_JOB_ID + whitespace + SCRIPT_NAME , "Job id and process name preceded and separated by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { EXPECTED_JOB_ID + whitespace + SCRIPT_NAME + whitespace , "Job id and process name trailed and separated by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { whitespace + EXPECTED_JOB_ID + whitespace + SCRIPT_NAME + whitespace , "Job id and process name bookended and separated by '"+whitespace+"' should throw an RemoteJobIDParsingException" });

			testCases.add(new Object[] { whitespace + SCRIPT_NAME , "Partial response from CLILauncher with only the script name preceded by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { SCRIPT_NAME + whitespace , "Partial response from CLILauncher with only the script name trailed by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { whitespace + SCRIPT_NAME + whitespace , "Partial response from CLILauncher with only the script name bookended by '"+whitespace+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { whitespace, "Respone from CLILauncher with only whitespace value of '"+whitespace+"' should throw an RemoteJobIDParsingException" });
		}

		for (String lineBreak: Arrays.asList(newline, carriageReturn, newlineCarraigeReturn)) {
			testCases.add(new Object[] { lineBreak + SCRIPT_NAME , "Partial response from CLILauncher with only the script name preceded by '"+lineBreak+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { SCRIPT_NAME + lineBreak , "Partial response from CLILauncher with only the script name trailed by '"+lineBreak+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { lineBreak + SCRIPT_NAME + lineBreak , "Partial response from CLILauncher with only the script name bookended by '"+lineBreak+"' should throw an RemoteJobIDParsingException" });
			testCases.add(new Object[] { lineBreak, "Respone from CLILauncher with only lineBreak value of '"+lineBreak+"' should throw an RemoteJobIDParsingException" });
		}

		return testCases.toArray(new Object[][]{});
	}

	/**
	 * Tests that whitespace is a non-factor in checking for a job id. When one is not found, a
	 * {@link RemoteJobIDParsingException} is thrown.
	 * @param remoteOutput the remote output to test
	 * @param message the assertion message on failure
	 */
	@Test(dataProvider = "getJobIdIgnoresWhitespaceThrowsRemoteJobIDParsingExceptionProvider",
			expectedExceptions = RemoteJobIDParsingException.class)
	public void getJobIdIgnoresWhitespaceThrowsRemoteJobIDParsingException(String remoteOutput, String message)
	throws RemoteJobIDParsingException {
		ForkJobIdParser parser = new ForkJobIdParser();
		parser.getJobId(remoteOutput);
		Assert.fail(message);
	}
}

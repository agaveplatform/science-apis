package org.iplantc.service.transfer.http;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.iplantc.service.systems.Settings;
import org.iplantc.service.transfer.BaseTransferTestCase;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Reduced test harness for http client tests.
 */
@Test(groups={"http.operations"})
public class HTTPClientTest extends BaseTransferTestCase 
{
	private URI httpUri = null;
	private URI httpsUri = null;
	private URI httpPortUri = null;
	private URI httpsPortUri = null;
	private URI httpsQueryUri = null;
	private URI httpNoPathUri = null;
	private URI httpEmptyPathUri = null;
	private URI httpBasicUri = null;

	private URI fileNotFoundUri = null;
	private URI httpMissingPasswordBasicUri = null;
	private URI httpInvalidPasswordBasicUri = null;

    @BeforeClass
    protected void beforeClass() throws Exception 
    {
    	super.beforeClass();

    	httpUri = new URI("http://httpbin:8000/stream-bytes/32768");
    	httpsUri = new URI("https://httpbin:8443/stream-bytes/32768");
    	httpPortUri = new URI("http://httpd:8000/public/test_upload.bin");
    	httpsPortUri = new URI("https://httpd:8443/public/test_upload.bin");
    	httpsQueryUri = new URI("https://httpd:8443/public/test_upload.bin?t=now");
    	httpNoPathUri = new URI("http://httpd:8000");
    	httpEmptyPathUri = new URI("http://httpd:8000/");
    	httpBasicUri = new URI("http://testuser:testuser@httpd:8000/private/test_upload.bin");

    	fileNotFoundUri = new URI("http://httpd:8000/" + MISSING_FILE);
    	httpMissingPasswordBasicUri = new URI("http://testuser@httpd:8000/private/test_upload.bin");
    	httpInvalidPasswordBasicUri = new URI("http://testuser:testotheruser@httpd:8000/private/test_upload.bin");
    }
    
    @AfterClass
    protected void afterClass() throws Exception {}
    
    @BeforeMethod
    protected void beforeMethod() throws Exception {}
    
    @AfterMethod
    protected void afterMethod() {
    	try { client.disconnect(); } catch (Exception ignore) {}
    }

	/**
	 * Generates a random directory name for the remote test folder.
	 * @return string representing remote test directory.
	 */
	protected String getRemoteTestDirPath() {
		return UUID.randomUUID().toString();
	}

	private RemoteDataClient getClient(URI uri) throws Exception {
    	return new RemoteDataClientFactory().getInstance(SYSTEM_USER, null, uri);
	}
    
    @Test
	public void isPermissionMirroringRequired() throws Exception
	{
		client = getClient(httpUri);
		
		Assert.assertFalse(client.isPermissionMirroringRequired(), 
				"HTTP permission mirroring should not be enabled.");
	}
    
    @DataProvider(name="getInputStreamProvider", parallel=true)
    public Object[][] getInputStreamProvider()
    {
    	return new Object[][] {
                { fileNotFoundUri, true, "get on 494 should throw exception." },
                { httpMissingPasswordBasicUri, true, "getInputStream on url with missing password file should throw exception." },
                { httpInvalidPasswordBasicUri, true, "getInputStream on url with bad password file should throw exception." },
                { httpNoPathUri, true, "getInputStream on dev https url with custom port and no path should throw an exception." },
                { httpEmptyPathUri, true, "getInputStream on dev https url with custom port and empty path should throw an exception." },
                
                { httpUri, false, "getInputStream on non dev http url should not throw an exception." },
//                { httpsUri, false, "getInputStream on non dev https url should not throw an exception." },
                { httpPortUri, false, "getInputStream on dev http url with custom port should not throw an exception." },
                { httpsPortUri, false, "getInputStream on dev https url with custom port should not throw an exception." },
                { httpsQueryUri, false, "getInputStream on dev https url with custom port and query parameters should not throw an exception." },
                { httpBasicUri, false, "getInputStream on non test http url with custom port and basic auth should not throw an exception." },
        };
    }

    @Test(dataProvider="getInputStreamProvider", dependsOnMethods = { "isPermissionMirroringRequired" })
    public void getInputStream(URI uri, boolean shouldThrowException, String message)
    throws IOException, RemoteDataException
    {
    	boolean actuallyThrewException = false;
        InputStream in = null;
        Path downloadFilePath = null;
		RemoteDataClient client = null;
		Path tmpDirPath = null;
		try
        {
        	client = getClient(uri);
        	
        	in = client.getInputStream(uri.getPath(), true);
            
        	Assert.assertFalse(shouldThrowException,
					"Opening an input stream should have thrown an exception for " + uri.toString());

        	// create temp dir for the test
			tmpDirPath = Files.createTempDirectory("getInputStream");

			// find a file path to write to
			downloadFilePath = tmpDirPath.resolve(UUID.randomUUID().toString());

			// open stream to the file
			Files.copy(in, downloadFilePath);

            Assert.assertTrue(Files.exists(downloadFilePath),
					"Downloaded file should be present at " + downloadFilePath.toString() +
							", but was not found.");

            Assert.assertTrue(Files.size(downloadFilePath) > 0, "Download file should not be empty.");
        }
        catch (Exception e) {
            actuallyThrewException = true;
            if (!shouldThrowException) e.printStackTrace();
        }
        finally {
        	try { if (in != null) in.close(); } catch (Exception ignore) {}
            try { if (downloadFilePath != null) FileUtils.deleteQuietly(downloadFilePath.toFile()); } catch(Exception ignore) {}
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }
    
    @Test(expectedExceptions = RemoteDataException.class)//(dependsOnMethods = { "getInputStream" })
    public void getInputStreamThrowsExceptionWhenNoPermission() throws RemoteDataException {
		Path downloadFilePath = null;
		RemoteDataClient client = null;
		InputStream in = null;
    	try 
    	{
    		client = getClient(httpInvalidPasswordBasicUri);
        	
        	in = client.getInputStream(httpInvalidPasswordBasicUri.getPath(), true);
    		Assert.fail("getInputStream should throw RemoteDataException on no permissions");
    	} 
    	catch (RemoteDataException e) {
    		throw e;
        }
    	catch (Exception e) {
    		Assert.fail("getInputStream should throw RemoteDataException on no permissions");
    	}
		finally {
			try { if (in != null) in.close(); } catch (Exception ignore) {}
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
		}
    }
    
	@DataProvider(name="getFileRetrievesToCorrectLocationProvider", parallel=true)
    public Object[][] getFileRetrievesToCorrectLocationProvider() throws IOException {
		ArrayList<Object[]> testCases = new ArrayList<Object[]>();

		Path localDownloadPath = Files.createTempDirectory("getFileRetrievesToNewFileInCorrectLocationProvider");
		Path expectedDownloadPath = localDownloadPath.resolve(LOCAL_BINARY_FILE_NAME);
		testCases.add(new Object[]{
				localDownloadPath,
				expectedDownloadPath,
				"Downloading to existing path creates new file in path."
		});

		localDownloadPath = Files.createTempDirectory("getFileRetrievesToExactNameInCorrectLocationProvider")
				.resolve(LOCAL_BINARY_FILE_NAME);
		expectedDownloadPath = localDownloadPath;
		testCases.add(new Object[]{
				localDownloadPath,
				expectedDownloadPath,
				"Downloading to explicit file path where no file exists creates the file."
		});

		return testCases.toArray(new Object[][]{});
	}
    
    @Test(dataProvider="getFileRetrievesToCorrectLocationProvider")//, dependsOnMethods = { "getInputStreamThrowsExceptionWhenNoPermission" })
	public void getFileRetrievesToCorrectLocation(Path localPath, Path expectedDownloadPath, String message)
	{
		RemoteDataClient client = null;
		try
    	{
			client = getClient(httpPortUri);

			client.get(httpPortUri.getPath(), localPath.toString());

			// Ensure local path is present
			Assert.assertTrue(Files.exists(expectedDownloadPath), message);
		}
		catch (Exception e) {
			Assert.fail("get should not throw unexpected exception", e);
		}
		finally {
			try { if (localPath != null) FileUtils.deleteQuietly(localPath.toFile()); } catch(Throwable ignore) {}
			try { if (expectedDownloadPath != null) FileUtils.deleteQuietly(expectedDownloadPath.toFile()); } catch(Throwable ignore) {}
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
		}
	}
    
    @Test()//(dependsOnMethods={"getFileRetrievesToCorrectLocation"})
    public void getFileOverwritesExistingFile() 
	{
		Path tmpFile = null;
		RemoteDataClient client = null;
		try
		{
			// create the file so it's present to be overwritten without endangering our test data
			tmpFile = Files.createTempFile("_getFileOverwritesExistingFile", "original");
			Path downloadFilePath = Files.write(tmpFile, "_getFileOverwritesExistingFile".getBytes());

			// get the orignial length to check against the remote length
			long originalLength = Files.size(downloadFilePath);

    		client = getClient(httpPortUri);

    		client.get(httpPortUri.getPath(), downloadFilePath.toString());

			Assert.assertTrue(
					downloadFilePath.toFile().exists(),
					"Getting remote file should overwrite local file if it exists.");

			Assert.assertNotEquals(Files.size(downloadFilePath), originalLength,
					"File length after download should not equal length before download.");
    	} 
    	catch (Exception e) {
        	Assert.fail("Overwriting local file on get should not throw unexpected exception", e);
        }
		finally {
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
			try { if (tmpFile != null) FileUtils.deleteQuietly(tmpFile.toFile()); } catch(Throwable ignore) {}
		}
	}

	@Test
	public void doesExistReturnsTrueFor200StatusCode() {
		RemoteDataClient client = null;
		try
		{
			String remotePath = "/status/200";
			client = getClient(URI.create("http://httpbin:8000" + remotePath));
			boolean result = client.doesExist(remotePath);
			Assert.assertTrue(result, "doesExist should return true on 200 responses");
		}
		catch (Exception e) {
			Assert.fail("Getting remote folder to a local directory that does not exist should throw FileNotFoundException.", e);
		}
		finally {
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
		}
	}

	@DataProvider
	public Object[][] doesExistThrowsExceptionOnAmbiguouStatusCodeProvider() {
		return new Object[][]{
				{HttpStatus.SC_CONTINUE, "doesExist should throw IOException when HTTP response code is 100"},
				{HttpStatus.SC_SWITCHING_PROTOCOLS, "doesExist should throw IOException when HTTP response code is 101"},
				{HttpStatus.SC_PROCESSING, "doesExist should throw IOException when HTTP response code is 102"},
		};
	}

	/**
	 * checks exception throws from {@link RemoteDataClient#doesExist(String)} when a 101-103 response code is returned.
	 * Our dev server does not support those codes, so we disale this test for the moment until we can mock out the call
	 * within the function.
	 * @param statusCode the code to return from the remote requet
	 * @param message the assertion message upon failure
	 * @throws IOException the expected exception for this test
	 * @throws Exception if anything else goes wrong
	 */
	@Test(dataProvider = "doesExistThrowsExceptionOnAmbiguouStatusCodeProvider", expectedExceptions = IOException.class, enabled = false)
	public void doesExistThrowsExceptionOnAmbiguouStatusCode(int statusCode, String message) throws IOException {
		try {
//			CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
//			Mockito.when(response.getStatusLine()).thenReturn(new BasicStatusLine(new ProtocolVersion("http",1,2),statusCode, "This is a test"));
//			RemoteDataClient client = Mockito.mock(HTTP.class);
//			Mockito.when(client.doRequest(Mockito.any(), Mockito.any())).thenReturn(response);

			_doesExist(statusCode, false, message);
		} catch (Exception e) {
			Assert.fail(message, e);
		}
	}

	@DataProvider
	public Object[][] doesExistReturnsTrueForSuccessStatusCodeProvider() {
		return new Object[][]{
				{HttpStatus.SC_OK, "doesExist should return true when HTTP response code is 200"},
				{HttpStatus.SC_CREATED, "doesExist should return true when HTTP response code is 201"},
				{HttpStatus.SC_ACCEPTED, "doesExist should return true when HTTP response code is 202"},
				{HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, "doesExist should return true when HTTP response code is 203"},
				{HttpStatus.SC_NO_CONTENT, "doesExist should return true when HTTP response code is 204"},
				{HttpStatus.SC_RESET_CONTENT, "doesExist should return true when HTTP response code is 205"},
				{HttpStatus.SC_PARTIAL_CONTENT, "doesExist should return true when HTTP response code is 206"},
				{HttpStatus.SC_MULTI_STATUS, "doesExist should return true when HTTP response code is 207"},
				{HttpStatus.SC_MULTIPLE_CHOICES, "doesExist should return true when HTTP response code is 300"},
				{HttpStatus.SC_MOVED_PERMANENTLY, "doesExist should return true when HTTP response code is 301"},
				{HttpStatus.SC_MOVED_TEMPORARILY, "doesExist should return true when HTTP response code is 302"},
				{HttpStatus.SC_SEE_OTHER, "doesExist should return true when HTTP response code is 303"},
				{HttpStatus.SC_NOT_MODIFIED, "doesExist should return true when HTTP response code is 304"},
				{HttpStatus.SC_USE_PROXY, "doesExist should return true when HTTP response code is 305"},
				{HttpStatus.SC_TEMPORARY_REDIRECT, "doesExist should return true when HTTP response code is 307"},
		};
	}

	@Test(dataProvider = "doesExistReturnsTrueForSuccessStatusCodeProvider")
	public void doesExistReturnsTrueForSuccessStatusCode(int statusCode, String message) throws Exception {
		_doesExist(statusCode, true, message);
	}

	@DataProvider
	public Object[][] doesExistReturnsFalseForRelevantStatusCodeProvider() {
		return new Object[][]{
			{ HttpStatus.SC_BAD_REQUEST, "doesExist should return false when HTTP response code is 400" },
			{ HttpStatus.SC_UNAUTHORIZED, "doesExist should return false when HTTP response code is 401" },
			{ HttpStatus.SC_PAYMENT_REQUIRED, "doesExist should return false when HTTP response code is 402" },
			{ HttpStatus.SC_FORBIDDEN, "doesExist should return false when HTTP response code is 403" },
			{ HttpStatus.SC_NOT_FOUND, "doesExist should return false when HTTP response code is 404" },
			{ HttpStatus.SC_METHOD_NOT_ALLOWED, "doesExist should return false when HTTP response code is 405" },
			{ HttpStatus.SC_NOT_ACCEPTABLE, "doesExist should return false when HTTP response code is 406" },
			{ HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED, "doesExist should return false when HTTP response code is 407" },
			{ HttpStatus.SC_REQUEST_TIMEOUT, "doesExist should return false when HTTP response code is 408" },
			{ HttpStatus.SC_CONFLICT, "doesExist should return false when HTTP response code is 409" },
			{ HttpStatus.SC_GONE, "doesExist should return false when HTTP response code is 410" },
			{ HttpStatus.SC_LENGTH_REQUIRED, "doesExist should return false when HTTP response code is 411" },
			{ HttpStatus.SC_PRECONDITION_FAILED, "doesExist should return false when HTTP response code is 412" },
			{ HttpStatus.SC_REQUEST_TOO_LONG, "doesExist should return false when HTTP response code is 413" },
			{ HttpStatus.SC_REQUEST_URI_TOO_LONG, "doesExist should return false when HTTP response code is 414" },
			{ HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE, "doesExist should return false when HTTP response code is 415" },
			{ HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "doesExist should return false when HTTP response code is 416" },
			{ HttpStatus.SC_EXPECTATION_FAILED, "doesExist should return false when HTTP response code is 417" },
			{ HttpStatus.SC_INSUFFICIENT_SPACE_ON_RESOURCE, "doesExist should return false when HTTP response code is 419" },
			{ HttpStatus.SC_METHOD_FAILURE, "doesExist should return false when HTTP response code is 420" },
			{ HttpStatus.SC_UNPROCESSABLE_ENTITY, "doesExist should return false when HTTP response code is 422" },
			{ HttpStatus.SC_LOCKED, "doesExist should return false when HTTP response code is 423" },
			{ HttpStatus.SC_FAILED_DEPENDENCY, "doesExist should return false when HTTP response code is 424" },
			{ HttpStatus.SC_INTERNAL_SERVER_ERROR, "doesExist should return false when HTTP response code is 500" },
			{ HttpStatus.SC_NOT_IMPLEMENTED, "doesExist should return false when HTTP response code is 501" },
			{ HttpStatus.SC_BAD_GATEWAY, "doesExist should return false when HTTP response code is 502" },
			{ HttpStatus.SC_SERVICE_UNAVAILABLE, "doesExist should return false when HTTP response code is 503" },
			{ HttpStatus.SC_GATEWAY_TIMEOUT, "doesExist should return false when HTTP response code is 504" },
			{ HttpStatus.SC_HTTP_VERSION_NOT_SUPPORTED, "doesExist should return false when HTTP response code is 505" },
			{ HttpStatus.SC_INSUFFICIENT_STORAGE, "doesExist should return false when HTTP response code is 507" },
		};
	}

	@Test(dataProvider = "doesExistReturnsFalseForRelevantStatusCodeProvider")
	public void doesExistReturnsFalseForRelevantStatusCode(int statusCode, String message) throws Exception {
		_doesExist(statusCode, false, message);
	}

	/**
	 * Handles execution of the {@link RemoteDataClient#doesExist(String)} method for success and failure tests
	 * @param statusCode the http code to simulate from the url request
	 * @param shouldSucceed whether the assertion should be a true or false response
	 * @param message message given to the assertion
	 * @throws Exception if remote path returns unexpected response code
	 */
	protected void _doesExist(int statusCode, boolean shouldSucceed, String message) throws Exception {
		RemoteDataClient client = getClient(URI.create("http://httpbin:8000/status/" + statusCode));
		boolean result = client.doesExist("status/" + statusCode);
		Assert.assertEquals(result, shouldSucceed, message);
	}
    
    @Test()//(dependsOnMethods = { "getFileOverwritesExistingFile" })
	public void getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath() 
	{
		File missingPath = null;
		RemoteDataClient client = null;
		try
    	{
			// create temp directory for download
			missingPath = new File("/tmp/" + getRemoteTestDirPath() + "/" + MISSING_FILE);

			// delete the tmp directory to ensure it does not exist.
			if (missingPath.exists()) {
				FileUtils.deleteQuietly(missingPath);
			}
        	
    		client = getClient(httpPortUri);
    		client.get(httpPortUri.getPath(), missingPath.getAbsolutePath());
    		Assert.fail("Getting remote file to a local directory that does not exist should throw FileNotFoundException.");
    	} 
    	catch (FileNotFoundException e) {
    		Assert.assertTrue(true);
    	}
    	catch (Exception e) {
    		Assert.fail("Getting remote folder to a local directory that does not exist should throw FileNotFoundException.", e);
    	}
    	finally {
			try { if (client != null) client.disconnect(); } catch (Exception ignore) {}
			try { FileUtils.deleteQuietly(missingPath); } catch(Exception ignore) {}
		}
	}
    
    @Test()//(dependsOnMethods = { "getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath" })
	public void getDownloadedContentMatchesOriginalContent() 
	{
    	InputStream originalIn = null, downloadIn = null;
    	Path tmpTestDir = null;
		Path testDownloadFile = null;
		Path localSourceFilePath = Paths.get(LOCAL_BINARY_FILE);
		RemoteDataClient client = null;
    	try 
    	{
			tmpTestDir = Files.createTempDirectory("getDownloadedContentMatchesOriginalContent");
			testDownloadFile = tmpTestDir.resolve(UUID.randomUUID().toString());

			long originalFileSize = Files.size(localSourceFilePath);

			client = getClient(httpsPortUri);
    		
    		client.get(httpsPortUri.getPath(), testDownloadFile.toString());
    		
    		Assert.assertEquals(Files.size(testDownloadFile), originalFileSize,
    				"Local file size is not the same as the http client reported length.");

    		originalIn = Files.newInputStream(localSourceFilePath);
    		downloadIn = Files.newInputStream(testDownloadFile);
    		
    		Assert.assertTrue(IOUtils.contentEquals(downloadIn, originalIn),
    				"File contents were not the same after download as before.");
    		
    	} 
    	catch (Exception e) {
    		Assert.fail("Fetching known file should not throw exception.", e);
    	}
    	finally {
    		try { if (downloadIn != null) downloadIn.close(); } catch (Exception ignore) {}
    		try { if (originalIn != null) originalIn.close(); } catch (Exception ignore) {}
    		try { if (tmpTestDir != null) FileUtils.deleteQuietly(tmpTestDir.toFile()); } catch(Exception ignore) {}
    	}
	}

	@Test
	public void getLengthReturnsValueWhenAvailable() {
		try {
			int testLength = 621;
			String remotePath = String.format("/bytes/%d",testLength);
			RemoteDataClient client = getClient(URI.create("http://httpbin:8000"+remotePath));
			long length = client.length(remotePath);
			Assert.assertEquals(length, testLength,
					"Present Content-Length in header should return that length");
		}
		catch(Exception e) {
			Assert.fail("Valid http response should not throw exception on response", e);
		}
	}

	@Test
	public void getLengthReturnsNegativeOneWhenNotAvaialble() {
		try {
			RemoteDataClient client = getClient(URI.create("http://httpbin:8000/stream/1024"));
			long length = client.length("/stream/1024");
			Assert.assertEquals(length, -1,
					"Missing Content-Length in header should result in a -1 length");
		}
		catch(Exception e) {
			Assert.fail("Valid http response should not throw exception on response", e);
		}
	}

	@Test(expectedExceptions = IOException.class)
	public void getLengthThrowsFileNotFoundExceptionOn404() throws IOException {
		try {
			RemoteDataClient client = getClient(URI.create("http://httpbin:8000/status/404"));
			long length = client.length("status/404");
		}
		catch(IOException e) {
			throw e;
		}
		catch(Exception e) {
			Assert.fail("404 response should return IOException", e);
		}
	}

	@Test
	public void getFileInfoReturnsFileInfo() throws Exception{
		client = getClient(httpUri);

		try {
			RemoteFileInfo info = client.getFileInfo(httpUri.getPath());

			Assert.assertEquals(info.getName(), FilenameUtils.getName(httpUri.getPath()),
					"RemoteFileInfo should return the final element of the path as the name.");

			Assert.assertEquals(info.getSize(), -1,
					"RemoteFileInfo should return size of the file when available.");

			Assert.assertEquals(info.getOwner(), Settings.WORLD_USER_USERNAME,
					"RemoteFileInfo should return the final element of the path as the name.");

			Assert.assertTrue(info.isFile(),
					"RemoteFileInfo should indicate the item is a file.");

			Assert.assertFalse(info.isDirectory(),
					"RemoteFileInfo should indicate the item is not a directory.");
		}
		catch (RemoteDataException e) {
			Assert.fail("getFileInfo should swallow RemoteDataExceptions", e);
		}
		catch (IOException e) {
			Assert.fail("getFileInfo should not throw IOExceptions on valid URL", e);
		}
	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void getFileInfoReturnsErrorOn404() throws Exception {

		client = getClient(fileNotFoundUri);

		try {
			client.getFileInfo(fileNotFoundUri.getPath());
		}
		catch (RemoteDataException e) {
			Assert.fail("getFileInfo should swallow RemoteDataExceptions", e);
		}
	}
    
    
}

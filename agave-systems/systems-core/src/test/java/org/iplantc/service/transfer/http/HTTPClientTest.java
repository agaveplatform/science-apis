package org.iplantc.service.transfer.http;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.iplantc.service.transfer.BaseTransferTestCase;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
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

    	LOCAL_BINARY_FILE = "src/test/resources/transfer/test_upload.bin";
		LOCAL_BINARY_FILE_NAME = FilenameUtils.getName(LOCAL_BINARY_FILE);

		LOCAL_TXT_FILE = "src/test/resources/transfer/test_upload.txt";
		LOCAL_TXT_FILE_NAME = FilenameUtils.getName(LOCAL_TXT_FILE);

		httpUri = new URI("http://httpbin.agaveplatform.org/stream-bytes/32768");
    	httpsUri = new URI("https://httpbin.agaveplatform.org/stream-bytes/32768");
    	httpPortUri = new URI("http://httpd:10080/public/test_upload.bin");
    	httpsPortUri = new URI("https://httpd:10443/public/test_upload.bin");
    	httpsQueryUri = new URI("https://httpd:10443/public/test_upload.bin?t=now");
    	httpNoPathUri = new URI("http://httpd:10080");
    	httpEmptyPathUri = new URI("http://httpd:10080/");
    	httpBasicUri = new URI("http://testuser:testuser@httpd:10080/private/test_upload.bin");

    	fileNotFoundUri = new URI("http://httpd:10080/" + MISSING_FILE);
    	httpMissingPasswordBasicUri = new URI("http://testuser@httpd:10080/private/test_upload.bin");
    	httpInvalidPasswordBasicUri = new URI("http://testuser:testotheruser@httpd:10080/private/test_upload.bin");
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
    	String downloadFile = new File(LOCAL_DOWNLOAD_DIR, FilenameUtils.getName(LOCAL_BINARY_FILE)).getPath();
        return new Object[][] {
                { downloadFile, fileNotFoundUri, true, "get on 494 should throw exception." },
                { downloadFile, httpMissingPasswordBasicUri, true, "getInputStream on url with missing password file should throw exception." },
                { downloadFile, httpInvalidPasswordBasicUri, true, "getInputStream on url with bad password file should throw exception." },
                { downloadFile, httpNoPathUri, true, "getInputStream on dev https url with custom port and no path should throw an exception." },
                { downloadFile, httpEmptyPathUri, true, "getInputStream on dev https url with custom port and empty path should throw an exception." },
                
                { downloadFile, httpUri, false, "getInputStream on non dev http url should not throw an exception." },
                { downloadFile, httpsUri, false, "getInputStream on non dev https url should not throw an exception." },
                { downloadFile, httpPortUri, false, "getInputStream on dev http url with custom port should not throw an exception." },
                { downloadFile, httpsPortUri, false, "getInputStream on dev https url with custom port should not throw an exception." },
                { downloadFile, httpsQueryUri, false, "getInputStream on dev https url with custom port and query parameters should not throw an exception." },
                { downloadFile, httpBasicUri, false, "getInputStream on non test http url with custom port and basic auth should not throw an exception." },
        };
    }

    @Test(dataProvider="getInputStreamProvider", dependsOnMethods = { "isPermissionMirroringRequired" })
    public void getInputStream(String localFile, URI uri, boolean shouldThrowException, String message)
    throws IOException, RemoteDataException
    {
    	boolean actuallyThrewException = false;
        InputStream in = null;
        BufferedOutputStream bout = null;
        Path downloadFilePath = null;
		RemoteDataClient client = null;
		try
        {
        	client = getClient(uri);
        	
        	in = client.getInputStream(uri.getPath(), true);
            
        	Assert.assertEquals(false, shouldThrowException,
					"Opening an input stream should have thrown an exception for " + uri.toString());

			downloadFilePath = Paths.get(localFile);
			// ensure directories are present
			Files.createDirectories(downloadFilePath.getParent());
            
            bout = new BufferedOutputStream(Files.newOutputStream(downloadFilePath));

            int bufferSize = client.getMaxBufferSize();
            byte[] b = new byte[bufferSize];
            int len = 0;

            while ((len = in.read(b)) > -1) {
                bout.write(b, 0, len);
            }

            bout.flush();
            
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
        	try { in.close(); } catch (Exception ignore) {}
            try { bout.close(); } catch (Exception ignore) {}
			try { FileUtils.deleteQuietly(downloadFilePath.toFile()); } catch(Exception ignore) {}
			try { client.disconnect(); } catch (Exception ignore) {}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }
    
    @Test()//(dependsOnMethods = { "getInputStream" })
    public void getInputStreamThrowsExceptionWhenNoPermission()
    {
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
    		Assert.assertTrue(true);
        }
    	catch (Exception e) {
    		Assert.fail("getInputStream should throw RemoteDataException on no permissions");
    	}
		finally {
			try { in.close(); } catch (Exception ignore) {}
			try { FileUtils.deleteQuietly(downloadFilePath.toFile()); } catch(Exception ignore) {}
			try { client.disconnect(); } catch (Exception ignore) {}
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

			client.get(httpPortUri.getPath(), localPath.toAbsolutePath().toString());

			// Ensure local path is present
			Assert.assertTrue(Files.exists(expectedDownloadPath), message);
		}
		catch (Exception e) {
			Assert.fail("get should not throw unexpected exception", e);
		}
		finally {
			try { if (localPath != null) FileUtils.deleteQuietly(localPath.toFile()); } catch(Throwable ignore) {}
			try { if (expectedDownloadPath != null) FileUtils.deleteQuietly(expectedDownloadPath.toFile()); } catch(Throwable ignore) {}
			try { client.disconnect(); } catch (Exception ignore) {}
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
			long originalLength = downloadFilePath.toFile().length();

    		client = getClient(httpPortUri);

    		client.get(httpPortUri.getPath(), downloadFilePath.toAbsolutePath().toString());

			Assert.assertTrue(
					downloadFilePath.toFile().exists(),
					"Getting remote file should overwrite local file if it exists.");

			Assert.assertNotEquals(downloadFilePath.toFile().length(), originalLength,
					"File length after download should not equal length before download.");
    	} 
    	catch (Exception e) {
        	Assert.fail("Overwriting local file on get should not throw unexpected exception", e);
        }
		finally {
			try { client.disconnect(); } catch (Exception ignore) {}
			try { if (tmpFile != null) FileUtils.deleteQuietly(tmpFile.toFile()); } catch(Throwable ignore) {}
		}
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
			try { client.disconnect(); } catch (Exception ignore) {}
			try { FileUtils.deleteQuietly(missingPath); } catch(Exception ignore) {}
		}
	}
    
    @Test()//(dependsOnMethods = { "getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath" })
	public void getDownloadedContentMatchesOriginalContent() 
	{
    	InputStream originalIn = null, downloadIn = null;
    	Path downloadDir = null;
		Path downloadFile = null;
		Path localSourceFilePath = Paths.get(LOCAL_BINARY_FILE);
		RemoteDataClient client = null;
    	try 
    	{
			downloadDir = Files.createTempDirectory("getDownloadedContentMatchesOriginalContent");
			downloadFile = downloadDir.resolve(LOCAL_BINARY_FILE_NAME);

			long originalFileSize = Files.size(localSourceFilePath);

			client = getClient(httpPortUri);
    		
    		client.get(httpPortUri.getPath(), downloadFile.toAbsolutePath().toString());
    		
    		Assert.assertEquals(originalFileSize, Files.size(downloadFile),
    				"Local file size is not the same as the http client reported length.");
    		
    		Assert.assertEquals(originalFileSize, client.length(httpPortUri.getPath()),
    				"Local file size is not the same as the original file length.");
    		
    		originalIn = Files.newInputStream(localSourceFilePath);
    		downloadIn = Files.newInputStream(downloadFile);
    		
    		Assert.assertTrue(IOUtils.contentEquals(originalIn, downloadIn), 
    				"File contents were not the same after download as before.");
    		
    	} 
    	catch (Exception e) {
    		Assert.fail("Fetching known file should not throw exception.", e);
    	}
    	finally {
    		try { downloadIn.close(); } catch (Exception ignore) {}
    		try { originalIn.close(); } catch (Exception ignore) {}
    		try { FileUtils.deleteQuietly(downloadDir.toFile()); } catch(Exception ignore) {}
    	}
	}
    
    
}

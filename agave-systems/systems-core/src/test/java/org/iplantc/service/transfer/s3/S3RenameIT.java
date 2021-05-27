package org.iplantc.service.transfer.s3;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.BaseTransferTestCase;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.TransferTestRetryAnalyzer;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.Instant;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Test(enabled = false, groups={"external","s3","s3.rename"})
public class S3RenameIT extends BaseTransferTestCase {
	private final Logger log = Logger.getLogger(S3RenameIT.class);

	protected ThreadLocal<RemoteDataClient> threadClient = new ThreadLocal<RemoteDataClient>();
	protected RemoteDataClient client = null;
	protected StorageSystem system;

	@BeforeClass
	protected void beforeClass() throws Exception {
		super.beforeClass();

		JSONObject json = getSystemJson();
		json.remove("id");
		json.put("id", this.getClass().getSimpleName());
		system = StorageSystem.fromJSON(json);
		system.setOwner(SYSTEM_USER);
		String homeDir = system.getStorageConfig().getHomeDir();
		homeDir = StringUtils.isEmpty(homeDir) ? "" : homeDir;
		system.getStorageConfig().setHomeDir( homeDir + "/" + getClass().getSimpleName());
		storageConfig = system.getStorageConfig();
		salt = system.getSystemId() + storageConfig.getHost() +
				storageConfig.getDefaultAuthConfig().getUsername();

		SystemDao dao = Mockito.mock(SystemDao.class);
		Mockito.when(dao.findBySystemId(Mockito.anyString()))
				.thenReturn(system);

		getClient().authenticate();
		if (getClient().doesExist("")) {
			try {
				getClient().delete("..");
			} catch (FileNotFoundException ignore) {
				// ignore if already gone
			}
		}
	}

	@AfterClass
	protected void afterClass() throws Exception {
		try
		{
			getClient().authenticate();
			// remove test directory
			getClient().delete("..");
			Assert.assertFalse(getClient().doesExist(""), "Failed to clean up home directory after test.");
		}
		catch (Exception e) {
			log.error("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
		}
		finally {
			try { getClient().disconnect(); } catch (Exception ignored) {}
		}
	}

	/**
	 * Fetches the test system description for the protocol.
	 *
	 * @return
	 * @throws JSONException
	 * @throws IOException
	 */
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "s3.example.com.json");
	}

	/**
	 * Gets getClient() from current thread
	 * @return
	 * @throws RemoteCredentialException
	 * @throws RemoteDataException
	 */
	protected RemoteDataClient getClient()
	{
		RemoteDataClient client;
		try {
			if (threadClient.get() == null) {
				client = system.getRemoteDataClient();
				client.updateSystemRoots(client.getRootDir(), system.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
				threadClient.set(client);
			}
		} catch (RemoteDataException | RemoteCredentialException e) {
			Assert.fail("Failed to get client", e);
		}

		return threadClient.get();
	}


	/**
	 * Generates the prefix or suffixes to use in rename tests.
	 *
	 * @return
	 */
	@DataProvider(name="renameProvider", parallel = true)
	private Object[][] _renamePrefixOrSuffixProvider() {
		String[] filenameDelimiters = { ".", "-", "_" };
		long tokenToAppendAndPrepend = Instant.now().getMillis();

		List<Object[]> testCases = new ArrayList<Object[]>();
		for (String delimiter: filenameDelimiters) {
			String baseFileItemName = UUID.randomUUID().toString();
			testCases.add(new Object[]{ baseFileItemName, baseFileItemName + delimiter, "Renaming with delimiter appended should not fail"});
			baseFileItemName = UUID.randomUUID().toString();

			testCases.add(new Object[]{ baseFileItemName, baseFileItemName + delimiter + tokenToAppendAndPrepend, "Renaming with delimiter and token appended should not fail"});
			baseFileItemName = UUID.randomUUID().toString();

			testCases.add(new Object[]{ baseFileItemName, delimiter + baseFileItemName, "Renaming with delimiter and token prepended should not fail"});
			baseFileItemName = UUID.randomUUID().toString();

			testCases.add(new Object[]{ baseFileItemName, delimiter + tokenToAppendAndPrepend + baseFileItemName, "Renaming with delimiter and token prepended should not fail"});
			baseFileItemName = UUID.randomUUID().toString();

			testCases.add(new Object[]{ baseFileItemName, delimiter + tokenToAppendAndPrepend + baseFileItemName + delimiter + tokenToAppendAndPrepend, "Renaming with delimiter and token appended and prepended should not fail"});
			baseFileItemName = UUID.randomUUID().toString();
		}

		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="renameProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
	public void testRenamedAlphaDirectoryNameWithPrefix(String basename, String prefix, String message)
	throws IOException, RemoteDataException
	{
		_testRenamedDirectory(basename, prefix, message);
	}

	@Test(dataProvider="renameProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
	public void testRenamedFileWithPrefix(String basename, String prefix, String message)
	throws IOException, RemoteDataException
	{
		_testRenamedFile(basename, prefix, message);
	}

	/**
	 * Generates a test file with the given {@code basename} on the remote system
	 * and performs the rename test.
	 *
	 * @param originalFilename
	 * @param renamedFilename
	 * @param message
	 * @throws IOException
	 * @throws RemoteDataException
	 */
	protected void _testRenamedFile(String originalFilename, String renamedFilename, String message)
	throws IOException, RemoteDataException
	{
		Path tmpFile = null;
		String remoteBaseUrl = UUID.randomUUID() + "/";
		try
		{
			tmpFile = java.nio.file.Files.createTempFile(null, null);

			// create the file so it's present to be overwritten without endangering our test data
			Path downloadFilePath = Files.write(
					tmpFile, "_testRenamedFile".getBytes());

			// get the orignial length to check against the remote length
			long originalLength = tmpFile.toFile().length();

			// create the remote directory for testing
			getClient().mkdirs(remoteBaseUrl);

			String relativeOriginalFilename = remoteBaseUrl + originalFilename;
			String relativeRenamedFilename = remoteBaseUrl + renamedFilename;

			getClient().put(tmpFile.toAbsolutePath().toString(), relativeOriginalFilename);
			getClient().doRename(relativeOriginalFilename, relativeRenamedFilename);

			Assert.assertFalse(getClient().doesExist(relativeOriginalFilename),
					"Original file " + relativeOriginalFilename + " should not not present on the remote " +
							"system after a rename operation");
			Assert.assertTrue(getClient().doesExist(relativeRenamedFilename),
					"Renamed file " + relativeRenamedFilename + " should be present on the remote system " +
							"after a rename operation");
		}
		catch (Exception e) {
			Assert.fail(message, e);
		}
		finally {
			try {Files.deleteIfExists(tmpFile); } catch (Exception ignore) {}
			try { getClient().delete(remoteBaseUrl);} catch (Exception ignore){}
		}
	}

	/**
	 * Generates a test folder with the given {@code basename} on the remote system
	 * and performs the rename test.
	 *
	 * @param originalDirName
	 * @param renamedDirName
	 * @param message
	 * @throws IOException
	 * @throws RemoteDataException
	 */
	protected void _testRenamedDirectory(String originalDirName, String renamedDirName, String message)
	throws IOException, RemoteDataException
	{
		String remoteBaseUrl = UUID.randomUUID() + "/";

		try {
			String relativeOriginalDirname = remoteBaseUrl + originalDirName;
			String relativeRenamedDirname = remoteBaseUrl + renamedDirName;

			// create the remote directory for testing
			getClient().mkdirs(relativeOriginalDirname);
			getClient().doRename(relativeOriginalDirname, relativeRenamedDirname);

			Assert.assertFalse(getClient().doesExist(relativeOriginalDirname),
					"Original directory " + relativeOriginalDirname + " should not not present on the " +
							"remote system after a rename operation");
			Assert.assertTrue(getClient().doesExist(relativeRenamedDirname),
					"Renamed directory " + relativeRenamedDirname + " should be present on the " +
							"remote system after a rename operation");
		}
		catch (Exception e) {
			Assert.fail(message, e);
		}
		finally {
			try { getClient().delete(remoteBaseUrl);} catch (Exception ignore){}
		}
	}
}



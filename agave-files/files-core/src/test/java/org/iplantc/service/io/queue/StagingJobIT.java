package org.iplantc.service.io.queue;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.dao.QueueTaskDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.io.queue.listeners.FilesTransferListener;
import org.iplantc.service.io.util.ServiceUtils;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.dao.SystemRoleDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests the behavior of the StagingJob. URL Copy should be tested independently
 */
@Test(singleThreaded = true, enabled = false, groups = {"integration"})
public class StagingJobIT extends BaseTestCase {
	private static final Logger log = Logger.getLogger(StagingJobIT.class);

	//	private StagingJob stagingJob;
//	private LogicalFile file;
//	private StagingTask task;
//	private RemoteDataClient remoteClient;
	private StorageSystem defaultStorageSystem;
	private StorageSystem sftpSystem;
	private StorageSystem gridftpSystem;
	private StorageSystem irodsSystem;
	private StorageSystem irods4System;
	private StorageSystem ftpSystem;
	private StorageSystem s3System;
	private final ThreadLocal<HashMap<String, RemoteDataClient>> threadClientMap = new ThreadLocal<HashMap<String, RemoteDataClient>>();
	private final ThreadLocal<String> uniqueSrcTestDir = new ThreadLocal<String>();
	private final ThreadLocal<String> uniqueDestTestDir = new ThreadLocal<String>();
	private final SystemDao systemDao = new SystemDao();
	private List<StorageSystem> testSystems;
	private FilesTransferListener listener;
	private final String TRANSFER_API_URL = "http://localhost:8085/api/transfers";

	@BeforeClass
	@Override
	protected void beforeClass() throws Exception {
		super.beforeClass();
		// init the remote data client map in each thread
		threadClientMap.set(new HashMap<String, RemoteDataClient>());

		defaultStorageSystem = initSystem("sftp", true, true);
		sftpSystem = initSystem("sftp", false, false);
		gridftpSystem = initSystem("gridftp", false, false);
		ftpSystem = initSystem("ftp", false, false);
		s3System = initSystem("s3", false, false);
		irodsSystem = initSystem("irods", false, false);
		irods4System = initSystem("irods4", false, false);

		testSystems = Arrays.asList(sftpSystem, irods4System);//defaultStorageSystem, sftpSystem, s3System, irodsSystem, );

		initFilesTransferListener();
	}

	private StorageSystem initSystem(String protocol, boolean setDefault, boolean setPublic)
			throws Exception
	{
		String systemId = String.format("%s%s%s", protocol, setPublic ? "-public" : "", setDefault ? "-default" : "");
		StorageSystem system = (StorageSystem) getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, StringUtils.lowerCase(protocol), systemId);
		system.setPubliclyAvailable(setPublic);
		system.setGlobalDefault(setDefault);
		system.setOwner(SYSTEM_OWNER);
		String testHomeDir = (StringUtils.isEmpty(system.getStorageConfig().getHomeDir()) ? "" : system.getStorageConfig().getHomeDir());
		system.getStorageConfig().setHomeDir(testHomeDir + "/" + this.getClass().getSimpleName());
		systemDao.persist(system);
		SystemRoleDao.persist(new SystemRole(SYSTEM_OWNER, RoleType.ADMIN, system));
		SystemRoleDao.persist(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN, system));
		SystemRoleDao.persist(new SystemRole(SYSTEM_UNSHARED_USER, RoleType.ADMIN, system));
//		system.addRole(new SystemRole(system.getStorageConfig().getDefaultAuthConfig().getUsername(), RoleType.ADMIN));
		SystemRoleDao.persist(new SystemRole(Settings.COMMUNITY_USERNAME, RoleType.ADMIN, system));
//		systemDao.persist(system);
		return system;
	}

	private void initFilesTransferListener() {
		FilesTransferListener listener = new FilesTransferListener();
		ExecutorService executorService = Executors.newCachedThreadPool();
		executorService.execute(listener);
	}

	@AfterClass
	protected void afterClass() throws Exception
	{
		clearSystems();
		clearLogicalFiles();

		for (RemoteSystem system : testSystems) {
			RemoteDataClient client = null;
			try {
				client = getClient(system);
				client.authenticate();
				client.delete("..");
			}
			catch (FileNotFoundException ignored) {
				// nothing to delete. we're good
			}
			catch (Exception e) {
				if (client != null) {
					log.error("Failed to clean up test home directory " +
							client.resolvePath("") + " after test method.", e);
				}
				else {
					log.error("Unable to create remote data client for " + system.getSystemId(), e);
				}
			}
			finally {
				try { if(client != null) client.disconnect(); } catch (Exception ignored){}
			}
		}
	}

	@BeforeMethod
	protected void beforeMethod(Method m, Object[] args) throws Exception
	{
		String resolvedHomeDir = "";
		try {
			// auth client and ensure test directory is present
			for (int i = 0; i < args.length; i++) {
				if (args[i] == RemoteSystem.class) {
					RemoteSystem system = (RemoteSystem) args[i];
					RemoteDataClient client = getClient(system);
					client.mkdirs("");
//					if (m.getParameters()[i].getName().equalsIgnoreCase("sourceSystem")) {
//						uniqueSrcTestDir.set(String.format("%s/%s", m.getName(), UUID.randomUUID().toString()));
//					} else if (m.getParameters()[i].getName().equalsIgnoreCase("destSystem")) {
//						uniqueDestTestDir.set(String.format("%s/%s", m.getName(), UUID.randomUUID().toString()));
//					}
				}
			}
		} catch (IOException | RemoteDataException | AssertionError e) {
			throw e;
		} catch (Exception e) {
			Assert.fail("Failed to create home directory " + resolvedHomeDir + " before test method.", e);
		}
	}

	@AfterMethod
	protected void afterMethod() throws Exception {

	}

	/**
	 * Gets a {@link RemoteDataClient} with a threadsafe home directory for the given system in the current thread.
	 * We use this method to save the overhead of creating the client and authenticating to the remote service every
	 * time.
	 * <p>Note that unique test case directories are not created here. Those need to be created and cleaned as part of
	 * each test case setup and teardown.</p>
	 * @return a remote data client for the given system for this thread
	 * @throws RemoteCredentialException
	 * @throws RemoteDataException
	 */
	protected RemoteDataClient getClient(RemoteSystem system)
	{
		RemoteDataClient client;
		try {
			if (threadClientMap.get().get(system.getSystemId()) == null) {
				client = system.getRemoteDataClient();
//				String threadHomeDir = String.format("%s/thread-%s-%d",
//						system.getStorageConfig().getHomeDir(),
//						UUID.randomUUID().toString(),
//						Thread.currentThread().getId());
//				client.updateSystemRoots(client.getRootDir(),  threadHomeDir);
				threadClientMap.get().put(system.getSystemId(), client);
			}
		} catch (RemoteDataException | RemoteCredentialException e) {
			Assert.fail("Failed to get client", e);
		}

		return threadClientMap.get().get(system.getSystemId());
	}

	/**
	 * Generates a random directory name for the remote test folder. The
	 * remote folder is not created. Use #createRemoteTestDir() to generate
	 * a test directory on the remote system.
	 *
	 * @see #createRemoteTestDir(RemoteSystem)
	 * @return string representing remote test directory.
	 */
	protected String getRemoteTestDirPath() {
		return UUID.randomUUID().toString();
	}

	/**
	 * Creates a random test directory on the remote system using the
	 * RemoteDataClient#mkdirs method. The path is obtained form a
	 * call to {@link #getRemoteTestDirPath()).
	 *
	 * @see #getRemoteTestDirPath()
	 * @param system the system on which to create the directory
	 * @return path to the test directory on the remote system
	 * @throws IOException
	 * @throws RemoteDataException
	 */
	protected String createRemoteTestDir(RemoteSystem system) throws IOException, RemoteDataException {
		String remoteBaseDir = getRemoteTestDirPath();
		getClient(system).mkdirs(remoteBaseDir);

		return remoteBaseDir;
	}

	private StagingTask createStagingTaskForUrl(URI sourceUri, RemoteSystem destSystem, String destPath)
			throws Exception
	{

		LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
		file.setStatus(StagingTaskStatus.STAGING_QUEUED);
		file.setOwner(SYSTEM_OWNER);

		LogicalFileDao.persist(file);

		new QueueTaskDao().enqueueStagingTask(file, SYSTEM_OWNER);

//		new StagingTransferTask().enqueueStagingTask(file, SYSTEM_OWNER);
//		new QueueTaskDao().enqueueStagingTask(file, SYSTEM_OWNER);

		return null;
	}

	@Test
	public void testStageNextFileQueueEmpty()
	{
		// no task is in the queue
		try {
			StagingJob stagingJob = new StagingJob();
			stagingJob.execute(null);
		} catch (Exception e) {
			Assert.fail("No queued files should return without exception", e);
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextURIProvider(Method m) throws Exception
	{
		URI httpUri = new URI("http://httpbin:8000/stream-bytes/32768");
		URI httpPortUri = new URI("http://httpd:8000/public/test_upload.bin");
		URI httpsPortUri = new URI("https://httpd:8443/public/test_upload.bin");
		URI httpsQueryUri = new URI("https://httpd:8443/public/test_upload.bin?t=now");
		URI httpNoPathUri = new URI("http://httpd:8000");
		URI httpEmptyPathUri = new URI("http://httpd:8000/");
		URI httpBasicUri = new URI("http://testuser:testuser@httpd:8000/private/test_upload.bin");

		URI fileNotFoundUri = new URI("http://httpd:8000/" + MISSING_FILE);
		URI httpMissingPasswordBasicUri = new URI("http://testuser@httpd:8000/private/test_upload.bin");
		URI httpInvalidPasswordBasicUri = new URI("http://testuser:testotheruser@httpd:8000/private/test_upload.bin");

		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem system : testSystems) //,ftpSystem, gridftpSystem))
		{
			for (URI uri: Arrays.asList(httpsQueryUri, httpUri, httpPortUri, httpsPortUri,  httpBasicUri))
			{
				testCases.add(new Object[]{uri,
						system,
						"",
						true,
						FilenameUtils.getName(uri.getPath()),
						String.format("Retrieving valid uri from %s to %s %s %s system should succeed",
								uri,
								system.isGlobalDefault() ? " default " : "",
								system.getStorageConfig().getProtocol().name(),
								system.getType().name())});
			}

			testCases.add(new Object[]{fileNotFoundUri,
					system,
					"",
					false,
					FilenameUtils.getName(fileNotFoundUri.getPath()),
					String.format("Staging 404 at %s to default sotrage system should fail.",
							fileNotFoundUri)});

			testCases.add(new Object[]{httpMissingPasswordBasicUri,
					defaultStorageSystem,
					"",
					false,
					FilenameUtils.getName(httpMissingPasswordBasicUri.getPath()),
					String.format("Missing basic password copying file at %s to default sotrage system should fail.",
							httpMissingPasswordBasicUri)});

			testCases.add(new Object[]{httpInvalidPasswordBasicUri,
					defaultStorageSystem,
					"",
					false,
					FilenameUtils.getName(httpInvalidPasswordBasicUri.getPath()),
					String.format("Bad basic password copying file at %s to default sotrage system should fail.",
							httpInvalidPasswordBasicUri)});

			testCases.add(new Object[]{httpNoPathUri,
					defaultStorageSystem,
					"",
					false,
					FilenameUtils.getName(httpNoPathUri.getPath()),
					String.format("No source path found copying file at %s to default sotrage system should fail.",
							httpNoPathUri)});
			testCases.add(new Object[]{httpEmptyPathUri,
					defaultStorageSystem,
					"",
					false,
					FilenameUtils.getName(httpEmptyPathUri.getPath()),
					String.format("No source path found copying file at %s to default sotrage system should fail.",
							httpEmptyPathUri)});
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider = "testStageNextURIProvider")//, dependsOnMethods={"testStageNextFileQueueEmpty"})
	public void testStageNextURIFromUrlToSystem(URI sourceUri, RemoteSystem destSystem, String destPath, boolean shouldSucceed, String expectedPath, String message)
	{
		RemoteDataClient destClient = null;
		org.iplantc.service.transfer.Settings.ALLOW_RELAY_TRANSFERS = true;
		String remoteBaseDir = null;
		String remoteDestPath = null;
		String remoteExpectedPath = null;

		try
		{
			// create a test directory and adjust expectedPath to show up there.
			remoteBaseDir = createRemoteTestDir(destSystem);
			remoteDestPath = remoteBaseDir;
			if (StringUtils.isNotEmpty(destPath)) {
				remoteDestPath += "/" + destPath;
			}

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			String adjustedRemoteDestPath = ServiceUtils.getAdjustedDestinationPath(destSystem.getRemoteDataClient(), file, SYSTEM_OWNER);

//            remoteExpectedPath = remoteBaseDir + "/" + expectedPath;

			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingTask task = createStagingTaskForUrl(sourceUri, destSystem, remoteDestPath);
//
//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//			stagingJob.doExecute();

			LogicalFile queuedFile = LogicalFileDao.findById(file.getId());
			Assert.assertNotNull(queuedFile, "LogicalFile should exist if TransferTaskScheduler is run");

			destClient = getClient(destSystem);

			if (shouldSucceed) {
				LogicalFile destFile = LogicalFileDao.findBySystemAndPath(destSystem, adjustedRemoteDestPath);

				Assert.assertNotNull(destFile, "Destination file was not created on destination system at " + adjustedRemoteDestPath);

				Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_COMPLETED.name(), message);

				Assert.assertTrue(destClient.doesExist(adjustedRemoteDestPath),
						"Staged file is not present on the remote system.");

				Assert.assertEquals(file.getPath(), destClient.resolvePath(adjustedRemoteDestPath),
						"Expected path," + adjustedRemoteDestPath + ", differed from the relative logical file path of " +
								file.getAgaveRelativePathFromAbsolutePath());

				Assert.assertTrue(destClient.isFile(adjustedRemoteDestPath),
						"Staged file is present, but not a file on the remote system.");
			}
			else
			{
				Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), message);
			}
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			if (destClient != null) {
				try { if (remoteBaseDir != null) destClient.delete(remoteBaseDir); } catch (Exception ignored) {}
			}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextFileFromSystemToSystemProvider(Method m) throws Exception
	{
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

			for (StorageSystem destSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

				if (!srcSystem.equals(destSystem)) {
					// copy file to home directory
					testCases.add(new Object[]{srcSystem, destSystem, "", LOCAL_BINARY_FILE_NAME});
					testCases.add(new Object[]{srcSystem, destSystem, LOCAL_BINARY_FILE_NAME, LOCAL_BINARY_FILE_NAME});
				}
			}
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider = "testStageNextFileFromSystemToSystemProvider")//, dependsOnMethods={"testStageNextURI"})
	public void testStageNextFileFromSystemToSystem(RemoteSystem sourceSystem, RemoteSystem destSystem, String destPath, String expectedPath)
	{
		String remoteSrcBaseDir = null;
		String remoteSrcPath = null;
		String remoteDestBaseDir = null;
		String remoteDestPath = null;
		String remoteExpectedPath = null;
		try
		{
			// create a test src directory to stage source data
			remoteSrcBaseDir = createRemoteTestDir(sourceSystem);
			remoteSrcPath = remoteSrcBaseDir + "/" + LOCAL_BINARY_FILE_NAME;
			getClient(sourceSystem).put(LOCAL_BINARY_FILE, remoteSrcPath);

			// create a test directory and adjust expectedPath to show up there.
			remoteDestBaseDir = createRemoteTestDir(destSystem);
			remoteDestPath = remoteDestBaseDir;
			if (StringUtils.isNotEmpty(destPath)) {
				remoteDestPath += "/" + destPath;
			}
			// adjust the expected path for the remote directory
			remoteExpectedPath = remoteDestBaseDir + "/" + expectedPath;

			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + remoteSrcPath);
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//            stagingJob.doExecute();

			LogicalFile queuedFile = LogicalFileDao.findById(file.getId());

			Assert.assertEquals(queuedFile.getStatus(), StagingTaskStatus.STAGING_COMPLETED.name(),
					"Logical file status was not STAGING_COMPLETED");

			Assert.assertTrue(getClient(destSystem).doesExist(remoteExpectedPath),
					"Staged file is not present on the remote system.");

			Assert.assertEquals(remoteExpectedPath, file.getAgaveRelativePathFromAbsolutePath(),
					"Expected path of " + remoteExpectedPath + " differed from the relative logical file path of " + file.getAgaveRelativePathFromAbsolutePath());

			Assert.assertTrue(getClient(destSystem).isFile(remoteExpectedPath),
					"Staged file is present, but not a file on the remote system.");
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { getClient(sourceSystem).delete(remoteSrcBaseDir); } catch (Exception ignored) {}
			try { getClient(destSystem).delete(remoteDestBaseDir); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveSourceDirectoryToDestFileFailsProvider(Method m) throws Exception
	{
		String localFilename = FilenameUtils.getName(LOCAL_BINARY_FILE);
		String localDirectory = FilenameUtils.getName(LOCAL_DIR);

		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

			for (StorageSystem destSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

				if (!srcSystem.equals(destSystem)) {
					// copy file to home directory
					testCases.add(new Object[]{srcSystem, destSystem});
				}
			}
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="testStageNextAgaveSourceDirectoryToDestFileFailsProvider")//, dependsOnMethods={"testStageNextFile"})
	public void testStageNextAgaveSourceFolderToDestFileFails(RemoteSystem sourceSystem, RemoteSystem destSystem)
	{
		String remoteSrcBaseDir = null;
		String remoteDestBaseDir = null;
		String remoteDestPath = null;
		try
		{
			// create a test src directory to stage source data
			remoteSrcBaseDir = createRemoteTestDir(sourceSystem);

			// create a test directory and adjust expectedPath to show up there.
			remoteDestBaseDir = createRemoteTestDir(destSystem);
			remoteDestPath = remoteDestBaseDir + "/" + LOCAL_BINARY_FILE_NAME;
			getClient(destSystem).put(LOCAL_BINARY_FILE, remoteDestPath);

			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + remoteSrcBaseDir);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(remoteDestPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//			stagingJob.doExecute();

			LogicalFile queuedFile = LogicalFileDao.findById(file.getId());

			Assert.assertEquals(queuedFile.getStatus(), StagingTaskStatus.STAGING_FAILED.name(),
					"Logical file status was not STAGING_FAILED when staging a directory onto a file");
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { getClient(sourceSystem).delete(remoteSrcBaseDir); } catch (Exception ignored) {}
			try { getClient(destSystem).delete(remoteDestBaseDir); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveFolderProvider(Method m) throws Exception
	{
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

			for (StorageSystem destSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

				if (!srcSystem.equals(destSystem)) {
					// copy file to home directory
					testCases.add(new Object[]{srcSystem, destSystem, "", ""});

					// copy file to named file in remote home directory that does not exist
					testCases.add(new Object[]{srcSystem, destSystem, LOCAL_DIR_NAME, LOCAL_DIR_NAME});

				}
			}
		}

		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="testStageNextAgaveFolderProvider")//, dependsOnMethods={"testStageNextAgaveSourceFolderToDestFileFails"})
	public void testStageNextFolder(RemoteSystem sourceSystem, RemoteSystem destSystem, String destPath, String expectedPath)
	{
		org.iplantc.service.transfer.Settings.ALLOW_RELAY_TRANSFERS = true;

		String remoteSrcBaseDir = null;
		String remoteSrcPath = null;
		String remoteDestBaseDir = null;
		String remoteDestPath = null;
		String remoteExpectedPath = null;
		try
		{
			/// create a test src directory to stage source data
			remoteSrcBaseDir = createRemoteTestDir(sourceSystem);
			remoteSrcPath = remoteSrcBaseDir + "/" + LOCAL_DIR_NAME;
			getClient(sourceSystem).put(LOCAL_DIR, remoteSrcPath);

			// create a test directory and adjust expectedPath to show up there.
			remoteDestBaseDir = createRemoteTestDir(destSystem);
			remoteDestPath = remoteDestBaseDir;
			if (StringUtils.isNotEmpty(destPath)) {
				remoteDestPath += "/" + destPath;
			}
			// adjust the expected path for the remote directory
			remoteExpectedPath = remoteDestBaseDir;
			if (StringUtils.isNotEmpty(expectedPath)) {
				remoteExpectedPath += "/" + expectedPath;
			}



			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + remoteSrcPath);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(remoteDestPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//			stagingJob.doExecute();

			LogicalFile queuedFile = LogicalFileDao.findById(file.getId());

			Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_COMPLETED.name(),
					"Logical file status was not STAGING_COMPLETED");

			Assert.assertTrue(getClient(destSystem).doesExist(remoteExpectedPath),
					"Staged directory is not present on the remote system.");

			Assert.assertEquals(file.getAgaveRelativePathFromAbsolutePath(), remoteExpectedPath,
					"Expected path of " + remoteExpectedPath + " differed from the relative logical file path of " + file.getAgaveRelativePathFromAbsolutePath());

			Assert.assertTrue(getClient(destSystem).isDirectory(remoteExpectedPath),
					"Staged directory is present, but not a directory on the remote system.");
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { getClient(sourceSystem).delete(remoteSrcBaseDir); } catch (Exception ignored) {}
			try { getClient(destSystem).delete(remoteDestBaseDir); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveFileSourcePathDoesNotExistProvider(Method m) throws Exception
	{
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

			for (StorageSystem destSystem : testSystems) {//,ftpSystem, gridftpSystem)) {

				if (!srcSystem.equals(destSystem)) {
					// copy file to home directory
					testCases.add(new Object[]{srcSystem, destSystem, MISSING_FILE, "", "Staging should fail when source path file does not exist"});
					testCases.add(new Object[]{srcSystem, destSystem, MISSING_DIRECTORY, "", "Staging should fail when source path dir does not exist"});
				}
			}
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="testStageNextAgaveFileSourcePathDoesNotExistProvider")//, dependsOnMethods={"testStageNextFolder"})
	public void testStageNextAgaveFileSourcePathDoesNotExist(RemoteSystem sourceSystem, RemoteSystem destSystem, String srcPath, String destPath, String message)
	{
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;
		try
		{
			// load the db with dummy records to stage a http-accessible file
			destClient = destSystem.getRemoteDataClient();
			destClient.authenticate();

			if (!destClient.doesExist("")) {
				destClient.mkdirs("");
			}

			srcClient = sourceSystem.getRemoteDataClient();
			srcClient.authenticate();
			if (srcClient.doesExist(srcPath)) {
				srcClient.delete(srcPath);
			}

			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + srcPath);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//            stagingJob.doExecute();

//			LogicalFile queuedFile = task.getLogicalFile();

			Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), message);
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { srcClient.delete(""); } catch (Exception ignored) {}
			try { destClient.delete(""); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveFileDestPathDoesNotExistProvider(Method m) throws Exception
	{
		String localFilename = FilenameUtils.getName(LOCAL_BINARY_FILE);

		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {
			if (!srcSystem.equals(defaultStorageSystem)) {
				// copy file to home directory
				testCases.add(new Object[]{srcSystem, defaultStorageSystem, MISSING_FILE, "", "Staging should fail when dest path file does not exist"});
				testCases.add(new Object[]{srcSystem, defaultStorageSystem, MISSING_DIRECTORY, "", "Staging should fail when dest path dir does not exist"});
			}
		}

		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="testStageNextAgaveFileDestPathDoesNotExistProvider")//, dependsOnMethods={"testStageNextAgaveFileSourcePathDoesNotExist"})
	public void testStageNextAgaveFileDestPathDoesNotExist(RemoteSystem sourceSystem, RemoteSystem destSystem, String srcPath, String destPath, String message)
	{
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;
		try
		{
			// load the db with dummy records to stage a http-accessible file
			destClient = destSystem.getRemoteDataClient();
			destClient.authenticate();

			if (!destClient.doesExist("")) {
				destClient.mkdirs("");
			}

			srcClient = sourceSystem.getRemoteDataClient();
			srcClient.authenticate();
			srcClient.mkdirs("");
			srcClient.put(LOCAL_BINARY_FILE, "");

			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + srcPath);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//            stagingJob.doExecute();
//
//			LogicalFile file = task.getLogicalFile();

			Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), message);
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { srcClient.delete(""); } catch (Exception ignored) {}
			try { destClient.delete(""); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveFileSourceNoPermissionProvider(Method m) throws Exception
	{
		return new Object[][]{
				{gridftpSystem, defaultStorageSystem, "/root", "", "Staging should fail when user does not have permission on source path"},
				{irodsSystem, defaultStorageSystem, "/testotheruser", "", "Staging should fail when user does not have permission on source path"},
				{sftpSystem, defaultStorageSystem, "/root", "", "Staging should fail when user does not have permission on source path"},
				{ftpSystem, defaultStorageSystem, "/root", "", "Staging should fail when user does not have permission on source path"},
				{s3System, defaultStorageSystem, "/", "", "Staging should fail when user does not have permission on source path"},
		};
	}

	@Test(dataProvider="testStageNextAgaveFileSourceNoPermissionProvider")//, dependsOnMethods={"testStageNextAgaveFileDestPathDoesNotExist"})
	public void testStageNextAgaveFileSourceNoPermission(RemoteSystem sourceSystem, RemoteSystem destSystem, String srcPath, String destPath, String message)
	{
		RemoteDataClient destClient = null;
		try
		{
			// load the db with dummy records to stage a http-accessible file
			destClient = destSystem.getRemoteDataClient();
			destClient.authenticate();

			if (!destClient.doesExist("")) {
				destClient.mkdirs("");
			}



			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + srcPath);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//			StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//            stagingJob.doExecute();
//
//			LogicalFile file = task.getLogicalFile();

			Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), message);
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { destClient.delete(""); } catch (Exception ignored) {}
		}
	}

	@DataProvider(parallel = false)
	private Object[][] testStageNextAgaveFileDestNoPermissionProvider(Method m) throws Exception
	{
		String localFilename = FilenameUtils.getName(LOCAL_BINARY_FILE);
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (StorageSystem srcSystem : testSystems) {//,ftpSystem, gridftpSystem)) {
			if (!srcSystem.equals(defaultStorageSystem)) {
				// copy file to home directory
				testCases.add(new Object[]{srcSystem, defaultStorageSystem, localFilename, "/root", "Staging should fail when user does not have permission on dest path"});
			}
		}

		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="testStageNextAgaveFileDestNoPermissionProvider")//, dependsOnMethods={"testStageNextAgaveFileSourceNoPermission"})
	public void testStageNextAgaveFileDestNoPermission(RemoteSystem sourceSystem, RemoteSystem destSystem, String srcPath, String destPath, String message)
	{
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;
		try
		{
			// load the db with dummy records to stage a http-accessible file
			destClient = destSystem.getRemoteDataClient();
			destClient.authenticate();

			if (!destClient.doesExist("")) {
				destClient.mkdirs("");
			}

			srcClient = sourceSystem.getRemoteDataClient();
			srcClient.authenticate();
			srcClient.mkdirs("");
			srcClient.put(LOCAL_BINARY_FILE, "");

			URI sourceUri = new URI("agave://" + sourceSystem.getSystemId() + "/" + srcPath);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, destSystem, sourceUri, destSystem.getRemoteDataClient().resolvePath(destPath));
			TransferTaskScheduler scheduler = new TransferTaskScheduler();
			scheduler.enqueueStagingTask(file, SYSTEM_OWNER, TRANSFER_API_URL);

//            StagingJob stagingJob = new StagingJob();
//
//			stagingJob.setQueueTask(task);
//
//            stagingJob.doExecute();
//
//			LogicalFile file = task.getLogicalFile();

			Assert.assertEquals(file.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), message);
		}
		catch (Exception e) {
			Assert.fail("File staging should not throw an exception", e);
		}
		finally {
			try { srcClient.delete(""); } catch (Exception ignored) {}
			try { destClient.delete(""); } catch (Exception ignored) {}
		}
	}

//	@Test(dependsOnMethods={"testStageNextAgaveFileDestNoPermission"})
//    public void testStageNextAgaveJobSourceNoPermissionProvider() throws Exception
//    {
//
//
//    }



}

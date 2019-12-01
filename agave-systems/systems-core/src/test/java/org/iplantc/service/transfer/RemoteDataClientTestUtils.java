package org.iplantc.service.transfer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public abstract class RemoteDataClientTestUtils extends BaseTransferTestCase
{
    protected ThreadLocal<RemoteDataClient> threadClient = new ThreadLocal<RemoteDataClient>();

    private static final Logger log = Logger.getLogger(RemoteDataClientTestUtils.class);

    /**
     * Returns a {@link JSONObject} representing the system to test.
     *
     * @return
     * @throws JSONException
     * @throws IOException
     */
    protected abstract JSONObject getSystemJson() throws JSONException, IOException;

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
                String threadHomeDir = String.format("%s/thread-%s-%d",
                        system.getStorageConfig().getHomeDir(),
                        UUID.randomUUID().toString(),
                        Thread.currentThread().getId());
                client.updateSystemRoots(client.getRootDir(),  threadHomeDir);
                threadClient.set(client);
            }
        } catch (RemoteDataException | RemoteCredentialException e) {
            Assert.fail("Failed to get client", e);
        }

        return threadClient.get();
    }

    @BeforeClass(alwaysRun=true)
    protected void beforeSubclass() throws Exception {
        super.beforeClass();

        JSONObject json = getSystemJson();
        json.remove("id");
        json.put("id", this.getClass().getSimpleName());
        system = (StorageSystem)StorageSystem.fromJSON(json);
        system.setOwner(SYSTEM_USER);
        String homeDir = system.getStorageConfig().getHomeDir();
        homeDir = StringUtils.isEmpty(homeDir) ? "" : homeDir;
        system.getStorageConfig().setHomeDir(homeDir + "/" + getClass().getSimpleName());
        storageConfig = system.getStorageConfig();
        salt = system.getSystemId() + storageConfig.getHost() +
                storageConfig.getDefaultAuthConfig().getUsername();

        SystemDao dao = Mockito.mock(SystemDao.class);
        Mockito.when(dao.findBySystemId(Mockito.anyString()))
            .thenReturn(system);
    }

    @AfterClass(alwaysRun=true)
    protected void afterClass() throws Exception {
        try
        {
            getClient().authenticate();
            // remove test directory
            getClient().delete("..");
        }
        catch (FileNotFoundException ignore) {}
        catch (Exception e) {
            Assert.fail("Failed to clean up test home directory " +
                    getClient().resolvePath("") + " after test method.", e);
        }
        finally {
            try { getClient().disconnect(); } catch (Exception ignored) {}
        }
    }

    @BeforeMethod(alwaysRun=true)
    protected void beforeMethod() throws Exception
    {
    	String resolvedHomeDir = "";
        try
        {
            // auth client and ensure test directory is present
            getClient().authenticate();

            resolvedHomeDir = getClient().resolvePath("");

            if (!getClient().mkdirs("")) {
                Assert.fail("System home directory " + resolvedHomeDir + " exists, but is not a directory.");
            }
        }
        catch (IOException | RemoteDataException e) {
            throw e;
        } catch (Exception e) {
            Assert.fail("Failed to create home directory " + resolvedHomeDir + " before test method.", e);
        }
    }

    /**
     * Since there won't be a universally forbidden path, we delegate
     * the decision to each system and protocol test. Specifying
     * shouldExist allows you to separate permission exceptions from
     * file not found exceptions.
     *
     * @param shouldExist Whether the path should exist.
     * @return
     * @throws RemoteDataException
     */
    protected abstract String getForbiddenDirectoryPath(boolean shouldExist) throws RemoteDataException;

    @DataProvider(name="putFolderProvider", parallel=true)
    protected Object[][] putFolderProvider()
    {
        return new Object[][] {
            // remote dest name,    expected dir name,      exception?, message
            { LOCAL_DIR_NAME,       LOCAL_DIR_NAME,         false,     "put local file to remote home directory explicitly setting the identical name should result in folder with same name on remote system." },
            { "somedir",            "somedir",              false,     "put local file to remote home directory explicitly setting a new name should result in folder with new name on remote system." },
            { "",                   LOCAL_DIR_NAME,         false,     "put local directory to empty (home) remote directory without setting a new name should result in folder with same name on remote system." },
        };
    }

    @DataProvider(name="mkdirProvider", parallel=true)
    protected Object[][] mkdirProvider()
    {
        return new Object[][] {
            { null, true, false, "mkdir on null name should resolve to user home and not throw an exception." },
            { "", true, false, "mkdir on empty name should resolve to user home and not throw an exception." },
            { "deleteme-"+System.currentTimeMillis(), false, false, "mkdir new directory in current folder should not fail." },
            { UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString(), false, true, "mkdir when parent does not exist should throw exception." },
        };
    }

    @DataProvider(name="mkdirsProvider", parallel=true)
    protected Object[][] mkdirsProvider()
    {
        return new Object[][] {
            { null, false, "mkdirs on null name should resolve to user home and not throw an exception." },
            { "", false, "mkdirs on empty name should resolve to user home and not throw an exception." },
            { "deleteme-"+System.currentTimeMillis(), false, "mkdirs new directory in current folder should not fail." },
            { UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString(), false, "mkdirs when parent does not exist should not throw an exception." },
        };
    }

    @DataProvider(name="putFileProvider", parallel=true)
    protected Object[][] putFileProvider()
    {
        // expected result of copying LOCAL_BINARY_FILE to the remote system given the following dest
        // paths
        return new Object[][] {
              // remote dest path,      expected result path,   exception?, message
            { LOCAL_TXT_FILE_NAME,      LOCAL_TXT_FILE_NAME,    false,      "put local file to remote destination with different name should result in file with new name on remote system." },
            { "",                       LOCAL_BINARY_FILE_NAME, false,      "put local file to empty(home) directory name should result in file with same name on remote system" },
        };
    }

    @DataProvider(name="putFileOutsideHomeProvider", parallel = false)
    protected Object[][] putFileOutsideHomeProvider() {
        String ext = "-" + new Date().toInstant().getEpochSecond();
        return new Object[][] {
            // remote dest path,            expected result path,       exception?, message
            { "",                           LOCAL_BINARY_FILE_NAME,     false,      "put local file to remote destination with no name outside of home should result in file with same name on remote system." },
            { LOCAL_BINARY_FILE_NAME,          LOCAL_BINARY_FILE_NAME,        false,      "put local file to remote destination with same name outside of home should result in file with the same name on remote system." },
            { LOCAL_TXT_FILE_NAME + ext,    LOCAL_TXT_FILE_NAME + ext,  false,      "put local file to remote destination with different name outside of home should result in file with different name on remote system." },

        };
    }

    @DataProvider(name="getDirectoryRetrievesToCorrectLocationProvider", parallel=true)
    protected Object[][] getDirectoryRetrievesToCorrectLocationProvider()
    {
        String localPath = UUID.randomUUID().toString();
        return new Object[][] {
            { localPath + "-1", true, localPath + "-1/" + LOCAL_DIR_NAME, "Downloading to existing path creates new folder in path." },
            { localPath + "-2/new_get_path", false, localPath + "-2/new_get_path" , "Downloading to non-existing target directory path downloads directory as named path." },
        };
    }

    @DataProvider(name="doRenameProvider", parallel=true)
    protected Object[][] doRenameProvider()
    {
        Object[][] testData = new Object[][] {
            { null, LOCAL_DIR_NAME, true, "null oldpath should resolve to home and throw an exception while trying to rename into its own subtree." },
            { LOCAL_DIR_NAME, null, true, "null newpath should resolve to home and throw an exception wile trying to rename into its own parent." },
            { LOCAL_DIR_NAME, "foo", false, "rename should work for valid file names" },
            { LOCAL_DIR_NAME, LOCAL_DIR_NAME, true, "Renaming file or directory to the same name should throw an exception" },
        };

        for(Object[] testCase: testData) {
            String prefix = getRemoteTestDirPath() + "/";
            if (testCase[0] != null) testCase[0] = prefix + testCase[0];
            if (testCase[1] != null) testCase[1] = prefix + testCase[1];
        }

        return testData;
    }

    @DataProvider(name = "copyIgnoreSlashesProvider", parallel=true)
    protected Object[][] copyIgnoreSlashesProvider() throws Exception {
        Object[][] testData = new Object[][] {
                { "foo", "bar", false, "bar", false, "foo => bar = bar when bar !exists" },
                { "foo/", "bar", false, "bar", false, "foo/ => bar = bar when bar !exists" },
                { "foo", "bar/", false, "bar", false, "foo => bar = bar when bar !exists" },
                { "foo/", "bar/", false, "bar", false, "foo/ => bar = bar when bar !exists" },

                { "foo", "bar", true, "bar", false, "foo => bar = bar/foo when bar exists" },
                { "foo/", "bar", true, "bar", false, "foo/ => bar = bar when bar exists" },
                { "foo", "bar/", true, "bar", false, "foo => bar/ = bar/foo when bar exists" },
                { "foo/", "bar/", true, "bar", false, "foo/ => bar/ = bar when bar exists" }
        };

        for(Object[] testCase: testData) {
            String prefix = UUID.randomUUID().toString() + "/";
            testCase[0] = prefix + testCase[0];
            testCase[1] = prefix + testCase[1];
            testCase[3] = prefix + testCase[3];
        }

        return testData;
    }

    @DataProvider(name="getFileRetrievesToCorrectLocationProvider", parallel=true)
    protected Object[][] getFileRetrievesToCorrectLocationProvider() throws IOException
    {
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

    @DataProvider(name="doesExistProvider", parallel=true)
    protected Object[][] doesExistProvider()
    {
        return new Object[][] {
            { null, true, "null path should resolve to home and not throw an exception." },
            { "", true, "Home directory should exist." },
            { UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString(), false, "Missing directory should not return true from doesExist." },
        };
    }

    protected void authenticate()
    {
        _authenticate();
    }

    protected void _authenticate()
    {
        boolean actuallyThrewException = false;

        boolean shouldThrowException = true;
        String message = "Invalid storage auth should fail";
        RemoteDataClient client = null;
        try
        {
            JSONObject json = getSystemJson();
            RemoteSystem system = (StorageSystem)StorageSystem.fromJSON(json);
            system.setOwner(SYSTEM_USER);
            system.setSystemId("qwerty12345");

            salt = system.getSystemId() + system.getStorageConfig().getHost() +
                    system.getStorageConfig().getDefaultAuthConfig().getUsername();

            system.getStorageConfig().setHomeDir(system.getStorageConfig().getHomeDir() + "/agave-data-unittests");
            system.getStorageConfig().getDefaultAuthConfig().setPassword("qwerty12345");
            system.getStorageConfig().getDefaultAuthConfig().encryptCurrentPassword(salt);

            system.getStorageConfig().getDefaultAuthConfig().setCredential("qwerty12345");
            system.getStorageConfig().getDefaultAuthConfig().encryptCurrentCredential(salt);

            system.getStorageConfig().getDefaultAuthConfig().setPublicKey("qwerty12345");
            system.getStorageConfig().getDefaultAuthConfig().encryptCurrentPublicKey(salt);

            system.getStorageConfig().getDefaultAuthConfig().setPrivateKey("qwerty12345");
            system.getStorageConfig().getDefaultAuthConfig().encryptCurrentPrivateKey(salt);

            SystemDao dao = new SystemDao();
            dao.persist(system);
            client = system.getRemoteDataClient();
            dao.remove(system);

            client.authenticate();
        }
        catch (Exception e) {
            actuallyThrewException = true;
            if (!shouldThrowException)
                Assert.fail(message, e);
        }
        finally {
            try { client.disconnect(); } catch (Exception ignored) {}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }

    protected void _isPermissionMirroringRequired() {
        Assert.assertFalse(getClient().isPermissionMirroringRequired(),
                "permission mirroring should not be required by default.");
    }

    protected void _isThirdPartyTransferSupported() {
        Assert.assertFalse(getClient().isThirdPartyTransferSupported(),
                "Third party transfer should not be supported by default in most protocols.");
    }

    protected void _doesExist(String remotedir, boolean shouldExist, String message)
    {
        try {
            boolean doesExist = getClient().doesExist(remotedir);
            Assert.assertEquals(doesExist, shouldExist, message);
        }
        catch (Exception e) {
            Assert.fail("Failed to query for existence of remote path " + remotedir, e);
        }
    }

    protected void _length()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);

            Assert.assertTrue(getClient().doesExist(remotePath),
                    "Data not found on remote system after put.");
            Assert.assertEquals(getClient().length(remotePath), Files.size(Paths.get(LOCAL_BINARY_FILE)),
                    "remote length does not match local length.");
        }
        catch (Exception e) {
            Assert.fail("Failed to retrieve length of remote file", e);
        }
        finally {
            try {
                getClient().delete(remoteBaseDir);
            } catch (Exception ignore){}
        }
    }

    protected void _mkdir(String remotedir, boolean shouldReturnFalse, boolean shouldThrowException, String message)
    {
        boolean actuallyThrewException = false;

        try
        {
            Assert.assertEquals(getClient().mkdir(remotedir), !shouldReturnFalse, message);

            if (!shouldReturnFalse) {
                Assert.assertTrue(getClient().doesExist(remotedir), "Failed to create remote directory");
            }
        }
        catch (Exception e) {
            if (!shouldThrowException) e.printStackTrace();
            actuallyThrewException = true;
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remotedir)) {
                    getClient().delete(remotedir);
                }
            } catch (Exception ignore){}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }

    protected void _mkdirWithoutRemotePermissionThrowsRemoteDataException()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getForbiddenDirectoryPath(false);

            getClient().mkdir(remoteBaseDir);
            Assert.fail("Mkdir on file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Mkdir on file or folder without permission "
                    + "on the remote system should throw RemoteDataException or FileNotFoundException", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            getClient().mkdir(remoteBaseDir);
        }
        catch (Exception e) {
            Assert.fail("Mkdir on existing path should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _mkdirs(String remotedir, boolean shouldThrowException, String message)
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remoteTestDir = remoteBaseDir + "/" + remotedir;

            getClient().mkdirs(remoteTestDir);
            Assert.assertFalse(shouldThrowException, message);
            Assert.assertTrue(getClient().doesExist(remoteTestDir), "Failed to create remote directory");
        }
        catch (Throwable e) {
            if (!shouldThrowException) e.printStackTrace();
            Assert.assertTrue(shouldThrowException, message);
        }
        finally {
            try {
                getClient().delete(remoteBaseDir);
            } catch (Exception ignore){}
        }
    }

    protected void _mkdirsWithoutRemotePermissionThrowsRemoteDataException()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getForbiddenDirectoryPath(false) + "/bar";

            getClient().mkdirs(remoteBaseDir);
            Assert.fail("Mkdirs on file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (RemoteDataException | IOException e)
        {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Mkdirs on file or folder without permission "
                    + "on the remote system should throw RemoteDataException or IOException", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }

    }

    protected void _putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remoteTestDir = remoteBaseDir + "/" + remotePath;
            String remoteExpectedFilename = remoteBaseDir + "/" + expectedRemoteFilename;

            getClient().put(LOCAL_BINARY_FILE, remoteTestDir);

            Assert.assertTrue(getClient().doesExist(remoteExpectedFilename),
                    "Expected destination " + expectedRemoteFilename + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                getClient().delete(remotePath);
            } catch (Exception ignore){}
        }
    }

    protected void _putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        String remoteBaseDir = null;
        String resolvedAbsolutePath = null;
        try
        {
            String rootDir = getClient().getRootDir();
            resolvedAbsolutePath = getClient().resolvePath("../");

            String baseTestPath = resolvedAbsolutePath.substring(rootDir.length());
            baseTestPath = StringUtils.strip(baseTestPath, "/");
            remoteBaseDir = String.format("/%s/", baseTestPath, getRemoteTestDirPath(), getRemoteTestDirPath());

            getClient().mkdirs(remoteBaseDir);

            String remoteFilePath = remoteBaseDir + "/" + remoteFilename;
            String expectedRemotePath = remoteBaseDir + "/" + FilenameUtils.getName(expectedRemoteFilename);

            getClient().put(LOCAL_BINARY_FILE, remoteFilePath);

            boolean fileExists = getClient().doesExist(expectedRemotePath);

            Assert.assertTrue(fileExists,
                    "Expected destination " + expectedRemotePath + " does not exist after put file");

            Assert.assertTrue(getClient().isFile(expectedRemotePath),
                    "Uploaded file should be a file. Directory found instead.");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteFilename)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderCreatesRemoteFolder(String remoteDirectoryName, String expectedRemoteDirectoryName, boolean shouldThrowException, String message)
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/" + remoteDirectoryName;
            String expectedRemotePath = remoteBaseDir + "/" + expectedRemoteDirectoryName;

            getClient().put(LOCAL_DIR, remotePath);
            for(File child: new File(LOCAL_DIR).listFiles()) {
                System.out.println(child.getAbsolutePath());
            }

            boolean fileExists = getClient().doesExist(expectedRemotePath);

            Assert.assertTrue(fileExists,
                    "Expected destination " + expectedRemotePath + " does not exist after put file");

            for(File child: new File(LOCAL_DIR).listFiles()) {
                Assert.assertTrue(getClient().doesExist(expectedRemotePath + "/" + child.getName()),
                        "Expected uploaded folder content " + expectedRemotePath + "/" + child.getName() +
                        " does not exist after put file");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getRemoteTestDirPath();
            String remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;
            getClient().mkdirs(remoteDir);

            getClient().put(LOCAL_DIR, remoteDir);

            Assert.assertTrue(getClient().doesExist(remoteDir + "/" + LOCAL_DIR_NAME),
                    "Expected destination " + remoteDir + "/" + LOCAL_DIR_NAME
                    + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderMergesContentsWhenRemoteFolderExists()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            getClient().mkdirs(remoteBaseDir + "/foo/bar");
            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "/foo/bar"),
                    "Failed to create test directory for put merge test.");

            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir + "/foo/file.dat");
//            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "foo/file.dat"),
//                    "Failed to upload test file for put merge test.");

            getClient().put(LOCAL_DIR, remoteBaseDir);

            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "/foo/bar"),
                    "Remote directory was deleted during put of folder with non-overlapping file trees.");
            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "/foo/file.dat"),
                    "Remote file was was deleted during put of folder with non-overlapping file trees.");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFileOverwritesExistingFile()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remoteFilePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().put(LOCAL_BINARY_FILE, remoteFilePath);
            Assert.assertTrue(getClient().isFile(remoteFilePath),
                    "Failed to put file prior to overwrite test.");

            getClient().put(LOCAL_BINARY_FILE, remoteFilePath);
            Assert.assertTrue(getClient().isFile(remoteFilePath),
                    "Failed to put file prior to overwrite test.");
        }
        catch (Exception e)
        {
            Assert.fail("Overwriting file should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFileWithoutRemotePermissionThrowsRemoteDataException()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getForbiddenDirectoryPath(false);
            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);
            Assert.fail("Writing to file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (IOException | RemoteDataException e)
        {
            Assert.assertTrue(true);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderWithoutRemotePermissionThrowsRemoteDataException()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getForbiddenDirectoryPath(false);

            getClient().put(LOCAL_DIR, remoteBaseDir);
            Assert.fail("Writing to file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (IOException | RemoteDataException e)
        {
            Assert.assertTrue(true);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remoteFilePath = remoteBaseDir + "/" + remoteFilename;

            getClient().put(LOCAL_DIR, remoteFilePath);

            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "/" + expectedRemoteFilename),
                    "Expected destination " + remoteBaseDir + expectedRemoteFilename + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFileFailsToMissingDestinationPath()
    {
        try
        {
            getClient().put(LOCAL_BINARY_FILE, UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);

            Assert.fail("Put local file to a remote path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.fail("Put local file to a remote path that does not exist should throw FileNotFoundException.");
        }
    }

    protected void _putFolderFailsToMissingDestinationPath()
    {
        try
        {
            getClient().put(LOCAL_DIR, UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);

            Assert.fail("Put folder to a remote directory that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.fail("Put folder to a local directory that does not exist should throw FileNotFoundException.", e);
        }
    }

    protected void _putFailsForMissingLocalPath()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            getClient().put(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY, remoteBaseDir);

            Assert.fail("Put on missing local folder should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.fail("Put on missing local folder should throw FileNotFoundException.");
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _putFolderFailsToRemoteFilePath()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);
            getClient().put(LOCAL_DIR, remotePath);

            Assert.fail("Put folder to a local directory that does not exist should throw FileNotFoundException.");
        }
        catch (IOException e) {
            Assert.fail("Put folder to path of remote file should throw RemoteDataException.", e);
        }
        catch (RemoteDataException e)
        {
            Assert.assertTrue(true);
        }
        finally {
            try {
                if (StringUtils.isNotBlank(remoteBaseDir)) {
                    getClient().delete(remoteBaseDir);
                }
            } catch (Exception ignore){}
        }
    }

    protected void _delete()
    {
        try
        {
            String remoteBaseDir = createRemoteTestDir();
            getClient().put(LOCAL_TXT_FILE, remoteBaseDir);
            String remoteFilename = remoteBaseDir + "/" + LOCAL_TXT_FILE_NAME;

            Assert.assertTrue(getClient().doesExist(remoteFilename),
                    "File " + remoteFilename + " not found on remote system after put.");
            getClient().delete(remoteFilename);
            Assert.assertFalse(getClient().doesExist(remoteFilename),
                    "File " + remoteFilename + " not deleted from remote system.");

            getClient().put(LOCAL_DIR, remoteBaseDir);
            String remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;

            Assert.assertTrue(getClient().doesExist(remoteDir),
                    "Directory " + remoteDir + " not found on remote system after put.");
            getClient().delete(remoteDir);
            Assert.assertFalse(getClient().doesExist(remoteDir),
                    "Directory " + remoteDir + " not deleted from remote system.");
        }
        catch (Exception e) {
            Assert.fail("Failed to delete file or folder", e);
        }
    }

    protected void _deleteFailsOnMissingDirectory()
    {
        try
        {
            getClient().delete(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);
            Assert.fail("delete should throw exception on missing directory");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Incorrect error thorwn when delete missing directory.", e);
        }
    }

    protected void _deleteThrowsExceptionWhenNoPermission()
    {
        try
        {
            getClient().delete(getForbiddenDirectoryPath(true));
            Assert.fail("delete should throw RemoteDataException on no permissions");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("delete should throw RemoteDataException or FileNotFoundException on no permissions");
        }
    }

    protected void _isDirectoryTrueForDirectory()
    {
        try
        {
            Assert.assertTrue(getClient().isDirectory("/"),
                    "root should exist and return true.");

            Assert.assertTrue(getClient().isDirectory(null),
                    "null remoteDirectory to get should resolve to the home directory and return true.");

            Assert.assertTrue(getClient().isDirectory(""),
                    "empty remoteDirectory to get should resolve to the home directory and return true.");

            getClient().mkdirs("some-test-folder");
            Assert.assertTrue(getClient().isDirectory("some-test-folder"),
                    "Directory created for this test should return true.");

            try {
                getClient().isDirectory(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);
                Assert.fail("Non-existent folder should throw exception");
            }
            catch (Exception e) {
                Assert.assertTrue(true);
            }
        }
        catch (Exception e) {
            Assert.fail("isDirectory should not throw unexpected exceptions", e);
        }
    }

    protected void _isDirectoryFalseForFile()
    {
        try
        {
            String remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);
            Assert.assertFalse(getClient().isDirectory(remotePath),
                    "File uploaded for this test should return false.");
        }
        catch (Exception e) {
            Assert.fail("isDirectory should not throw unexpected exceptions", e);
        }
    }

    protected void _isDirectorThrowsExceptionForMissingPath()
    {
        try
        {
            getClient().isDirectory(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);
            Assert.fail("isDirectory should throw exception when checking a non-existent path");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    protected void _isFileFalseForDirectory()
    {
        try
        {
            Assert.assertFalse(getClient().isFile("/"),
                    "root should exist and return false.");

            Assert.assertFalse(getClient().isFile(null),
                    "null remoteDirectory to get should resolve to the home directory and return false.");

            Assert.assertFalse(getClient().isFile(""),
                    "empty remoteDirectory to get should resolve to the home directory and return false.");

            getClient().mkdirs("some-test-folder");
            Assert.assertFalse(getClient().isFile("some-test-folder"),
                    "Directory created for this test should return false.");

            try {
                getClient().isFile(UUID.randomUUID().toString() + "/" + MISSING_FILE);
                Assert.fail("Non-existent folder should throw exception");
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        }
        catch (Exception e) {
            Assert.fail("isFile should not throw unexpected exceptions", e);
        }
    }

    protected void _isFileTrueForFile()
    {
        try
        {
            getClient().put(LOCAL_BINARY_FILE, "");
            Assert.assertTrue(getClient().isFile(LOCAL_BINARY_FILE_NAME),
                    "File uploaded for this test should return true.");
        }
        catch (Exception e) {
            Assert.fail("isFile should not throw unexpected exceptions", e);
        }
    }

    protected void _isFileThrowsExceptionForMissingPath()
    {
        try
        {
            getClient().isFile(UUID.randomUUID().toString() + "/" + MISSING_FILE);
            Assert.fail("isFile should throw exception when checking a non-existent path");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    protected void _ls()
    {
        String remotePath = null;
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            remotePath = remoteBaseDir + "/" + LOCAL_DIR_NAME;
            getClient().put(LOCAL_DIR, remoteBaseDir);

            List<RemoteFileInfo> files = getClient().ls(remotePath);
            List<String> localFiles = Arrays.asList(new File(LOCAL_DIR).list());

            for (RemoteFileInfo file: files)
            {
                if (file.getName().equals(".")) {
                    Assert.fail("Listing should not return current directory in response list.");
                }
                else if (file.getName().equals("..")) {
                    Assert.fail("Listing should not return parent directory in response list.");
                }
                else
                {
                    Assert.assertTrue(localFiles.contains(file.getName()),
                            "Remote file is not present on local file system.");
                }
            }
        }
        catch (Exception e) {
            Assert.fail("Failed to list contents of " + remotePath, e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _lsFailsOnMissingDirectory()
    {
        try
        {
            getClient().ls(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY);
            Assert.fail("ls should throw exception on missing directory");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Incorrect error thorwn when listing missing directory.", e);
        }
    }

    protected void _lsThrowsExceptionWhenNoPermission()
    {
        try
        {
            getClient().ls(getForbiddenDirectoryPath(true));
            Assert.fail("ls should throw exception on no permissions");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("ls should throw RemoteDataException or FileNotFoundException on no permissions");
        }
    }

    protected void _getThrowsExceptionOnMissingRemotePath()
    {
        Path localDir = null;
        try
        {
            localDir = Files.createTempDirectory("_getThrowsExceptionOnMissingRemotePath");

            getClient().get(UUID.randomUUID().toString() + "/" + MISSING_DIRECTORY,
                    localDir.toAbsolutePath().toString());

            Assert.fail("Get on unknown remote path should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Get on unknown remote path should throw FileNotFoundException", e);
        }
        finally {
            try { FileUtils.deleteQuietly(localDir.toFile()); } catch(Exception ignore) {}
        }
    }

    protected void _getThrowsExceptionWhenDownloadingFolderToLocalFilePath()
    {
        Path tempFilePath = null;
        String remoteBaseDir = null;
        try
        {
            tempFilePath = Files.createTempFile("_", "getThrowsExceptionWhenDownloadingFolderToLocalFilePath");

            remoteBaseDir = createRemoteTestDir();

            getClient().get(remoteBaseDir, tempFilePath.toString());
            Assert.fail("Getting remote folder to a local file path should throw RemoteDataException.");
        }
        catch (IOException e) {
            Assert.fail("Getting remote folder to a local file path should throw RemoteDataException.", e);
        }
        catch (RemoteDataException e) {
            // This is what is to be expected
        }
        finally {
            try { if (tempFilePath != null) FileUtils.deleteQuietly(tempFilePath.toFile()); } catch(Exception ignore) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath()
    {
        File missingPath = null;
        String remoteBaseDir = null;
        try
        {
            // create temp directory for download
            missingPath = new File("/tmp/" + getRemoteTestDirPath() + "/" + MISSING_FILE);

            // set up a remote test dir
            remoteBaseDir = createRemoteTestDir();

            // upload a test file to the remote test dir
            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);

            // delete the tmp directory to ensure it does not exist.
            if (missingPath.exists()) {
                FileUtils.deleteQuietly(missingPath);
            }

            getClient().get(remoteBaseDir, missingPath.getAbsolutePath());

            Assert.fail("Getting remote folder to a local directory that does not exist should throw IOException.");
        }
        catch (IOException e) {
            // This is expected
        }
        catch (Exception e) {
            Assert.fail("Getting remote folder to a local directory that does not exist should throw IOException.", e);
        }
        finally {
            try { if (missingPath != null) FileUtils.deleteQuietly(missingPath); } catch(Exception ignore) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath()
    {
        File missingPath = null;
        String remoteBaseDir = null;
        try
        {
            // create temp directory for download
            missingPath = new File("/tmp/" + getRemoteTestDirPath() + "/" + MISSING_FILE);

            // delete the tmp directory to ensure it does not exist.
            if (missingPath.exists()) {
                FileUtils.deleteQuietly(missingPath);
            }

            // set up a remote test dir
            remoteBaseDir = createRemoteTestDir();

            // upload a test file to the remote test dir
            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);

            // try to download the remote test file to the missing path
            String remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;
            getClient().get(remotePath, missingPath.getAbsolutePath());

            // this should not get called
            Assert.fail("Getting remote file to a local directory that does not exist should throw IOException.");
        }
        catch (IOException ignore) {
            // this is expected behavior
        }
        catch (Exception e) {
            Assert.fail("Getting remote folder to a local directory that does not exist should throw IOException.", e);
        }
        finally {
            try { if (missingPath != null) FileUtils.deleteQuietly(missingPath); } catch (Exception ignore) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message)
    {
        String remotePath = null;
        String remoteBaseUrl = null;
        Path tmpDir = null;
        Path testDownloadPath = null;//FileUtils.getFile( + "/" + localdir);
        Path testExpectedDownloadPath = null; //FileUtils.getFile(FileUtils.getTempDirectory() + "/" + expectedDownloadPath);
        try
        {
            tmpDir = Files.createTempDirectory(null);
            testDownloadPath = tmpDir.resolve(localdir);
            testExpectedDownloadPath = tmpDir.resolve(expectedDownloadPath);

            if (createTestDownloadFolder) {
                testDownloadPath.toFile().mkdirs();
            } else {
                testDownloadPath.getParent().toFile().mkdirs();
            }

            remoteBaseUrl = createRemoteTestDir();

            getClient().put(LOCAL_DIR, remoteBaseUrl);
            remotePath = remoteBaseUrl + "/" + LOCAL_DIR_NAME;

            Assert.assertTrue(getClient().doesExist(remotePath),
                    "Data not found on remote system after put.");

            getClient().get(remotePath, testDownloadPath.toAbsolutePath().toString());
            Assert.assertTrue(testExpectedDownloadPath.toFile().exists(), message);

            for(File localFile: FileUtils.getFile(LOCAL_DIR).listFiles()) {
                if (!localFile.getName().equals(".") && !localFile.getName().equals(".."))
                    Assert.assertTrue(testExpectedDownloadPath.resolve(localFile.getName()).toFile().exists(),
                            "Data not found on local system after get.");
            }
        }
        catch (Exception e) {
            Assert.fail("get should not throw unexpected exception", e);
        }
        finally {
            try { getClient().delete(remoteBaseUrl); } catch (Exception ignore) {}
            try { FileUtils.deleteQuietly(testDownloadPath.toFile()); } catch (Exception ignore) {}
            try { FileUtils.deleteQuietly(testExpectedDownloadPath.toFile()); } catch (Exception ignore) {}
        }
    }

    protected void _getFileRetrievesToCorrectLocation(Path localPath, Path expectedDownloadPath, String message)
    {
        String remoteBaseDir = null;
        String remotePath = null;
        Path tempDir = null;
        try
        {
            // Create remote test directory
            remoteBaseDir = createRemoteTestDir();
            remotePath = remoteBaseDir + "/" + expectedDownloadPath.getFileName().toString();

            // Stage upload date
            getClient().put(LOCAL_BINARY_FILE, remotePath);
//
//            // ensure put succeeded
//            Assert.assertTrue(getClient().doesExist(remotePath), "Data not found on remote system after put.");

            // download remote path to local file system
            getClient().get(remotePath, localPath.toAbsolutePath().toString());

            // Ensure local path is present
            Assert.assertTrue(Files.exists(expectedDownloadPath), message);
        }
        catch (Exception e) {
            Assert.fail("get should not throw unexpected exception", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
            try { if (localPath != null) FileUtils.deleteQuietly(localPath.toFile()); } catch(Throwable ignore) {}
            try { if (expectedDownloadPath != null) FileUtils.deleteQuietly(expectedDownloadPath.toFile()); } catch(Throwable ignore) {}
        }
    }

    protected void _getFileOverwritesExistingFile()
    {
        String remoteBaseDir = null;
        Path tmpFile = null;
        try
        {
            // create the file so it's present to be overwritten without endangering our test data
            tmpFile = Files.createTempFile("_getFileOverwritesExistingFile", "original");
            Path downloadFilePath = Files.write(tmpFile, "_getFileOverwritesExistingFile".getBytes());

            // get the orignial length to check against the remote length
            long originalLength = downloadFilePath.toFile().length();

            // create remote test dir for the download file
            remoteBaseDir = createRemoteTestDir();
            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);

            String remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().get(remotePath, downloadFilePath.toString());

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
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
            try { if (tmpFile != null) tmpFile.toFile().delete(); } catch(Throwable ignore) {}
        }
    }

    protected void _getOutputStream()
    {
        OutputStream out = null;
        BufferedInputStream in = null;
        String remotePath = null;
        String remoteBaseDir = null;

        try
        {
            remoteBaseDir = createRemoteTestDir();
            remotePath = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;
            out = getClient().getOutputStream(remotePath, true, false);
            in = new BufferedInputStream(new FileInputStream(LOCAL_BINARY_FILE));
            Assert.assertTrue(IOUtils.copy(in, out) > 0, "Zero bytes were copied to remote output stream.");
            out.flush();
            in.close();
            out.close();

            int i = 0;
            boolean doesExist = false;
            do {
                try {
                    Assert.assertTrue(getClient().doesExist(remotePath),
                            "Data not found on remote system after writing via output stream.");
                    doesExist = true;
                } catch (RemoteDataException e) {
                    if (i==4) throw e;
                }
                i++;
            }
            while (i<5 && !doesExist);


            Assert.assertTrue(getClient().isFile(remotePath),
                    "Data found to be a directory on remote system after writing via output stream.");
        }
        catch (Throwable e) {
            Assert.fail("Writing to output stream threw unexpected exception", e);
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception ignore) {}
            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception ignored) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}

        }
    }

    /**
     * Generates a random directory name for the remote test folder. The
     * remote folder is not created. Use #createRemoteTestDir() to generate
     * a test directory on the remote system.
     *
     * @see #createRemoteTestDir()
     * @return string representing remote test directory.
     */
    protected String getRemoteTestDirPath() {
        return UUID.randomUUID().toString();
    }

    /**
     * Creates a random test directory on the remote system using the
     * RemoteDataClient#mkdirs method. The path is obtained form a
     * call to #getRemoteTestDirPath().
     *
     * @see #getRemoteTestDirPath()
     * @return path to the test directory on the remote system
     * @throws IOException
     * @throws RemoteDataException
     */
    protected String createRemoteTestDir() throws IOException, RemoteDataException {
        String remoteBaseDir = getRemoteTestDirPath();
        getClient().mkdirs(remoteBaseDir);

        return remoteBaseDir;
    }

    protected void _getOutputStreamFailsWhenRemotePathIsNullOrEmpty()
    {
        OutputStream out = null;
        try
        {
            out = getClient().getOutputStream(null, true, false);
            Assert.fail("null remotedir to getOutputStream should throw exception.");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        finally {
            try { out.close(); } catch (Exception ignored) {}
        }

        try
        {
            out = getClient().getOutputStream("", true, false);
            Assert.fail("empty remotedir to getOutputStream should throw exception.");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        finally {
            try { out.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * @
     */
    protected void _getOutputStreamFailsWhenRemotePathIsDirectory()
    {
        OutputStream out = null;
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            out = getClient().getOutputStream(remoteBaseDir, true, false);
        }
        catch (RemoteDataException e) {
            // This is expected
        }
        catch (Throwable t) {
            Assert.fail("Passing valid directory to getOutputStream should throw RemoteDataException.", t);
        }
        finally {
            try { if (out != null) out.close(); } catch (Exception ignore) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }

        try {
            out = getClient().getOutputStream("", true, false);
        }
        catch (RemoteDataException e) {
            // This is expected
        }
        catch (Throwable t) {
            Assert.fail("empty remote path to getOutputStream should throw exception.", t);
        }
        finally {
            try { if (out != null) out.close(); } catch (Exception ignore) {}
        }
    }

    protected void _getOutputStreamFailsOnMissingPath()
    {
        String remoteBaseDir = null;
        String remoteDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            remoteDir = remoteBaseDir + "/" + getRemoteTestDirPath();

            getClient().getOutputStream(MISSING_DIRECTORY + "/" + LOCAL_TXT_FILE_NAME, true, false);
            Assert.fail("getOutputStream should throw FileNotFoundException on missing directory");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("getOutputStream should throw FileNotFoundException on missing directory", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _getOutputStreamThrowsExceptionWhenNoPermission()
    {
        try
        {
            getClient().getOutputStream(getForbiddenDirectoryPath(true) + "/" + LOCAL_TXT_FILE_NAME, true, false);
            Assert.fail("getOutputStream should throw RemoteDataException on no permissions");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("getOutputStream should throw RemoteDataException or FileNotFoundException on no permissions",e);
        }
        finally {
            try { getClient().delete(getForbiddenDirectoryPath(true) + "/" + LOCAL_TXT_FILE_NAME); } catch (Exception ignore) {}
        }
    }

    @DataProvider(name="getInputStreamProvider", parallel=true)
    protected Object[][] getInputStreamProvider()
    {
        return new Object[][] {
                { LOCAL_TXT_FILE, "get local file from remote home directory should succeed." },
        };
    }

    @DataProvider(name="getInputStreamOnRemoteDirectoryThrowsExceptionProvider", parallel=true)
    protected Object[][] getInputStreamOnRemoteDirectoryThrowsExceptionProvider()
    {
        return new Object[][] {
                { "", "opening input stream on empty remote path should throw exception." },
                { null, "opening input stream on null remote path should throw exception." },
                { getRemoteTestDirPath(), "opening input stream on valid remote directory should throw exception." },
        };
    }

    protected void _getInputStream(String localFile, String message) {
        InputStream in = null;
        OutputStream out = null;
        BufferedOutputStream bout = null;
        String remoteBaseDir = null;
        String remotePath = null;
        Path localFilePath = null;
        Path localDownloadFile = null;
        try {
            localFilePath = Files.createTempFile("_getInputStream", "txt");
            Files.write(localFilePath, "_getInputStream".getBytes());

            remoteBaseDir = createRemoteTestDir();
            getClient().put(localFilePath.toAbsolutePath().toString(), remoteBaseDir);

            remotePath = remoteBaseDir + "/" + localFilePath.toFile().getName();

            localDownloadFile = Files.createTempFile("_getInputStream_download", "txt");

            in = getClient().getInputStream(remotePath, true);
            out = Files.newOutputStream(localDownloadFile);
            bout = new BufferedOutputStream(out);

            int bufferSize = getClient().getMaxBufferSize();
            byte[] b = new byte[bufferSize];
            int len = 0;

            while ((len = in.read(b)) > -1) {
                bout.write(b, 0, len);
            }

            bout.flush();

            Assert.assertEquals(localFilePath.toFile().length(), localDownloadFile.toFile().length(),
                    "Input stream read from remote host resulted in different file size than local test file.");
        } catch (Exception e) {
            Assert.fail("Reading input stream of valid remote file should not result in exception", e);
        } finally {
            try {
                if (in != null) in.close();
            } catch (Exception ignored) {
            }
            try {
                if (out != null) out.close();
            } catch (Exception ignored) {
            }
            try {
                if (bout != null) bout.close();
            } catch (Exception ignored) {
            }
            try {
                getClient().delete(remoteBaseDir);
            } catch (Exception ignore) {
            }
        }
    }

    protected void _getInputStreamOnRemoteDirectoryThrowsException(String remotedir, String message)
    {
        InputStream in = null;
        try
        {
            getClient().mkdirs(remotedir);

            in = getClient().getInputStream(remotedir, true);

            Assert.fail(message);
        }
        catch (IOException e) {
            Assert.fail("Opening input stream on directory should throw RemoteDataException", e);
        }
        catch (RemoteDataException e) {
            // this is what we're looking for
            Assert.assertEquals(e.getClass().getSimpleName(), RemoteDataException.class.getSimpleName(),
                    "Opening input stream on directory should throw RemoteDataException");
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            }
            catch (Exception ignore) {}

            try {
                if (StringUtils.isNotBlank(remotedir)) {
                    getClient().delete(remotedir);
                }
            } catch (Exception ignore) {}
        }
    }

    protected void _getInputStreamThrowsExceptionWhenNoPermission()
    {
        try
        {
            getClient().getInputStream(getForbiddenDirectoryPath(true), true);
            Assert.fail("getInputStream should throw RemoteDataException on no permissions");
        }
        catch (RemoteDataException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("getInputStream should throw RemoteDataException on no permissions", e);
        }
    }

    protected void _checksumDirectoryFails()
    {

        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            getClient().checksum(remoteBaseDir);
            Assert.fail("Checksum is not supported on directories.");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _checksumMissingPathThrowsException()
    {
        try
        {
            getClient().checksum(getRemoteTestDirPath() + "/" + MISSING_DIRECTORY);
            Assert.fail("Checksum a missing path should throw a FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Checksum a missing path should throw a FileNotFoundException.");
        }
    }

    protected void _checksum()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/" + LOCAL_TXT_FILE_NAME;
            getClient().put(LOCAL_TXT_FILE, remoteBaseDir);

            Assert.assertTrue(getClient().doesExist(remotePath),
                    "Data not found on remote system after put.");

            getClient().checksum(remotePath);
            Assert.fail("Checksum should throw a NotImplementedException unless overridden by a concrete test class");
        }
        catch (NotImplementedException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Checksum should throw a NotImplementedException for " +
                    system.getStorageConfig().getProtocol().toString(), e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _doRename(String oldpath, String newpath, boolean shouldThrowException, String message)
    {
        boolean actuallyThrewException = false;

        try
        {
            if (! StringUtils.isEmpty(oldpath)) {
                getClient().mkdirs(oldpath);
            }

            Assert.assertTrue(getClient().doesExist(oldpath),
                    "Source folder not created for rename test.");

            getClient().doRename(oldpath, newpath);

            Assert.assertTrue(getClient().doesExist(newpath),
                    "Rename operation did not rename the file or directory to " + newpath);
        }
        catch (Exception e) {
            actuallyThrewException = true;
            if (!shouldThrowException)
                Assert.fail(message, e);
        }
        finally {
            try {
                if (! StringUtils.isBlank(oldpath)) {
                    getClient().delete(oldpath.substring(0, oldpath.lastIndexOf("/")));
                }
            } catch (Exception ignore) {}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }

    protected void _doRenameThrowsRemotePermissionExceptionToRestrictedSource()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = getRemoteTestDirPath();
            getClient().doRename(getForbiddenDirectoryPath(true), remoteBaseDir);

            Assert.fail("Rename a restricted remote path to a dest path should throw RemoteDataException.");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Rename a restricted remote path to a dest path should throw RemoteDataException or FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _doRenameThrowsRemotePermissionExceptionToRestrictedDest()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            getClient().doRename(remoteBaseDir, getForbiddenDirectoryPath(true));

            Assert.fail("Rename a remote path to a dest path the user does not have "
                    + "permission to access should throw RemoteDataException.");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Rename a remote path to a dest path the user does not have "
                    + "permission to access should throw RemoteDataException or FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _doRenameThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        String remoteBaseDir = null;
        String remoteRenameBaseDir = getRemoteTestDirPath();
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remoteRenameDir = remoteRenameBaseDir + "/" + MISSING_DIRECTORY;
            getClient().doRename(remoteBaseDir, remoteRenameDir);

            Assert.fail("Rename a remote source path to a dest path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.fail("Rename a remote source path to a dest path that does not exist should throw FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
            try { getClient().delete(remoteRenameBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _doRenameThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            String remoteMissingDir = remoteBaseDir + "/" + MISSING_DIRECTORY;
            String remoteDestDir = remoteBaseDir + "/" + getRemoteTestDirPath();
            getClient().doRename(remoteMissingDir, remoteDestDir);

            Assert.fail("Rename a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e)
        {
            Assert.fail("Rename a remote source path that does not exist should throw FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _getUrlForPath()
    {}

    protected void _copyFile()
    {
        String remoteBaseDir = null;
        String remoteSrc = null;
        String remoteDest = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            remoteSrc = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME;
            remoteDest = remoteBaseDir + "/" + LOCAL_BINARY_FILE_NAME + ".copy";

            getClient().put(LOCAL_BINARY_FILE, remoteBaseDir);

            getClient().copy(remoteSrc, remoteDest);

            Assert.assertTrue(getClient().doesExist(remoteDest), "Copy operation failed from " +
                    remoteSrc + " to " + remoteDest);
        }
        catch (Exception e) {
            Assert.fail("Copy operation failed from " +
                remoteSrc + " to " + remoteDest, e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message)
    {

        String remoteBaseDir = null;
        String remoteSrc = null;
        String remoteDest = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            remoteSrc = remoteBaseDir + "/" + src;
            remoteDest = remoteBaseDir + "/" + dest;

            getClient().mkdirs(remoteSrc);

            if (createDest) {
                getClient().mkdirs(remoteDest);
            }

            getClient().copy(remoteSrc, remoteDest);

            Assert.assertTrue(getClient().doesExist(remoteBaseDir + "/" + expectedPath), message);
        }
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _copyThrowsRemoteDataExceptionToRestrictedDest()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();

            getClient().copy(remoteBaseDir, getForbiddenDirectoryPath(true));

            Assert.fail("copy a remote path to a dest path the user does not have "
                    + "permission to access should throw RemoteDataException.");
           }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote path to a dest path the user does not have "
                    + "permission to access should throw RemoteDataException or FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _copyThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        String remoteBaseDir = null;
        String remoteMissingPath = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            remoteMissingPath = getRemoteTestDirPath() + "/" + MISSING_DIRECTORY;
            getClient().copy(remoteBaseDir, remoteMissingPath);

            Assert.fail("copy a remote source path to a dest path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path to a dest path that does not exist should throw FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
            try { getClient().delete(remoteMissingPath); } catch (Exception ignore) {}
        }
    }

    protected void _copyThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        String remoteBaseDir = null;
        String remoteMissingPath = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            remoteMissingPath = getRemoteTestDirPath() + "/" + MISSING_FILE;

            getClient().copy(remoteMissingPath, remoteBaseDir);

            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _copyThrowsRemoteDataExceptionToRestrictedSource()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String forbiddenDir = getForbiddenDirectoryPath(true);
            getClient().copy(forbiddenDir, remoteBaseDir);

            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException | RemoteDataException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path that does not exist should throw RemoteDataException or FileNotFoundException.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteFile()
    {
        File tmpFile = null;
        String remoteBaseDir = null;
        try
        {
            tmpFile = File.createTempFile("_syncToRemoteFile", "txt");
            FileUtils.writeStringToFile(tmpFile, "This should be overwritten.");

            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/_syncToRemoteFile.txt";

            getClient().syncToRemote(tmpFile.getAbsolutePath(), remotePath,null);
            Assert.assertTrue(getClient().doesExist(remotePath),
                    "Remote file was not created during sync");

            Assert.assertEquals(getClient().length(remotePath), tmpFile.length(),
                    "Remote file was not copied in whole created during sync");

            getClient().syncToRemote(LOCAL_BINARY_FILE, remotePath,null);
            Assert.assertNotEquals(getClient().length(remotePath), tmpFile.length(),
                    "Remote file size should not match temp file after syncing.");

            Assert.assertEquals(getClient().length(remotePath), new File(LOCAL_BINARY_FILE).length(),
                    "Remote file size should match local binary file after syncing.");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try { if (tmpFile != null) tmpFile.delete(); } catch (Exception ignore) {}
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteFolderAddsMissingFiles()
    {
        String remoteBaseDir = null;
        try
        {
            remoteBaseDir = createRemoteTestDir();
            String remotePath = remoteBaseDir + "/_syncToRemoteFile.txt";

            File localDir = new File(LOCAL_DIR);
            String remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;
            getClient().mkdirs(remoteDir);

            Assert.assertTrue(getClient().doesExist(remoteDir),
                    "Remote directory was not created. Test will fail");

            getClient().syncToRemote(LOCAL_DIR, remoteBaseDir, null);
            Assert.assertFalse(getClient().doesExist(remoteDir + "/" + LOCAL_DIR_NAME),
                    "Local directory was synced as a subfolder instead of overwriting");

            List<RemoteFileInfo> remoteListing = getClient().ls(remoteDir);

            Assert.assertEquals(localDir.list().length, remoteListing.size(),
                    "Mismatch between file count locally and remote.");

            for (RemoteFileInfo fileInfo: remoteListing)
            {
                File localFile = new File(LOCAL_DIR, fileInfo.getName());

                Assert.assertEquals(fileInfo.getSize(), localFile.length(),
                        "Remote file is a different size than the local one.");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteFolderLeavesExistingFilesUntouched()
    {
        String remoteBaseDir = null;
        try
        {
            // create test dir for test data staging
            remoteBaseDir = createRemoteTestDir();

            // define expected uploaded test directory path
            String remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;
            getClient().put(LOCAL_DIR, remoteBaseDir);

            Assert.assertTrue(getClient().doesExist(remoteDir),
                    "Remote directory was not uploaded. Test will fail");

            Map<String, Date> lastUpdatedMap = new HashMap<String,Date>();
            List<RemoteFileInfo> remoteListing = getClient().ls(remoteDir);
            for (RemoteFileInfo fileInfo: remoteListing)
            {
                lastUpdatedMap.put(fileInfo.getName(), fileInfo.getLastModified());
            }

            getClient().syncToRemote(LOCAL_DIR, remoteBaseDir, null);

            Assert.assertFalse(getClient().doesExist(remoteDir + "/" + LOCAL_DIR_NAME),
                    "Local directory was synced as a subfolder instead of overwriting");

            remoteListing = getClient().ls(remoteDir);

            Assert.assertEquals(new File(LOCAL_DIR).list().length, remoteListing.size(),
                    "Mismatch between file count locally and remote.");

            for (RemoteFileInfo fileInfo: remoteListing)
            {
                File localFile = new File(LOCAL_DIR, fileInfo.getName());

                Assert.assertTrue(localFile.exists(),
                        String.format("No local file found matching remote remote file item %s/%s",
                                remoteBaseDir, fileInfo.getName()));

                Assert.assertEquals(fileInfo.getSize(), localFile.length(),
                        "Remote file is a different size than the local one.");

                Assert.assertEquals(fileInfo.getLastModified(), lastUpdatedMap.get(fileInfo.getName()),
                        "Dates do not match up. File must have been overwritten.");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteFolderOverwritesFilesWithDeltas()
    {
        String remoteBaseDir = null;
        String remoteDir = null;
        Path tmpFile = null;
        try
        {
            // create test dir for test data staging
            remoteBaseDir = getRemoteTestDirPath();
            remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;

            File localDir = new File(LOCAL_DIR);
            getClient().mkdirs(remoteDir);

            tmpFile = Files.createTempFile("syncToRemoteFile", "txt");
            Files.write(tmpFile, "This should be overwritten.".getBytes());

            String remoteFileToOverwrightString = remoteDir + "/" + LOCAL_BINARY_FILE_NAME;
            getClient().put(tmpFile.toAbsolutePath().toString(), remoteFileToOverwrightString);

            Assert.assertTrue(getClient().doesExist(remoteFileToOverwrightString),
                    "Remote file was not created during sync");

            Assert.assertEquals(getClient().length(remoteFileToOverwrightString), Files.size(tmpFile),
                    "Remote file was not copied in whole created during sync");

            getClient().syncToRemote(LOCAL_DIR, remoteBaseDir, null);
            Assert.assertFalse(getClient().doesExist(remoteDir + "/" + LOCAL_DIR_NAME),
                    "Local directory was synced as a subfolder instead of overwriting");

            List<RemoteFileInfo> remoteListing = getClient().ls(remoteDir);
            File[] localListing = localDir.listFiles();

            Assert.assertEquals(localListing.length, remoteListing.size(),
                    "Mismatch between file count locally and remote.");

            for (RemoteFileInfo fileInfo: remoteListing)
            {
                File localFile = new File(LOCAL_DIR, fileInfo.getName());

                Assert.assertEquals(fileInfo.getSize(), localFile.length(),
                        "Remote file is a different size than the local one.");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteFolderReplacesOverlappingFilesAndFolders()
    {
        String remoteBaseDir = null;
        try
        {
            // create test dir for test data staging
            remoteBaseDir = getRemoteTestDirPath();

            // define expected uploaded test directory path
            String remoteDir = remoteBaseDir + "/" + LOCAL_DIR_NAME;
            String remoteFolderToOverwrightString = remoteDir + "/" + LOCAL_BINARY_FILE_NAME;

            getClient().mkdirs(remoteFolderToOverwrightString);
            getClient().put(LOCAL_TXT_FILE, remoteDir + "/" + LOCAL_TXT_FILE_NAME);

            Assert.assertTrue(getClient().doesExist(remoteFolderToOverwrightString),
                    "Remote subdirectory was not created during sync");

            // sync the local dir to remote. This shoudl overwrite the LOCAL_BINARY_FILE_NAME folder
            // we created in the setup
            getClient().syncToRemote(LOCAL_DIR, remoteBaseDir, null);

            List<RemoteFileInfo> remoteListing = getClient().ls(remoteDir);

            File localDir = new File(LOCAL_DIR);
            Assert.assertEquals(localDir.list().length, remoteListing.size(),
                    "Mismatch between file count locally and remote.");

            for (RemoteFileInfo fileInfo: remoteListing)
            {
                File localFile = new File(LOCAL_DIR, fileInfo.getName());

                Assert.assertEquals(fileInfo.getSize(), localFile.length(),
                        "Remote file is a different size than the local one.");

                Assert.assertEquals(fileInfo.isDirectory(), localFile.isDirectory(),
                        "Remote file is " + (localFile.isDirectory()? "not ": "") +
                        "a file like the local one. Sync should delete remote and replace with local.");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore) {}
        }
    }

    protected void _syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException
    {
        Path tempLocalDir = null;
        Path tmpFile;
        String remoteBaseDir = null;
        try
        {
            // create test dir for test data staging
            remoteBaseDir = getRemoteTestDirPath();
            byte[] tmpOriginalFileText =  "This should be overwritten.".getBytes();
            String[] tempSubdirectories = {
                    "foo",
                    "foo/alpha",
                    "foo/alpha/gamma",
                    "bar",
                    "bar/beta",
            };
            
            tempLocalDir = Files.createTempDirectory(null);
            tmpFile = Files.createFile(tempLocalDir.resolve("temp.txt"));
            Files.write(tmpFile, tmpOriginalFileText);

            for (String path: tempSubdirectories) {
                Path subDirPath = Files.createDirectories(tempLocalDir.resolve(path));
                Path tmpFile1 = Files.createFile(subDirPath.resolve("file1.txt"));
                Files.write(tmpFile1, tmpOriginalFileText);
                Path tmpFile2 = Files.createFile(subDirPath.resolve("file2.txt"));
                Files.write(tmpFile2, tmpOriginalFileText);
            }

            String remoteDir = remoteBaseDir + "/" + tempLocalDir.toFile().getName();
            getClient().mkdirs(remoteDir + "/foo/alpha/gamma");
            getClient().mkdirs(remoteDir + "/bar/beta");
            
//            TransferTask task = new TransferTask(
//                    tempDir.toURI().toString(), 
//                    getClient().getUriForPath(tempDir.getName() + "/" + tempDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(tempLocalDir, "", new RemoteTransferListener(task));

            getClient().syncToRemote(tempLocalDir.toAbsolutePath().toString(), remoteBaseDir, null);
            Assert.assertFalse(getClient().doesExist(remoteDir + "/" + tempLocalDir.toFile().getName()),
                    "Syncing put the local dir into the remote directory instead of overwriting");
            
            for (String path: tempSubdirectories) 
            {
                Assert.assertFalse(getClient().doesExist(remoteDir + "/" + path + "/" + FilenameUtils.getName(path)),
                    "Syncing put the local subdirectory into the remote subdirectory instead of overwriting");
                Assert.assertTrue(getClient().doesExist(remoteDir + "/" + path + "/file1.txt"),
                    "Failed to copy local file1.txt to remote folder in proper place.");
                Assert.assertTrue(getClient().doesExist(remoteDir + "/" + path + "/file2.txt"),
                        "Failed to copy local file2.txt to remote folder in proper place.");
            }
        }
        catch (AssertionError e) {
            throw e;
        }
        catch (Exception e) 
        {
            log.error("Put of file or folder should not throw exception.", e);
            throw e;
        }
        finally {
            try { getClient().delete(remoteBaseDir); } catch (Exception ignore){}
            try { FileUtils.deleteQuietly(tempLocalDir.toFile()); } catch (Exception ignore) {}
        }
    }
}

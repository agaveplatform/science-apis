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
import org.testng.annotations.*;

import java.io.*;
import java.util.*;

public abstract class AbstractRemoteDataClientTest extends BaseTransferTestCase 
{
    protected File tmpFile = null;
    protected File tempDir = null;
    protected ThreadLocal<RemoteDataClient> threadClient = new ThreadLocal<RemoteDataClient>();
    
    private static final Logger log = Logger.getLogger(AbstractRemoteDataClientTest.class);
    
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
                client.updateSystemRoots(client.getRootDir(), system.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
                threadClient.set(client);
            } 
        } catch (RemoteDataException | RemoteCredentialException e) {
            Assert.fail("Failed to get client", e);
        }
        
        return threadClient.get();
    }
    
    protected String getLocalDownloadDir() {
        return LOCAL_DOWNLOAD_DIR + Thread.currentThread().getId();
    }
    
    @BeforeClass(alwaysRun=true)
    protected void beforeSubclass() throws Exception {
        super.beforeClass();
        
        JSONObject json = getSystemJson();
        json.remove("id");
        json.put("id", this.getClass().getSimpleName());
        system = StorageSystem.fromJSON(json);
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
            FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
            FileUtils.deleteQuietly(tmpFile);
            FileUtils.deleteQuietly(tmpFile);
//            clearSystems();
        }
        finally {
            try { getClient().disconnect(); } catch (Exception ignored) {}
        }   
        
        try
        {
            getClient().authenticate();
            // remove test directory
            getClient().delete("..");
            Assert.assertFalse(getClient().doesExist(""), "Failed to clean up home directory after test.");
        } 
        catch (Exception e) {
            Assert.fail("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
        }
        finally {
            try { getClient().disconnect(); } catch (Exception ignored) {}
        }
    }

    @BeforeMethod(alwaysRun=true)
    protected void beforeMethod() throws Exception 
    {
    	FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
        FileUtils.deleteQuietly(tmpFile);
        
        try
        {   
            // auth client and ensure test directory is present
            getClient().authenticate();
            if (getClient().doesExist("")) {
                getClient().delete("");
            }
            
            
            if (!getClient().mkdirs("")) {
                Assert.fail("System home directory " + client.resolvePath("") + " exists, but is not a directory.");
            }
        } 
        catch (IOException | RemoteDataException e) {
            throw e;
        } catch (Exception e) {
            Assert.fail("Failed to create home directory " + (client == null ? "" : client.resolvePath("")) + " before test method.", e);
        }
    }

    @AfterMethod(alwaysRun=true)
    protected void afterMethod() throws Exception {}

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
    
    @DataProvider(name="putFolderProvider", parallel=false)
    protected Object[][] putFolderProvider()
    {
        return new Object[][] {
            // remote dest path,   expected result path,   exception?, message
            { LOCAL_DIR_NAME,       LOCAL_DIR_NAME,         false,     "put local file to remote home directory explicitly setting the identical name should result in folder with same name on remote system." },
            { "somedir",            "somedir",              false,     "put local file to remote home directory explicitly setting a new name should result in folder with new name on remote system." },
            { "",                   LOCAL_DIR_NAME,         false,     "put local directory to empty (home) remote directory without setting a new name should result in folder with same name on remote system." },
            
        };
    }
    
    @DataProvider(name="mkdirProvider", parallel=false)
    protected Object[][] mkdirProvider()
    {
        return new Object[][] {
            { null, true, false, "mkdir on null name should resolve to user home and not throw an exception." },
            { "", true, false, "mkdir on empty name should resolve to user home and not throw an exception." },
            { "deleteme-"+System.currentTimeMillis(), false, false, "mkdir new directory in current folder should not fail." },
            { MISSING_DIRECTORY, false, true, "mkdir when parent does not exist should throw exception." },
        };
    }
    
    @DataProvider(name="mkdirsProvider", parallel=false)
    protected Object[][] mkdirsProvider()
    {
        return new Object[][] {
            { null, false, "mkdirs on null name should resolve to user home and not throw an exception." },
            { "", false, "mkdirs on empty name should resolve to user home and not throw an exception." },
            { "deleteme-"+System.currentTimeMillis(), false, "mkdirs new directory in current folder should not fail." },
            { MISSING_DIRECTORY, false, "mkdirs when parent does not exist should not throw an exception." },
        };
    }
    
    @DataProvider(name="putFileProvider", parallel=false)
    protected Object[][] putFileProvider()
    {
        // expected result of copying LOCAL_BINARY_FILE to the remote system given the following dest
        // paths
        return new Object[][] {
              // remote dest path,  expected result path,  exception?, message
            { LOCAL_TXT_FILE_NAME, LOCAL_TXT_FILE_NAME,    false,      "put local file to remote destination with different name should result in file with new name on remote system." },
            { "",                  LOCAL_BINARY_FILE_NAME, false,      "put local file to empty(home) directory name should result in file with same name on remote system" },
        };
    }

    @DataProvider(name="getDirectoryRetrievesToCorrectLocationProvider", parallel=false)
    protected Object[][] getDirectoryRetrievesToCorrectLocationProvider()
    {
        String localPath = UUID.randomUUID().toString();
        return new Object[][] {
            { localPath + "-1", true, localPath + "-1/" + LOCAL_DIR_NAME, "Downloading to existing path creates new folder in path." },
            { localPath + "-2/new_get_path", false, localPath + "-2/new_get_path" , "Downloading to non-existing target directory path downloads directory as named path." },
        };
    }
    
    @DataProvider(name="doRenameProvider", parallel=false)
    protected Object[][] doRenameProvider()
    {
        return new Object[][] {
            { null, LOCAL_DIR_NAME, true, "null oldpath should resolve to home and throw an exception while trying to rename into its own subtree." },
            { LOCAL_DIR_NAME, null, true, "null newpath should resolve to home and throw an exception wile trying to rename into its own parent." },
            { LOCAL_DIR_NAME, "foo", false, "rename should work for valid file names" },
            { LOCAL_DIR_NAME, LOCAL_DIR_NAME, true, "Renaming file or directory to the same name should throw an exception" },
        };
    }
    
    @DataProvider(name = "copyIgnoreSlashesProvider", parallel=false)
    protected Object[][] copyIgnoreSlashesProvider() throws Exception {
        return new Object[][] {
                { "foo", "bar", false, "bar", false, "foo => bar = bar when bar !exists" },
                { "foo/", "bar", false, "bar", false, "foo/ => bar = bar when bar !exists" },
                { "foo", "bar/", false, "bar", false, "foo => bar = bar when bar !exists" },
                { "foo/", "bar/", false, "bar", false, "foo/ => bar = bar when bar !exists" },
                
                { "foo", "bar", true, "bar", false, "foo => bar = bar/foo when bar exists" },
                { "foo/", "bar", true, "bar", false, "foo/ => bar = bar when bar exists" },
                { "foo", "bar/", true, "bar", false, "foo => bar/ = bar/foo when bar exists" },
                { "foo/", "bar/", true, "bar", false, "foo/ => bar/ = bar when bar exists" }    
        };
    }
    
    @DataProvider(name="getFileRetrievesToCorrectLocationProvider")
    protected Object[][] getFileRetrievesToCorrectLocationProvider()
    {
        return new Object[][] {
            { getLocalDownloadDir(), getLocalDownloadDir() + "/" + LOCAL_BINARY_FILE_NAME, "Downloading to existing path creates new file in path." },
            { getLocalDownloadDir() + "/" + LOCAL_BINARY_FILE_NAME, getLocalDownloadDir() + "/" + LOCAL_BINARY_FILE_NAME, "Downloading to explicit file path where no file exists creates the file." },
        };
    }
    
    @DataProvider(name="doesExistProvider")
    protected Object[][] doesExistProvider()
    { 
        return new Object[][] {
            { null, true, "null path should resolve to home and not throw an exception." },
            { "", true, "Home directory should exist." },
            { MISSING_DIRECTORY, false, "Missing directory should not return true from doesExist." },
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
            RemoteSystem system = StorageSystem.fromJSON(json);
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
    
    @Test(groups= {"proxy"})
    public void isPermissionMirroringRequired() {
        _isPermissionMirroringRequired();
    }
    
    protected void _isPermissionMirroringRequired() {
        Assert.assertFalse(getClient().isPermissionMirroringRequired(), 
                "permission mirroring should not be required by default.");
    }
    
    @Test(groups= {"proxy"}, dependsOnMethods= {"isPermissionMirroringRequired"})
    public void isThirdPartyTransferSupported() 
    {
        _isThirdPartyTransferSupported();
    }
    
    protected void _isThirdPartyTransferSupported() {
        Assert.assertFalse(getClient().isThirdPartyTransferSupported(),
                "Third party transfer should not be supported by default in most protocols.");
    }
    
    @Test(dataProvider="doesExistProvider", dependsOnGroups= {"proxy"})
    public void doesExist(String remotedir, boolean shouldExist, String message)
    {
        _doesExist(remotedir, shouldExist, message);
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
   
    @Test(groups="size", dependsOnMethods= {"doesExist"})
    public void length()
    {
        _length();
    }
    
    protected void _length()
    {
        try 
        {
            getClient().put(LOCAL_BINARY_FILE, "");
            String remotePutPath = LOCAL_BINARY_FILE_NAME;
            Assert.assertTrue(getClient().doesExist(remotePutPath), 
                    "Data not found on remote system after put.");
            Assert.assertTrue(new File(LOCAL_BINARY_FILE).length() == getClient().length(remotePutPath),
                    "remote length does not match local length.");
        } 
        catch (Exception e) {
            Assert.fail("Failed to retrieve length of remote file", e);
        }
    }
    
    @Test(dataProvider="mkdirProvider", groups= {"mkdir"}, dependsOnGroups={"size"})
    public void mkdir(String remotedir, boolean shouldReturnFalse, boolean shouldThrowException, String message)
    {
        _mkdir(remotedir, shouldReturnFalse, shouldThrowException, message);
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
        
        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }
    
    @Test(groups= {"mkdir"}, dependsOnMethods={"mkdir"})
    public void mkdirWithoutRemotePermissionThrowsRemoteDataException() 
    {
        _mkdirWithoutRemotePermissionThrowsRemoteDataException(); 
    }
    
    protected void _mkdirWithoutRemotePermissionThrowsRemoteDataException() 
    {
        try 
        {
            getClient().mkdir(getForbiddenDirectoryPath(false));
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
    }

    @Test(groups= {"mkdir"}, dependsOnMethods={"mkdirWithoutRemotePermissionThrowsRemoteDataException"})
    public void mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists() 
    {
        _mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists();
    }
    
    protected void _mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists()
    {
        try 
        { 
            getClient().mkdir("someincrediblylongdirectoryname");
            getClient().mkdir("someincrediblylongdirectoryname");
        } 
        catch (Exception e) {
            Assert.fail("Mkdir on existing path should not throw exception.", e);
        }
    }
    
    @Test(dataProvider="mkdirsProvider", groups= {"mkdir"}, dependsOnMethods={"mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists"})
    public void mkdirs(String remotedir, boolean shouldThrowException, String message)
    {
        _mkdirs(remotedir, shouldThrowException, message);
    }
    
    protected void _mkdirs(String remotedir, boolean shouldThrowException, String message)
    {
        try 
        {
            getClient().mkdirs(remotedir);
            Assert.assertFalse(shouldThrowException, message);
            Assert.assertTrue(getClient().doesExist(remotedir), "Failed to create remote directory");
        } 
        catch (Throwable e) {
            if (!shouldThrowException) e.printStackTrace();
            Assert.assertTrue(shouldThrowException, message); 
        }
    }
    
    @Test(groups= {"mkdir"}, dependsOnMethods={"mkdirs"})
    public void mkdirsWithoutRemotePermissionThrowsRemoteDataException() 
    {
        _mkdirsWithoutRemotePermissionThrowsRemoteDataException();
    }
    
    protected void _mkdirsWithoutRemotePermissionThrowsRemoteDataException()
    {
        try 
        {
            getClient().mkdirs(getForbiddenDirectoryPath(false) + "/bar");
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
        
    }
    
    @Test(dataProvider="putFileProvider", groups={"put", "upload"}, dependsOnGroups={"mkdir"})//, invocationCount=25, threadPoolSize=5 )
    public void putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        _putFile(remotePath, expectedRemoteFilename, shouldThrowException, message);
    }

    protected void _putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        try
        {
            getClient().put(LOCAL_BINARY_FILE, remotePath);

            Assert.assertTrue(getClient().doesExist(FilenameUtils.getName(expectedRemoteFilename)),
                    "Expected destination " + expectedRemoteFilename + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

    @Test(dataProvider="putFileProvider", groups={"put", "upload"}, dependsOnMethods={"putFile"})
    public void putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        _putFileOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
    }

    protected void _putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        try
        {
            String remoteDir = "put/File/Outside/Home";
            getClient().mkdirs(remoteDir);

            String remoteFilePath = remoteDir + File.separator + remoteFilename;

            getClient().put(LOCAL_BINARY_FILE, remoteFilePath);

            Assert.assertTrue(getClient().doesExist(remoteDir + File.separator + FilenameUtils.getName(expectedRemoteFilename)),
                    "Expected destination " + remoteDir + File.separator + expectedRemoteFilename + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

    @Test(dataProvider="putFolderProvider", groups={"put", "upload"}, dependsOnMethods={"putFileOutsideHome"})
    public void putFolderCreatesRemoteFolder(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        _putFolderCreatesRemoteFolder(remotePath, expectedRemoteFilename, shouldThrowException, message);
    }

    protected void _putFolderCreatesRemoteFolder(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        try
        {
            getClient().put(LOCAL_DIR, remotePath);

            Assert.assertTrue(getClient().doesExist(FilenameUtils.getName(expectedRemoteFilename)),
                    "Expected destination " + expectedRemoteFilename + " does not exist after put file");

            for(File child: new File(LOCAL_DIR).listFiles()) {
                Assert.assertTrue(getClient().doesExist(expectedRemoteFilename + "/" + child.getName()),
                        "Expected uploaded folder content " + expectedRemoteFilename + "/" + child.getName() +
                        " does not exist after put file");
            }
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

    @Test(groups={"put", "upload"}, dependsOnMethods={"putFolderCreatesRemoteFolder"})
    public void putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists()
    {
        _putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists();
    }

    protected void _putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists()
    {
        try
        {
            getClient().mkdirs(LOCAL_DIR_NAME);

            getClient().put(LOCAL_DIR, LOCAL_DIR_NAME);

            Assert.assertTrue(getClient().doesExist(LOCAL_DIR_NAME + File.separator + LOCAL_DIR_NAME),
                    "Expected destination " + LOCAL_DIR_NAME + File.separator + LOCAL_DIR_NAME
                    + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

    @Test(groups={"put", "upload"}, dependsOnMethods={"putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists"})
    public void putFolderMergesContentsWhenRemoteFolderExists()
    {
        _putFolderMergesContentsWhenRemoteFolderExists();
    }

    protected void _putFolderMergesContentsWhenRemoteFolderExists()
    {
        String remoteDir = UUID.randomUUID().toString();
        try
        {
            getClient().mkdirs(remoteDir + "foo/bar");
            Assert.assertTrue(getClient().doesExist(remoteDir + "foo/bar"),
                    "Failed to create test directory for put merge test.");

            getClient().put(LOCAL_BINARY_FILE, remoteDir + "foo/file.dat");
            Assert.assertTrue(getClient().doesExist(remoteDir + "foo/file.dat"),
                    "Failed to upload test file for put merge test.");

            getClient().put(LOCAL_DIR, remoteDir);

            Assert.assertTrue(getClient().doesExist(remoteDir + "foo/bar"),
                    "Remote directory was deleted during put of folder with non-overlapping file trees.");
            Assert.assertTrue(getClient().doesExist(remoteDir + "foo/file.dat"),
                    "Remote file was was deleted during put of folder with non-overlapping file trees.");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

    @Test(groups={"put", "upload"}, dependsOnMethods={"putFolderMergesContentsWhenRemoteFolderExists"})
    public void putFileOverwritesExistingFile()
    {
        _putFileOverwritesExistingFile();
    }

    protected void _putFileOverwritesExistingFile()
    {
        String remoteName = UUID.randomUUID().toString();
        try
        {

            getClient().put(LOCAL_BINARY_FILE, remoteName);
            Assert.assertTrue(getClient().doesExist(remoteName),
                    "Failed to put file prior to overwrite test.");

            getClient().put(LOCAL_BINARY_FILE, remoteName);
            Assert.assertTrue(getClient().doesExist(remoteName),
                    "Failed to put file prior to overwrite test.");
        }
        catch (Exception e)
        {
            Assert.fail("Overwriting file should not throw exception.", e);
        }
        finally {
            try {getClient().delete(remoteName);} catch (Exception ignored) {}
        }
    }

    @Test(groups={"put", "upload"}, dependsOnMethods={"putFileOverwritesExistingFile"})
    public void putFileWithoutRemotePermissionThrowsRemoteDataException()
    {
        _putFileWithoutRemotePermissionThrowsRemoteDataException();
    }

    protected void _putFileWithoutRemotePermissionThrowsRemoteDataException()
    {
        try
        {
            getClient().put(LOCAL_BINARY_FILE, getForbiddenDirectoryPath(false));
            Assert.fail("Writing to file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (IOException | RemoteDataException e)
        {
            Assert.assertTrue(true);
        }
    }

    @Test(groups={"put", "upload"}, dependsOnMethods={"putFolderMergesContentsWhenRemoteFolderExists"})//dependsOnMethods={"putFileWithoutRemotePermissionThrowsRemoteDataException"})
    public void putFolderWithoutRemotePermissionThrowsRemoteDataException()
    {
        _putFolderWithoutRemotePermissionThrowsRemoteDataException();
    }

    protected void _putFolderWithoutRemotePermissionThrowsRemoteDataException()
    {
        try
        {
            getClient().put(LOCAL_DIR, getForbiddenDirectoryPath(false));
            Assert.fail("Writing to file or folder without permission "
                    + "on the remote system should throw RemoteDataException");
        }
        catch (IOException | RemoteDataException e)
        {
            Assert.assertTrue(true);
        }
    }

    @Test(dataProvider="putFolderProvider", groups={"put", "upload"}, dependsOnMethods= {"putFolderMergesContentsWhenRemoteFolderExists"})//dependsOnMethods={"putFolderWithoutRemotePermissionThrowsRemoteDataException"})
    public void putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        _putFolderOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
    }

    protected void _putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
    {
        try
        {
            String remoteDir = "put/File/Outside/Home/";
            getClient().mkdirs(remoteDir);

            String remoteFilePath = remoteDir + remoteFilename;

            getClient().put(LOCAL_DIR, remoteFilePath);

            Assert.assertTrue(getClient().doesExist(remoteDir + FilenameUtils.getName(expectedRemoteFilename)),
                    "Expected destination " + remoteDir + FilenameUtils.getName(expectedRemoteFilename) + " does not exist after put file");
        }
        catch (Exception e)
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }

//    @Test(dependsOnMethods={"putFolderOutsideHome"}, groups={"put", "upload"})
    @Test(groups={"put", "upload"})
    public void putFileFailsToMissingDestinationPath()
    {
        _putFileFailsToMissingDestinationPath();
    }

    protected void _putFileFailsToMissingDestinationPath()
    {
        try
        {
            getClient().put(LOCAL_BINARY_FILE, MISSING_DIRECTORY);

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
    
    @Test(dependsOnMethods={"putFileFailsToMissingDestinationPath"}, groups={"put", "upload"})
    public void putFolderFailsToMissingDestinationPath()
    {
        _putFolderFailsToMissingDestinationPath();
    }
    
    protected void _putFolderFailsToMissingDestinationPath()
    {
        try 
        {
            getClient().put(LOCAL_DIR, MISSING_DIRECTORY);
            
            Assert.fail("Put folder to a remote directory that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) 
        {
            Assert.fail("Put folder to a local directory that does not exist should throw FileNotFoundException.");
        }
    }
    
    @Test(dependsOnMethods={"putFolderFailsToMissingDestinationPath"}, groups={"put", "upload"})
    public void putFailsForMissingLocalPath()
    {
        _putFailsForMissingLocalPath();
    }
    
    protected void _putFailsForMissingLocalPath()
    {
        try 
        {
            getClient().put(MISSING_DIRECTORY, "");
            
            Assert.fail("Put on missing local folder should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) 
        {
            Assert.fail("Put on missing local folder should throw FileNotFoundException.");
        }
    }
    
    @Test(dependsOnMethods={"putFailsForMissingLocalPath"}, groups={"put", "upload"})
    public void putFolderFailsToRemoteFilePath()
    {
        _putFolderFailsToRemoteFilePath();
    }
    
    protected void _putFolderFailsToRemoteFilePath()
    {
        try 
        {
            getClient().put(LOCAL_BINARY_FILE, "");
            getClient().put(LOCAL_DIR, LOCAL_BINARY_FILE_NAME);
            
            Assert.fail("Put folder to a local directory that does not exist should throw FileNotFoundException.");
        }
        catch (IOException e) {
            Assert.fail("Put folder to path of remote file should throw RemoteDataException.", e);
        }
        catch (RemoteDataException e) 
        {
            Assert.assertTrue(true);
        }
    }
    
    @Test(groups={"delete"}, dependsOnGroups={"put"})
    public void delete()
    {
        _delete();
    }
    
    protected void _delete()
    {
        try 
        {
            getClient().put(LOCAL_TXT_FILE, "");
            
            String remoteFilename = LOCAL_TXT_FILE_NAME;
            Assert.assertTrue(getClient().doesExist(remoteFilename), 
                    "File " + remoteFilename + " not found on remote system after put.");
            getClient().delete(remoteFilename);
            Assert.assertFalse(getClient().doesExist(remoteFilename), 
                    "File " + remoteFilename + " not deleted from remote system.");
            
            getClient().put(LOCAL_DIR, "");
            
            String remoteDir = LOCAL_DIR_NAME;
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
    
    @Test(groups={"delete"}, dependsOnMethods={"delete"})
    public void deleteFailsOnMissingDirectory()
    {
        _deleteFailsOnMissingDirectory();
    }
    
    protected void _deleteFailsOnMissingDirectory()
    {
        try 
        {
            getClient().delete(MISSING_DIRECTORY);
            Assert.fail("delete should throw exception on missing directory");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Incorrect error thorwn when delete missing directory.", e);
        } 
    }
    
    @Test(groups={"delete"}, dependsOnMethods={"deleteFailsOnMissingDirectory"})
    public void deleteThrowsExceptionWhenNoPermission()
    {
        _deleteThrowsExceptionWhenNoPermission();
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
    
    @Test(groups={"is"}, dependsOnGroups={"delete"})
    public void isDirectoryTrueForDirectory()
    {
        _isDirectoryTrueForDirectory();
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
                getClient().isDirectory(MISSING_DIRECTORY);
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
    
    @Test(groups={"is"}, dependsOnMethods={"isDirectoryTrueForDirectory"})
    public void isDirectoryFalseForFile()
    {
        _isDirectoryFalseForFile();
    }
    
    protected void _isDirectoryFalseForFile()
    {
        try 
        {
            getClient().put(LOCAL_BINARY_FILE, "");
            Assert.assertFalse(getClient().isDirectory(LOCAL_BINARY_FILE_NAME), 
                    "File uploaded for this test should return false.");
        } 
        catch (Exception e) {
            Assert.fail("isDirectory should not throw unexpected exceptions", e);
        }
    }
    
    @Test(groups={"is"}, dependsOnMethods={"isDirectoryFalseForFile"})
    public void isDirectorThrowsExceptionForMissingPath()
    {
        _isDirectorThrowsExceptionForMissingPath();
    }
    
    protected void _isDirectorThrowsExceptionForMissingPath()
    {
        try 
        {
            getClient().isDirectory(MISSING_DIRECTORY);
            Assert.fail("isDirectory should throw exception when checking a non-existent path");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test(groups={"is"}, dependsOnMethods={"isDirectorThrowsExceptionForMissingPath"})
    public void isFileFalseForDirectory()
    {
        _isFileFalseForDirectory();
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
                getClient().isFile(MISSING_FILE);
                Assert.fail("Non-existent folder should throw exception");
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } 
        catch (Exception e) {
            Assert.fail("isFile should not throw unexpected exceptions", e);
        }
    }
    
    @Test(groups={"is"}, dependsOnMethods={"isFileFalseForDirectory"})
    public void isFileTrueForFile()
    {
        _isFileTrueForFile();
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
    
    @Test(groups={"is"}, dependsOnMethods={"isFileTrueForFile"})
    public void isFileThrowsExceptionForMissingPath()
    {
        _isFileThrowsExceptionForMissingPath();
    }
    
    protected void _isFileThrowsExceptionForMissingPath()
    {
        try 
        {
            getClient().isFile(MISSING_FILE);
            Assert.fail("isFile should throw exception when checking a non-existent path");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
  
    @Test(groups={"list"}, dependsOnGroups={"is"})
    public void ls()
    {
        _ls();
    }
    
    protected void _ls()
    {
        String remoteDir = null;
        try 
        {
            getClient().put(LOCAL_DIR, "");
            remoteDir = LOCAL_DIR_NAME;
            Assert.assertTrue(getClient().doesExist(remoteDir), 
                    "Directory " + remoteDir + " not found on remote system after put.");
            
            List<RemoteFileInfo> files = getClient().ls(remoteDir);
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
            Assert.fail("Failed to list contents of " + remoteDir, e);
        } 
    }
    
    @Test(groups={"list"}, dependsOnMethods={"ls"})
    public void lsFailsOnMissingDirectory()
    {
        _lsFailsOnMissingDirectory();
    }
    
    protected void _lsFailsOnMissingDirectory()
    {
        try 
        {
            getClient().ls(MISSING_DIRECTORY);
            Assert.fail("ls should throw exception on missing directory");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Incorrect error thorwn when listing missing directory.", e);
        } 
    }
    
    @Test(groups={"list"}, dependsOnMethods={"lsFailsOnMissingDirectory"})
    public void lsThrowsExceptionWhenNoPermission()
    {
        _lsThrowsExceptionWhenNoPermission();
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
    
    @Test(groups={"get", "download"}, dependsOnGroups={"list"})
    public void getThrowsExceptionOnMissingRemotePath() 
    {
        _getThrowsExceptionOnMissingRemotePath();
    }
    
    protected void _getThrowsExceptionOnMissingRemotePath()
    {
        try 
        {
            getClient().get(MISSING_DIRECTORY, getLocalDownloadDir());
            Assert.fail("Get on unknown remote path should throw FileNotFoundException.");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Get on unknown remote path should throw FileNotFoundException", e);
        }
    }
    
    @Test(groups={"get", "download"}, dependsOnMethods={"getThrowsExceptionOnMissingRemotePath"})
    public void getThrowsExceptionWhenDownloadingFolderToLocalFilePath() 
    {
        _getThrowsExceptionWhenDownloadingFolderToLocalFilePath();
    }
    
    protected void _getThrowsExceptionWhenDownloadingFolderToLocalFilePath()
    {
        try 
        {
            FileUtils.getFile(getLocalDownloadDir()).mkdirs();
            
            // copy the file so it's present to be overwritten without endangering our test data
            FileUtils.copyFileToDirectory(new File(LOCAL_BINARY_FILE), new File(getLocalDownloadDir()));
            
            getClient().put(LOCAL_DIR, "");
            getClient().get(LOCAL_DIR_NAME, 
                    getLocalDownloadDir() + File.separator + LOCAL_BINARY_FILE_NAME);
            Assert.fail("Getting remote folder to a local file path should throw RemoteDataException.");
        } 
        catch (IOException e) {
            Assert.fail("Getting remote folder to a local file path should not throw IOException.", e);
        }
        catch (RemoteDataException e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test(groups={"get", "download"}, dependsOnMethods={"getThrowsExceptionWhenDownloadingFolderToLocalFilePath"})
    public void getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath()
    {
        _getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath();
    }
    
    protected void _getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath()
    {
        try 
        {
            FileUtils.getFile(getLocalDownloadDir()).delete();
            
            getClient().put(LOCAL_DIR, "");
            
            getClient().get(LOCAL_DIR_NAME, getLocalDownloadDir() + File.separator + MISSING_DIRECTORY);
            Assert.fail("Getting remote folder to a local directory that does not exist should throw FileNotFoundException.");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Getting remote folder to a local directory that does not exist should throw FileNotFoundException.", e);
        }
    }
    
    @Test(groups={"get", "download"}, dependsOnMethods={"getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath"})
    public void getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath() 
    {
        _getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath();
    }
    
    protected void _getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath()
    {
        try 
        {
            FileUtils.getFile(getLocalDownloadDir()).delete();
            
            getClient().put(LOCAL_BINARY_FILE, "");
            
            getClient().get(LOCAL_BINARY_FILE_NAME, getLocalDownloadDir() + File.separator + MISSING_DIRECTORY);
            Assert.fail("Getting remote file to a local directory that does not exist should throw FileNotFoundException.");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Getting remote folder to a local directory that does not exist should throw FileNotFoundException.", e);
        }
    }
    
    @Test(groups={"get", "download"}, dataProvider="getDirectoryRetrievesToCorrectLocationProvider", dependsOnMethods={"getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath"})
    public void getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message) 
    {
        _getDirectoryRetrievesToCorrectLocation(localdir, createTestDownloadFolder, expectedDownloadPath, message);
    }
    
    protected void _getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message)
    {
        String remotePutPath = null;
        File testDownloadPath = FileUtils.getFile(FileUtils.getTempDirectory() + "/" + localdir);
        File testExpectedDownloadPath = FileUtils.getFile(FileUtils.getTempDirectory() + "/" + expectedDownloadPath);
        try 
        {
            if (createTestDownloadFolder) {
                testDownloadPath.mkdirs();
            } else {
                testDownloadPath.getParentFile().mkdirs();
            }
            
            remotePutPath = LOCAL_DIR_NAME;
            
            getClient().put(LOCAL_DIR, "");
            
            Assert.assertTrue(getClient().doesExist(remotePutPath), "Data not found on remote system after put.");
            
            getClient().get(remotePutPath, testDownloadPath.getAbsolutePath());
            Assert.assertTrue(testExpectedDownloadPath.exists(), message);
            
            for(File localFile: FileUtils.getFile(LOCAL_DIR).listFiles()) {
                if (!localFile.getName().equals(".") && !localFile.getName().equals(".."))
                    Assert.assertTrue(new File(testExpectedDownloadPath, localFile.getName()).exists(), 
                            "Data not found on local system after get.");
            }
        } 
        catch (Exception e) {
            Assert.fail("get should not throw unexpected exception", e);
        }
        finally {
            FileUtils.deleteQuietly(testDownloadPath);
            FileUtils.deleteQuietly(testExpectedDownloadPath);
        }
    }
    
    @Test(groups={"get", "download"}, dataProvider="getFileRetrievesToCorrectLocationProvider", dependsOnMethods={"getDirectoryRetrievesToCorrectLocation"})
    public void getFileRetrievesToCorrectLocation(String localPath, String expectedDownloadPath, String message) 
    {
        _getFileRetrievesToCorrectLocation(localPath, expectedDownloadPath, message);
    }
    
    protected void _getFileRetrievesToCorrectLocation(String localPath, String expectedDownloadPath, String message)
    {
        String remotePutPath = null;
        try 
        {
            FileUtils.getFile(getLocalDownloadDir()).mkdirs();
            
            getClient().put(LOCAL_BINARY_FILE, "");
            remotePutPath = LOCAL_BINARY_FILE_NAME;
            Assert.assertTrue(getClient().doesExist(remotePutPath), "Data not found on remote system after put.");
            
            getClient().get(remotePutPath, localPath);
            Assert.assertTrue(FileUtils.getFile(expectedDownloadPath).exists(), message);           
        } 
        catch (Exception e) {
            Assert.fail("get should not throw unexpected exception", e);
        }
    }
    
    @Test(groups={"get", "download"}, dependsOnMethods={"getFileRetrievesToCorrectLocation"})
    public void getFileOverwritesExistingFile() 
    {
        _getFileOverwritesExistingFile();
    }
    
    protected void _getFileOverwritesExistingFile()
    {
        String remotePutPath = null;
        try 
        {
            File downloadDir = FileUtils.getFile(getLocalDownloadDir());
            Assert.assertTrue(downloadDir.mkdirs(), "Failed to create download directory");
            
            // copy the file so it's present to be overwritten without endangering our test data
            FileUtils.copyFileToDirectory(new File(LOCAL_BINARY_FILE), new File(getLocalDownloadDir()));
            File downloadFile = new File(downloadDir, LOCAL_BINARY_FILE_NAME);
            
            remotePutPath = LOCAL_BINARY_FILE_NAME;
            getClient().put(LOCAL_BINARY_FILE, "");
            Assert.assertTrue(getClient().doesExist(remotePutPath), "Data not found on remote system after put.");
            
            getClient().get(remotePutPath, downloadFile.getAbsolutePath());
            Assert.assertTrue(
                    downloadFile.exists(), 
                    "Getting remote file should overwrite local file if it exists.");
        } 
        catch (Exception e) {
            Assert.fail("Overwriting local file on get should not throw unexpected exception", e);
        }
    }
    
    @Test(groups={"stream","put"})
    public void getOutputStream()
    {
        _getOutputStream();
    }

    protected void _getOutputStream()
    {
        OutputStream out = null;
        BufferedInputStream in = null;
        String remotePath = null;
        try
        {
            remotePath = LOCAL_BINARY_FILE_NAME;
            out = getClient().getOutputStream(remotePath, true, false);
            in = new BufferedInputStream(new FileInputStream(LOCAL_BINARY_FILE));
            Assert.assertTrue(IOUtils.copy(in, out) > 0, "Zero bytes were copied to remote output stream.");
            out.flush();
            in.close();
            out.close();

            Assert.assertTrue(getClient().doesExist(remotePath),
                    "Data not found on remote system after writing via output stream.");

            Assert.assertTrue(getClient().isFile(remotePath),
                    "Data found to be a directory on remote system after writing via output stream.");
        }
        catch (Throwable e) {
            Assert.fail("Writing to output stream threw unexpected exception", e);
        }
        finally {
            try { in.close(); } catch (Exception ignored) {}
            try { out.close(); } catch (Exception ignored) {}
        }
    }

    @Test(groups={"stream","put"}, dependsOnMethods={"getOutputStream"})
    public void getOutputStreamFailsWhenRemotePathIsNullOrEmpty()
    {
        _getOutputStreamFailsWhenRemotePathIsNullOrEmpty();
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

    @Test(groups={"stream","put"}, dependsOnMethods={"getOutputStreamFailsWhenRemotePathIsNullOrEmpty"})
    public void getOutputStreamFailsWhenRemotePathIsDirectory()
    {
        _getOutputStreamFailsWhenRemotePathIsDirectory();
    }

    protected void _getOutputStreamFailsWhenRemotePathIsDirectory()
    {
        OutputStream out = null;
        try
        {
            getClient().mkdir(LOCAL_DIR_NAME);
            out = getClient().getOutputStream(LOCAL_DIR_NAME, true, false);
            Assert.fail("Passing valid directory to getOutputStream should throw exception.");
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

    @Test(groups={"stream","put"}, dependsOnMethods={"getOutputStreamFailsWhenRemotePathIsDirectory"})
    public void getOutputStreamFailsOnMissingPath()
    {
        _getOutputStreamFailsOnMissingPath();
    }

    protected void _getOutputStreamFailsOnMissingPath()
    {
        try
        {
            getClient().getOutputStream(MISSING_DIRECTORY + "/" + LOCAL_TXT_FILE_NAME, true, false);
            Assert.fail("getOutputStream should throw FileNotFoundException on missing directory");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("getOutputStream should throw FileNotFoundException on missing directory", e);
        }
    }

    @Test(groups={"stream","put"}, dependsOnMethods={"getOutputStreamFailsOnMissingPath"})
    public void getOutputStreamThrowsExceptionWhenNoPermission()
    {
        _getOutputStreamThrowsExceptionWhenNoPermission();
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
    }

    @DataProvider(name="getInputStreamProvider", parallel=false)
    protected Object[][] getInputStreamProvider()
    {
        return new Object[][] {
                { "", "", true, "empty localfile to get should throw exception." },
                { null, "", true, "null localfile to get should throw exception." },
                { "", null, true, "null remotedir to get should throw exception." },
                { LOCAL_TXT_FILE, MISSING_DIRECTORY, true, "get on missing remote file should throw exception." },
                { LOCAL_TXT_FILE, "", false, "get local file from remote home directory should succeed." },
        };
    }

    @Test(groups={"stream", "get"}, dataProvider="getInputStreamProvider", dependsOnGroups= {"download", "put"})
    public void getInputStream(String localFile, String remotedir, boolean shouldThrowException, String message)
    {
        _getInputStream(localFile, remotedir, shouldThrowException, message);
    }

    protected void _getInputStream(String localFile, String remotedir, boolean shouldThrowException, String message)
    {
        boolean actuallyThrewException = false;
        InputStream in = null;
        BufferedOutputStream bout = null;
        String remotePutPath = null;
        try
        {
            getClient().put(localFile, remotedir);
            if (StringUtils.isEmpty(remotedir)) {
                remotePutPath = FilenameUtils.getName(localFile);
            } else {
                remotePutPath = remotedir + "/" + FilenameUtils.getName(localFile);
            }
            String localGetPath = getLocalDownloadDir() + "/" + FilenameUtils.getName(remotePutPath);
            Assert.assertTrue(getClient().doesExist(remotePutPath), "Data not found on remote system after put.");

            in = getClient().getInputStream(remotePutPath, true);
            File downloadfile = new File(localGetPath);
            if (!org.codehaus.plexus.util.FileUtils.fileExists(downloadfile.getParent())) {
                FileUtils.getFile(downloadfile.getParent()).mkdirs();
            }
            bout = new BufferedOutputStream(new FileOutputStream(downloadfile));

            int bufferSize = getClient().getMaxBufferSize();
            byte[] b = new byte[bufferSize];
            int len = 0;

            while ((len = in.read(b)) > -1) {
                bout.write(b, 0, len);
            }

            bout.flush();

            Assert.assertTrue(org.codehaus.plexus.util.FileUtils.fileExists(localGetPath), "Data not found on local system after get.");

        }
        catch (Exception e) {
            actuallyThrewException = true;
            if (!shouldThrowException) e.printStackTrace();
        }
        finally {
            try { in.close(); } catch (Exception ignored) {}
            try { bout.close(); } catch (Exception ignored) {}
        }

        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }

    @Test(groups={"stream", "get"}, dependsOnMethods= {"getInputStream"})
    public void getInputStreamThrowsExceptionWhenNoPermission()
    {
        _getInputStreamThrowsExceptionWhenNoPermission();
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

    @Test(groups={"checksum"}, dependsOnGroups={"stream"})
    public void checksumDirectoryFails()
    {
        _checksumDirectoryFails();
    }
    
    protected void _checksumDirectoryFails()
    {
        try 
        {
            getClient().put(LOCAL_DIR, "");
            Assert.assertTrue(getClient().doesExist(LOCAL_DIR_NAME), 
                    "Data not found on remote system after put.");
            getClient().checksum(LOCAL_DIR_NAME);
            Assert.fail("Checksum is not supported on directories.");
        } 
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test(groups={"checksum"}, dependsOnMethods={"checksumDirectoryFails"})
    public void checksumMissingPathThrowsException()
    {
        _checksumMissingPathThrowsException();
    }
    
    protected void _checksumMissingPathThrowsException()
    {
        try 
        {
            getClient().checksum(MISSING_DIRECTORY);
            Assert.fail("Checksum a missing path should throw a FileNotFoundException.");
        } 
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        } 
        catch (Exception e) {
            Assert.fail("Checksum a missing path should throw a FileNotFoundException.");
        }
    }

    @Test(groups={"checksum"}, dependsOnMethods={"checksumMissingPathThrowsException"})
    public void checksum()
    {
        _checksum();
    }
    
    protected void _checksum()
    {
        try 
        {
            getClient().put(LOCAL_TXT_FILE, "");
            Assert.assertTrue(getClient().doesExist(LOCAL_TXT_FILE_NAME), 
                    "Data not found on remote system after put.");
        
            getClient().checksum(LOCAL_TXT_FILE_NAME);
            Assert.fail("Checksum should throw a NotImplementedException unless overridden by a concrete test class");
        } 
        catch (NotImplementedException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Checksum should throw a NotImplementedException for " +
                    system.getStorageConfig().getProtocol().toString(), e);
        }
    }
    
    @Test(dataProvider="doRenameProvider", groups={"rename"}, dependsOnGroups= {"checksum"})
    public void doRename(String oldpath, String newpath, boolean shouldThrowException, String message)
    {
        _doRename(oldpath, newpath, shouldThrowException, message);
    }
    
    protected void _doRename(String oldpath, String newpath, boolean shouldThrowException, String message)
    {
        boolean actuallyThrewException = false;
        
        try 
        {
            getClient().mkdirs(oldpath);
            Assert.assertTrue(getClient().doesExist(oldpath), "Source folder not created for rename test.");
            
            getClient().doRename(oldpath, newpath);
            Assert.assertTrue(getClient().doesExist(newpath), "Rename operation did not rename the file or directory to " + newpath);
            
        } 
        catch (Exception e) {
            actuallyThrewException = true;
            if (!shouldThrowException) 
                Assert.fail(message, e);
        }
        
        Assert.assertEquals(actuallyThrewException, shouldThrowException, message);
    }
    
    @Test(groups={"rename"}, dependsOnMethods= {"doRename"})
    public void doRenameThrowsRemotePermissionExceptionToRestrictedSource()
    {
        _doRenameThrowsRemotePermissionExceptionToRestrictedSource();
    }
    
    protected void _doRenameThrowsRemotePermissionExceptionToRestrictedSource()
    {
        try 
        {
            getClient().doRename(getForbiddenDirectoryPath(true), "foo");
            
            Assert.fail("Rename a restricted remote path to a dest path should throw RemoteDataException.");
        }
        catch (RemoteDataException | FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("Rename a restricted remote path to a dest path should throw RemoteDataException or FileNotFoundException.", e);
        }
    }
    
    @Test(groups={"rename"}, dependsOnMethods= {"doRenameThrowsRemotePermissionExceptionToRestrictedSource"})
    public void doRenameThrowsRemotePermissionExceptionToRestrictedDest()
    {
        _doRenameThrowsRemotePermissionExceptionToRestrictedDest();
    }
    
    protected void _doRenameThrowsRemotePermissionExceptionToRestrictedDest() 
    {
        try 
        {
            getClient().mkdir("foo");
            getClient().doRename("foo", getForbiddenDirectoryPath(true));
            
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
    }
    
    @Test(groups={"rename"}, dependsOnMethods= {"doRenameThrowsRemotePermissionExceptionToRestrictedDest"})
    public void doRenameThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        _doRenameThrowsFileNotFoundExceptionOnMissingDestPath();
    }
    
    protected void _doRenameThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        try 
        {
            getClient().mkdir("foo");
            getClient().doRename("foo", MISSING_DIRECTORY);
            
            Assert.fail("Rename a remote source path to a dest path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) 
        {
            Assert.fail("Rename a remote source path to a dest path that does not exist should throw FileNotFoundException.", e);
        }
    }
    
    @Test(groups={"rename"}, dependsOnMethods= {"doRenameThrowsFileNotFoundExceptionOnMissingDestPath"})
    public void doRenameThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        _doRenameThrowsFileNotFoundExceptionOnMissingSourcePath();
    }
    
    protected void _doRenameThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        try 
        {
            getClient().doRename(MISSING_DIRECTORY, "foo");
            
            Assert.fail("Rename a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) 
        {
            Assert.fail("Rename a remote source path that does not exist should throw FileNotFoundException.", e);
        }
    }

    @Test(enabled=false)
    public void getUrlForPath()
    {
        _getUrlForPath();
    }
    
    protected void _getUrlForPath() {}
    
    @Test(groups={"copy"}, dependsOnGroups={"rename"})
    public void copyFile()
    {
        _copyFile();
    }
    
    protected void _copyFile()
    {
        String remoteSrc = LOCAL_BINARY_FILE_NAME;
        String remoteDest = LOCAL_BINARY_FILE_NAME + ".copy";
        
        try
        {
            getClient().put(LOCAL_BINARY_FILE, "");
            Assert.assertTrue(getClient().doesExist(remoteSrc), "Failed to upload source file");
            
            getClient().copy(remoteSrc, remoteDest);
            
            Assert.assertTrue(getClient().doesExist(remoteDest), "Copy operation failed from " + 
                    remoteSrc + " to " + remoteDest);
        }
        catch (Exception e) {
            Assert.fail("Copy operation failed from " + 
                remoteSrc + " to " + remoteDest, e);
        }
    }
    
    @Test(groups={"copy"}, dataProvider="copyIgnoreSlashesProvider", dependsOnMethods= {"copyFile"})
    public void copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message)
    {
        _copyDir(src, dest, createDest, expectedPath, shouldThrowException, message);
    }
    
    protected void _copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message)
    {
        try
        {
            getClient().mkdir(src);
            Assert.assertTrue(getClient().doesExist(src), "Failed to create source directory");
            
            if (createDest) {
                getClient().mkdir(dest);
                Assert.assertTrue(getClient().doesExist(dest), "Failed to create dest directory");
            }
            
            getClient().copy(src, dest);
            
            Assert.assertTrue(getClient().doesExist(expectedPath), message);
        }
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
    }
    
    @Test(groups={"copy"}, dependsOnMethods={"copyDir"})
    public void copyThrowsRemoteDataExceptionToRestrictedDest()
    {
        _copyThrowsRemoteDataExceptionToRestrictedDest();
    }
    
    protected void _copyThrowsRemoteDataExceptionToRestrictedDest()
    {
        try 
        {
            getClient().mkdir("foo");
            getClient().copy("foo", getForbiddenDirectoryPath(true));
               
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
    }
   
    @Test(groups={"copy"}, dependsOnMethods={"copyThrowsRemoteDataExceptionToRestrictedDest"})
    public void copyThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        _copyThrowsFileNotFoundExceptionOnMissingDestPath();
        
    }
    
    protected void _copyThrowsFileNotFoundExceptionOnMissingDestPath()
    {
        try 
        {
            getClient().mkdir("foo");
            getClient().copy("foo", MISSING_DIRECTORY);
               
            Assert.fail("copy a remote source path to a dest path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path to a dest path that does not exist should throw FileNotFoundException.", e);
        }
    }
    
    @Test(groups={"copy"}, dependsOnMethods={"copyThrowsFileNotFoundExceptionOnMissingDestPath"})
    public void copyThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        _copyThrowsFileNotFoundExceptionOnMissingSourcePath();
    }
    
    protected void _copyThrowsFileNotFoundExceptionOnMissingSourcePath()
    {
        try 
        {
            getClient().copy(MISSING_DIRECTORY, "foo");
               
            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.", e);
        }
    }
   
    @Test(groups={"copy"}, dependsOnMethods= {"copyThrowsFileNotFoundExceptionOnMissingSourcePath"})
    public void copyThrowsRemoteDataExceptionToRestrictedSource()
    {
        _copyThrowsRemoteDataExceptionToRestrictedSource();
    }
    
    protected void _copyThrowsRemoteDataExceptionToRestrictedSource() {
        try 
        {
            getClient().copy(MISSING_DIRECTORY, "foo");
               
            Assert.fail("copy a remote source path that does not exist should throw FileNotFoundException.");
        }
        catch (FileNotFoundException | RemoteDataException e) {
            Assert.assertTrue(true);
        }
        catch (Exception e) {
            Assert.fail("copy a remote source path that does not exist should throw RemoteDataException or FileNotFoundException.", e);
        }
    }
           
    @Test(groups={"sync"}, dependsOnGroups= {"copy"})
    public void syncToRemoteFile() 
    {
        _syncToRemoteFile();
    }
    
    protected void _syncToRemoteFile()
    {
        try 
        {
            tmpFile = File.createTempFile("syncToRemoteFile", "txt");
            org.apache.commons.io.FileUtils.writeStringToFile(tmpFile, "This should be overwritten.");
            
//            TransferTask task = new TransferTask(
//                    tmpFile.toURI().toString(), 
//                    getClient().getUriForPath(LOCAL_BINARY_FILE_NAME).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(tmpFile.getAbsolutePath(), LOCAL_BINARY_FILE_NAME, new RemoteTransferListener(task));
            getClient().syncToRemote(tmpFile.getAbsolutePath(), LOCAL_BINARY_FILE_NAME,null);
            Assert.assertTrue(getClient().doesExist(LOCAL_BINARY_FILE_NAME), 
                    "Remote file was not created during sync");
            
            Assert.assertEquals(getClient().length(LOCAL_BINARY_FILE_NAME), tmpFile.length(),
                    "Remote file was not copied in whole created during sync");
            
//            TransferTask task2 = new TransferTask(
//                    new File(LOCAL_BINARY_FILE).toURI().toString(), 
//                    getClient().getUriForPath(LOCAL_BINARY_FILE_NAME).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task2);
//            
//            getClient().syncToRemote(LOCAL_BINARY_FILE, LOCAL_BINARY_FILE_NAME, new RemoteTransferListener(task2));
            getClient().syncToRemote(LOCAL_BINARY_FILE, LOCAL_BINARY_FILE_NAME,null);
            Assert.assertNotEquals(getClient().length(LOCAL_BINARY_FILE_NAME), tmpFile.length(),
                    "Remote file size should not match temp file after syncing.");
            
            Assert.assertEquals(getClient().length(LOCAL_BINARY_FILE_NAME), new File(LOCAL_BINARY_FILE).length(),
                    "Remote file size should match local binary file after syncing.");
        } 
        catch (Exception e) 
        {
            Assert.fail("Put of file or folder should not throw exception.", e);
        }
    }
    
    @Test(groups={"sync"}, dependsOnMethods={"syncToRemoteFile"})
    public void syncToRemoteFolderAddsMissingFiles() 
    {
        _syncToRemoteFolderAddsMissingFiles();
    }
    
    protected void _syncToRemoteFolderAddsMissingFiles()
    {
        try 
        {
            File localDir = new File(LOCAL_DIR);
            getClient().mkdir(localDir.getName());
            
            Assert.assertTrue(getClient().doesExist(localDir.getName()), 
                    "Remote directory was not created. Test will fail");
            
//            TransferTask task = new TransferTask(
//                    localDir.toURI().toString(), 
//                    getClient().getUriForPath(localDir.getName() + "/" + localDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(LOCAL_DIR, "", new RemoteTransferListener(task));
            getClient().syncToRemote(LOCAL_DIR, "", null);
            Assert.assertFalse(getClient().doesExist(localDir.getName() + "/" + localDir.getName()),
                    "Local directory was synced as a subfolder instead of overwriting");
            
            List<RemoteFileInfo> remoteListing = getClient().ls(LOCAL_DIR_NAME);
            
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
    }
    
    @Test(groups={"sync"}, dependsOnMethods={"syncToRemoteFolderAddsMissingFiles"})
    public void syncToRemoteFolderLeavesExistingFilesUntouched() 
    {
        _syncToRemoteFolderLeavesExistingFilesUntouched();
    }
    
    protected void _syncToRemoteFolderLeavesExistingFilesUntouched()
    {
        try 
        {
            File localDir = new File(LOCAL_DIR);
            getClient().put(LOCAL_DIR, "");
            
            Assert.assertTrue(getClient().doesExist(localDir.getName()), 
                    "Remote directory was not uploaded. Test will fail");
            
            Map<String, Date> lastUpdatedMap = new HashMap<String,Date>();
            List<RemoteFileInfo> remoteListing = getClient().ls(LOCAL_DIR_NAME);
            for (RemoteFileInfo fileInfo: remoteListing) 
            {   
                lastUpdatedMap.put(fileInfo.getName(), fileInfo.getLastModified());
            }
            
//            TransferTask task = new TransferTask(
//                    localDir.toURI().toString(), 
//                    getClient().getUriForPath(localDir.getName() + "/" + localDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(LOCAL_DIR, "", new RemoteTransferListener(task));
            getClient().syncToRemote(LOCAL_DIR, "", null);
            Assert.assertFalse(getClient().doesExist(localDir.getName() + "/" + localDir.getName()),
                    "Local directory was synced as a subfolder instead of overwriting");
            
            remoteListing = getClient().ls(LOCAL_DIR_NAME);
            
            Assert.assertEquals(localDir.list().length, remoteListing.size(), 
                    "Mismatch between file count locally and remote.");
                    
            for (RemoteFileInfo fileInfo: remoteListing) 
            {
                File localFile = new File(LOCAL_DIR, fileInfo.getName());
                
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
    }
    
    @Test(groups={"sync"}, dependsOnMethods={"syncToRemoteFolderLeavesExistingFilesUntouched"})
    public void syncToRemoteFolderOverwritesFilesWithDeltas() 
    {
        _syncToRemoteFolderOverwritesFilesWithDeltas();
    }
    
    protected void _syncToRemoteFolderOverwritesFilesWithDeltas()
    {
        try 
        {
            File localDir = new File(LOCAL_DIR);
            getClient().mkdir(localDir.getName());
            
            Assert.assertTrue(getClient().doesExist(localDir.getName()), 
                    "Remote directory was not created. Test will fail");
            
            tmpFile = File.createTempFile("syncToRemoteFile", "txt");
            org.apache.commons.io.FileUtils.writeStringToFile(tmpFile, "This should be overwritten.");
            
            String remoteFileToOverwrightString = localDir.getName() + "/" + LOCAL_BINARY_FILE_NAME;
            getClient().put(tmpFile.getAbsolutePath(), remoteFileToOverwrightString);
            
            Assert.assertTrue(getClient().doesExist(remoteFileToOverwrightString), 
                    "Remote file was not created during sync");
            
            Assert.assertEquals(getClient().length(remoteFileToOverwrightString), tmpFile.length(),
                    "Remote file was not copied in whole created during sync");
            
//            TransferTask task = new TransferTask(
//                    localDir.toURI().toString(), 
//                    getClient().getUriForPath(localDir.getName() + "/" + localDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(LOCAL_DIR, "", new RemoteTransferListener(task));
            getClient().syncToRemote(LOCAL_DIR, "", null);
            Assert.assertFalse(getClient().doesExist(localDir.getName() + "/" + localDir.getName()),
                    "Local directory was synced as a subfolder instead of overwriting");
            
            List<RemoteFileInfo> remoteListing = getClient().ls(LOCAL_DIR_NAME);
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
    }
    
    @Test(groups={"sync"}, dependsOnMethods={"syncToRemoteFolderOverwritesFilesWithDeltas"})
    public void syncToRemoteFolderReplacesOverlappingFilesAndFolders() 
    {
        _syncToRemoteFolderReplacesOverlappingFilesAndFolders();
    }
    
    protected void _syncToRemoteFolderReplacesOverlappingFilesAndFolders()
    {
        try 
        {
            File localDir = new File(LOCAL_DIR);
            getClient().mkdir(localDir.getName());
            
            Assert.assertTrue(getClient().doesExist(localDir.getName()), 
                    "Remote directory was not created. Test will fail");
            
            String remoteFolderToOverwrightString = localDir.getName() + "/" + LOCAL_BINARY_FILE_NAME;
            getClient().mkdir(remoteFolderToOverwrightString);
            
            Assert.assertTrue(getClient().doesExist(remoteFolderToOverwrightString), 
                    "Remote subdirectory was not created during sync");
            
//            TransferTask task = new TransferTask(
//                    localDir.toURI().toString(), 
//                    getClient().getUriForPath(localDir.getName() + "/" + localDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(LOCAL_DIR, "", new RemoteTransferListener(task));
            getClient().syncToRemote(LOCAL_DIR, "", null);
            List<RemoteFileInfo> remoteListing = getClient().ls(LOCAL_DIR_NAME);
            
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
    }
    
    @Test(groups={"sync"}, dependsOnMethods= {"syncToRemoteFolderReplacesOverlappingFilesAndFolders"})
    public void syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException {
        _syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem();
    }
    
    protected void _syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException {
        try 
        {
            tmpFile = new File(tempDir, "temp.txt");
            org.apache.commons.io.FileUtils.writeStringToFile(tmpFile, "This should be overwritten.");
            
            String[] tempSubdirectories = {
                    "foo",
                    "foo/alpha",
                    "foo/alpha/gamma",
                    "bar",
                    "bar/beta",
            };
            
            String tempLocalDir = Settings.TEMP_DIRECTORY + "/deleteme-" + System.currentTimeMillis();
            tempDir = new File(tempLocalDir);
            tempDir.mkdirs();
            
            for (String path: tempSubdirectories) {
                FileUtils.copyFile(tmpFile, new File(tempLocalDir + "/" + path + "/file1.txt"));
                FileUtils.copyFile(tmpFile, new File(tempLocalDir + "/" + path + "/file2.txt"));
            }
            
            getClient().mkdirs(tempDir.getName() + "/foo/alpha/gamma");
            getClient().mkdirs(tempDir.getName() + "/bar/beta");
            
//            TransferTask task = new TransferTask(
//                    tempDir.toURI().toString(), 
//                    getClient().getUriForPath(tempDir.getName() + "/" + tempDir.getName()).toString(),
//                    SYSTEM_USER, null, null);
//            
//            TransferTaskDao.persist(task);
//            
//            getClient().syncToRemote(tempLocalDir, "", new RemoteTransferListener(task));
            getClient().syncToRemote(tempLocalDir, "", null);
            Assert.assertFalse(getClient().doesExist(tempDir.getName() + "/" + tempDir.getName()), 
                    "Syncing put the local dir into the remote directory instead of overwriting");
            
            for (String path: tempSubdirectories) 
            {
                Assert.assertFalse(getClient().doesExist(tempDir.getName() + "/" + path + "/" + FilenameUtils.getName(path)), 
                    "Syncing put the local subdirectory into the remote subdirectory instead of overwriting");
                Assert.assertTrue(getClient().doesExist(tempDir.getName() + "/" + path + "/file1.txt"),
                    "Failed to copy local file1.txt to remote folder in proper place.");
                Assert.assertTrue(getClient().doesExist(tempDir.getName() + "/" + path + "/file2.txt"),
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
    }
    
   
    
}

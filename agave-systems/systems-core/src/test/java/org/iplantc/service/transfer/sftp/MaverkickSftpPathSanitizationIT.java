/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.AbstractPathSanitizationTest;
import org.iplantc.service.transfer.IPathSanitizationTest;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.TransferTestRetryAnalyzer;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.UUID;

/**
 * @author dooley
 *
 */
@Test(groups= {"sftp","sftp.path.sanitization"})
public class MaverkickSftpPathSanitizationIT extends AbstractPathSanitizationTest implements IPathSanitizationTest {

    private static final Logger log = Logger.getLogger(AbstractPathSanitizationTest.class);

//    @BeforeClass(alwaysRun=true)
//    protected void beforeSubclass() throws Exception {
//        super.beforeClass();
//
//        JSONObject json = getSystemJson();
//        json.remove("id");
//        json.put("id", this.getClass().getSimpleName());
//        system = (StorageSystem)StorageSystem.fromJSON(json);
//        system.setOwner(SYSTEM_USER);
//        String homeDir = system.getStorageConfig().getHomeDir();
//        homeDir = StringUtils.isEmpty(homeDir) ? "" : homeDir;
//        system.getStorageConfig().setHomeDir( homeDir + "/" + getClass().getSimpleName());
//        storageConfig = system.getStorageConfig();
//        salt = system.getSystemId() + storageConfig.getHost() +
//                storageConfig.getDefaultAuthConfig().getUsername();
//
//        SystemDao dao = Mockito.mock(SystemDao.class);
//        Mockito.when(dao.findBySystemId(Mockito.anyString()))
//                .thenReturn(system);
//    }

//    @BeforeMethod(alwaysRun=true)
//    protected void beforeMethod(Method m) throws Exception
//    {
//        String resolvedHomeDir = "";
//        try {
//            // auth client and ensure test directory is present
//            getClient().authenticate();
//        } catch (RemoteDataException e) {
//            log.error(e.getMessage());
//        }
//
//        try {
//            resolvedHomeDir = getClient().resolvePath("");
//
//            if (!getClient().mkdirs("")) {
//                if (!getClient().isDirectory("")) {
//                    Assert.fail("System home directory " + resolvedHomeDir + " exists, but is not a directory.");
//                }
//            }
//        } catch (IOException | RemoteDataException | AssertionError e) {
//            throw e;
//        } catch (Exception e) {
//            Assert.fail("Failed to create home directory " + resolvedHomeDir + " before test method.", e);
//        }
//    }
//
//    @Override
//    protected RemoteDataClient getClient()
//    {
//        RemoteDataClient client;
//        try {
//            AuthConfig userAuthConfig = system.getStorageConfig().getDefaultAuthConfig();
//            String salt = system.getSystemId() + system.getStorageConfig().getHost() + userAuthConfig.getUsername();
//            String username = userAuthConfig.getUsername();
//            String password = userAuthConfig.getClearTextPassword(salt);
//            String host = system.getStorageConfig().getHost();
//            int port = system.getStorageConfig().getPort();
//            String rootDir = system.getStorageConfig().getRootDir();
//            String threadHomeDir = String.format("%s/thread-%s-%d",
//                    system.getStorageConfig().getHomeDir(),
//                    UUID.randomUUID().toString(),
//                    Thread.currentThread().getId());
//            client = new MaverickSFTP(host, port, username, password, rootDir, threadHomeDir);
//            threadClient.set(client);
//        } catch (EncryptionException e) {
//            Assert.fail("Failed to get client", e);
//        }
//
//        return threadClient.get();
//    }

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.AbstractPathSanitizationTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp.example.com.json");
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirSanitizesSingleSpecialCharacterRelativePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        String relativePath = UUID.randomUUID().toString() + "/";

        _mkDirsSanitizationTest(relativePath + filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirSanitizesSingleSpecialCharacterAbsolutePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        String absolutePath = system.getStorageConfig().getHomeDir() +
                "/thread-" + Thread.currentThread().getId() +
                "/" + UUID.randomUUID().toString() + "/";

        _mkDirsSanitizationTest(absolutePath + filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesRepeatedSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirSanitizesRepeatedSpecialCharacter(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        _mkDirSanitizationTest(filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesWhitespaceProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirSanitizesWhitespace(String filename, String message)  throws IOException, RemoteDataException
    {
        _mkDirSanitizationTest(filename, false, message);
    }

    @Override
    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirsSanitizesSingleSpecialCharacterRelativePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        _mkDirsSanitizationTest(filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirsSanitizesSingleSpecialCharacterAbsolutePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        _mkDirsAbsolutePathSanitizationTest(filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesRepeatedSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirsSanitizesRepeatedSpecialCharacter(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        _mkDirsSanitizationTest(filename, shouldSucceed, message);
    }
}

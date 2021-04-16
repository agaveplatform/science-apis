/**
 * 
 */
package org.iplantc.service.transfer.irods4;

import org.apache.log4j.Logger;
import org.iplantc.service.transfer.AbstractPathSanitizationTest;
import org.iplantc.service.transfer.IPathSanitizationTest;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.TransferTestRetryAnalyzer;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

/**
 * @author dooley
 *
 */
@Test(groups={"irods4","irods4.path.sanitization"})
public class Irods4PathSanitizationIT extends AbstractPathSanitizationTest implements IPathSanitizationTest {

    private static final Logger log = Logger.getLogger(Irods4PathSanitizationIT.class);

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.AbstractPathSanitizationTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods4.example.com.json");
    }

//    @BeforeClass
//    @Override
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
//
//        log.debug("Pausing to allow irods4 to catchup.");
//        Thread.sleep(3000);
//
//        try {
//            RemoteDataClient client = system.getRemoteDataClient();
//            client.authenticate();
//            client.delete("/testuser/" + getClass().getSimpleName());
//        } catch (Exception e) {
//            log.debug("IRODS4 home directory " + system.getStorageConfig().getHomeDir() +
//                    " not present at start of test.");
//        }
//    }



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

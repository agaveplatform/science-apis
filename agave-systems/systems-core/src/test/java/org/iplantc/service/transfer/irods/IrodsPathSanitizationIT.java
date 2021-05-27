/**
 * 
 */
package org.iplantc.service.transfer.irods;

import org.iplantc.service.transfer.AbstractPathSanitizationTest;
import org.iplantc.service.transfer.IPathSanitizationTest;
import org.iplantc.service.transfer.TransferTestRetryAnalyzer;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

/**
 * @author dooley
 *
 */
@Test(singleThreaded = true, groups={"irods3","irods3.path.sanitization"}, threadPoolSize = 2)
public class IrodsPathSanitizationIT extends AbstractPathSanitizationTest implements IPathSanitizationTest {

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.AbstractPathSanitizationTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods3.example.com.json");
    }

    /**
     * Calls to parent #mkDirSanitizesSingleSpecialCharacterProvider() data provider so the default
     * parallel annotation will not be honored. For the irods3 container, this particular test seems to
     * run fast enough to cause race conditions that cause false failures.
     *
     * @return
     * @throws Exception
     */
    @DataProvider(parallel = false)
    protected Object[][] altMkDirSanitizesSingleSpecialCharacterProvider() throws Exception {
        return super.mkDirSanitizesSingleSpecialCharacterProvider();
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="altMkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class, threadPoolSize = 2)
    public void mkDirSanitizesSingleSpecialCharacterRelativePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        String relativePath = UUID.randomUUID() + "/";

        _mkDirsSanitizationTest(relativePath + filename, shouldSucceed, message);
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirSanitizesSingleSpecialCharacterAbsolutePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        String absolutePath = system.getStorageConfig().getHomeDir() +
                "/thread-" + Thread.currentThread().getId() +
                "/" + UUID.randomUUID() + "/";

        _mkDirsSanitizationTest(absolutePath + filename, shouldSucceed, message);
    }

    @DataProvider(parallel=false)
    protected Object[][] altMkDirSanitizesRepeatedSpecialCharacterProvider() throws Exception {
        return super.mkDirSanitizesRepeatedSpecialCharacterProvider();
    }

    @Override
    @Test(groups={"mkdir"}, dataProvider="altMkDirSanitizesRepeatedSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
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
    @Test(groups={"mkdirs"}, dataProvider="altMkDirSanitizesRepeatedSpecialCharacterProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    public void mkDirsSanitizesRepeatedSpecialCharacter(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException
    {
        _mkDirsSanitizationTest(filename, shouldSucceed, message);
    }
}

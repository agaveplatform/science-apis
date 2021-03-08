/**
 * 
 */
package org.iplantc.service.transfer.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.iplantc.service.transfer.AbstractPathSanitizationTest;
import org.iplantc.service.transfer.IPathSanitizationTest;
import org.iplantc.service.transfer.TransferTestRetryAnalyzer;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author dooley
 *
 */
@Test(groups={"external","s3","s3.path.sanitization"})
public class S3PathSanitizationIT extends AbstractPathSanitizationTest implements IPathSanitizationTest {

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.AbstractPathSanitizationTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "s3.example.com.json");
    }

    /**
     * Returns the special characters used to test. This may be overridden by subclasses
     * to prune based on naming restrictions of the underlying storage solution.
     * @return
     */
    @Override
    protected char[] getSpecialCharArray() {
        String specialChars = " _-!@#$%^*()+[]{}:."; // excluding "&" due to a bug in irods
        return specialChars.toCharArray();
    }

    @Override
    @DataProvider(parallel=true)
    protected Object[][] mkDirSanitizesSingleSpecialCharacterNameProvider() throws Exception {
        List<Object[]> tests = new ArrayList<Object[]>();
        for (char c: getSpecialCharArray()) {
            String sc = String.valueOf(c);
            if (c == ' ' || c == '.') {
                continue;
            } else if (c == '{') {
                tests.add(new Object[] { sc, false, "Directories named '" + sc + "' are disallowed and should throw an exception" });
            }
            else {
                tests.add(new Object[] { sc, true, "Directory name with single special character '" + sc + "' should be created" });
            }
        }

        return tests.toArray(new Object[][] {});
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

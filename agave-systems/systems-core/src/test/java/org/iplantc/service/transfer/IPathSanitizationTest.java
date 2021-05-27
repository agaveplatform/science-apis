package org.iplantc.service.transfer;

import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface IPathSanitizationTest {
    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider")
    void mkDirSanitizesSingleSpecialCharacterRelativePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;

    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider")
    void mkDirSanitizesSingleSpecialCharacterAbsolutePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;

    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesRepeatedSpecialCharacterProvider")
    void mkDirSanitizesRepeatedSpecialCharacter(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;

    @Test(groups={"mkdir"}, dataProvider="mkDirSanitizesWhitespaceProvider")
    void mkDirSanitizesWhitespace(String filename, String message)  throws IOException, RemoteDataException;

    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider")
    void mkDirsSanitizesSingleSpecialCharacterRelativePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;

    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesSingleSpecialCharacterProvider")
    void mkDirsSanitizesSingleSpecialCharacterAbsolutePath(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;

    @Test(groups={"mkdirs"}, dataProvider="mkDirSanitizesRepeatedSpecialCharacterProvider")
    void mkDirsSanitizesRepeatedSpecialCharacter(String filename, boolean shouldSucceed, String message)  throws FileNotFoundException;
}

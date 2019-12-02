package org.iplantc.service.transfer;

import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.s3.TransferTestRetryAnalyzer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;

public interface IRemoteDataClientIT {
    @Test(groups={"proxy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isPermissionMirroringRequired();

    @Test(groups={"proxy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isThirdPartyTransferSupported();

    @Test(groups={"exists"}, dataProvider="doesExistProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doesExist(String remotedir, boolean shouldExist, String message);

    @Test(groups="size", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void length();

    @Test(groups={"mkdir"}, dataProvider="mkdirProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void mkdir(String remotedir, boolean shouldReturnFalse, boolean shouldThrowException, String message);

    @Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void mkdirWithoutRemotePermissionThrowsRemoteDataException();

    @Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists();

    @Test(groups={"mkdir"}, dataProvider="mkdirsProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void mkdirs(String remotedir, boolean shouldThrowException, String message);

    @Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void mkdirsWithoutRemotePermissionThrowsRemoteDataException();

    @Test(groups={"put"} , dataProvider="putFileProvider",retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message);

    @Test(groups={"put"}, dataProvider="putFileOutsideHomeProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message);

    @Test(groups={"put"}, dataProvider="putFolderProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderCreatesRemoteFolder(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message);

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderMergesContentsWhenRemoteFolderExists();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFileOverwritesExistingFile();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFileWithoutRemotePermissionThrowsRemoteDataException();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderWithoutRemotePermissionThrowsRemoteDataException();

    @Test(groups={"put"}, dataProvider="putFolderProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message);

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFileFailsToMissingDestinationPath();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderFailsToMissingDestinationPath();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFailsForMissingLocalPath();

    @Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void putFolderFailsToRemoteFilePath();

    @Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void delete();

    @Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void deleteFailsOnMissingDirectory();

    @Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void deleteThrowsExceptionWhenNoPermission();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isDirectoryTrueForDirectory();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isDirectoryFalseForFile();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isDirectorThrowsExceptionForMissingPath();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isFileFalseForDirectory();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isFileTrueForFile();

    @Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void isFileThrowsExceptionForMissingPath();

    @Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void ls();

    @Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void lsFailsOnMissingDirectory();

    @Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void lsThrowsExceptionWhenNoPermission();

    @Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getThrowsExceptionOnMissingRemotePath();

    @Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getThrowsExceptionWhenDownloadingFolderToLocalFilePath();

    @Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath();

    @Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath();

    @Test(groups={"get"}, dataProvider="getDirectoryRetrievesToCorrectLocationProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
    void getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message);

    @Test(groups={"get"}, dataProvider="getFileRetrievesToCorrectLocationProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getFileRetrievesToCorrectLocation(Path localPath, Path expectedDownloadPath, String message);

    @Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getFileOverwritesExistingFile();

    @Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getOutputStream();

    @Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getOutputStreamFailsWhenRemotePathIsNullOrEmpty();

    @Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getOutputStreamFailsWhenRemotePathIsDirectory();

    @Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getOutputStreamFailsOnMissingPath();

    @Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getOutputStreamThrowsExceptionWhenNoPermission();

    @Test(groups={"getstream"}, dataProvider="getInputStreamProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getInputStream(String localFile, String message);

    @Test(groups={"getstream"}, dataProvider="getInputStreamOnRemoteDirectoryThrowsExceptionProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getInputStreamOnRemoteDirectoryThrowsException(String remotedir, String message);

    @Test(groups={"getstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void getInputStreamThrowsExceptionWhenNoPermission();

    @Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void checksumDirectoryFails();

    @Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void checksumMissingPathThrowsException();

    @Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void checksum();

    @Test(groups={"rename"}, dataProvider="doRenameProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doRename(String oldpath, String newpath, boolean shouldThrowException, String message);

    @Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doRenameThrowsRemotePermissionExceptionToRestrictedSource();

    @Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doRenameThrowsRemotePermissionExceptionToRestrictedDest();

    @Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doRenameThrowsFileNotFoundExceptionOnMissingDestPath();

    @Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void doRenameThrowsFileNotFoundExceptionOnMissingSourcePath();

    @Test(groups={"links"}, enabled=false)
    void getUrlForPath();

    @Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyFile();

    @Test(groups={"copy"}, dataProvider="copyIgnoreSlashesProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message);

    @Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyThrowsRemoteDataExceptionToRestrictedDest();

    @Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyThrowsFileNotFoundExceptionOnMissingDestPath();

    @Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyThrowsFileNotFoundExceptionOnMissingSourcePath();

    @Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void copyThrowsRemoteDataExceptionToRestrictedSource();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteFile();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteFolderAddsMissingFiles();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteFolderLeavesExistingFilesUntouched();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteFolderOverwritesFilesWithDeltas();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteFolderReplacesOverlappingFilesAndFolders();

    @Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
    void syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException;
}

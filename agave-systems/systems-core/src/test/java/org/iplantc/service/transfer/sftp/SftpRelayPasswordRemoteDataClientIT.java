/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.apache.commons.io.FileUtils;
import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.transfer.IRemoteDataClientIT;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientTestUtils;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.s3.TransferTestRetryAnalyzer;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * @author dooley
 *
 */
@Test(groups={"sftprelay","sftprelay.operations"})
public class SftpRelayPasswordRemoteDataClientIT extends RemoteDataClientTestUtils implements IRemoteDataClientIT {

	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#getSystemJson()
	 */
	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-password.example.com.json");
	}

	protected RemoteDataClient _getRemoteDataClient() throws EncryptionException {
		StorageConfig storageConfig = system.getStorageConfig();
		AuthConfig authConfig = storageConfig.getDefaultAuthConfig();
		String salt = system.getSystemId() + system.getStorageConfig().getHost() + authConfig.getUsername();
		if (system.getStorageConfig().getProxyServer() == null)
		{
			if (authConfig.getType().equals(AuthConfigType.SSHKEYS)) {
				return new SftpRelay(storageConfig.getHost(),
						storageConfig.getPort(),
						authConfig.getUsername(),
						authConfig.getClearTextPassword(salt),
						storageConfig.getRootDir(),
						storageConfig.getHomeDir(),
						authConfig.getClearTextPublicKey(salt),
						authConfig.getClearTextPrivateKey(salt));
			} else {
				return new SftpRelay(storageConfig.getHost(),
						storageConfig.getPort(),
						authConfig.getUsername(),
						authConfig.getClearTextPassword(salt),
						storageConfig.getRootDir(),
						storageConfig.getHomeDir());
			}
		}
		else
		{
			if (authConfig.getType().equals(AuthConfigType.SSHKEYS)) {
				return new SftpRelay(storageConfig.getHost(),
						storageConfig.getPort(),
						authConfig.getUsername(),
						authConfig.getClearTextPassword(salt),
						storageConfig.getRootDir(),
						storageConfig.getHomeDir(),
						system.getStorageConfig().getProxyServer().getHost(),
						system.getStorageConfig().getProxyServer().getPort(),
						authConfig.getClearTextPublicKey(salt),
						authConfig.getClearTextPrivateKey(salt));
			}
			else
			{
				return new SftpRelay(storageConfig.getHost(),
						storageConfig.getPort(),
						authConfig.getUsername(),
						authConfig.getClearTextPassword(salt),
						storageConfig.getRootDir(),
						storageConfig.getHomeDir(),
						system.getStorageConfig().getProxyServer().getHost(),
						system.getStorageConfig().getProxyServer().getPort());
			}
		}
	}

	/**
	 * Gets getClient() from current thread
	 * @return SftpRelay instance to the test server
	 * @throws RemoteCredentialException
	 * @throws RemoteDataException
	 */
	protected RemoteDataClient getClient()
	{
		RemoteDataClient client;
		try {
			if (threadClient.get() == null) {

				client = _getRemoteDataClient();
				String threadHomeDir = String.format("%s/thread-%s-%d",
						system.getStorageConfig().getHomeDir(),
						UUID.randomUUID().toString(),
						Thread.currentThread().getId());
				client.updateSystemRoots(client.getRootDir(),  threadHomeDir);
				threadClient.set(client);
			}
		} catch (EncryptionException e) {
			Assert.fail("Failed to get client", e);
		}

		return threadClient.get();
	}
	
	@Override
	protected String getForbiddenDirectoryPath(boolean shouldExist) {
		if (shouldExist) {
			return "/root";
		} else {
			return "/root/" + UUID.randomUUID().toString();
		}
	}

	/**
	 * Creates a temp directory within the shared "target/test-classes/transfer
	 * directory, which is mounted into the sftp-relay container for testing.
	 * Without overriding this method, we could not verify that transferred
	 * data matches the test data.
	 * @param prefix the prefix string to be used in generating the directory's name. may be {@code null}
	 * @return path to the temp dir
	 * @throws IOException
	 */
	@Override
	protected Path _createTempDirectory(String prefix) throws IOException {
		Path mountedRelayServerDataDirectory = Paths.get("target/test-classes/transfer");
		return Files.createTempDirectory(mountedRelayServerDataDirectory, prefix);
	}

	/** Creates a temp file within the shared "target/test-classes/transfer
	 * directory, which is mounted into the sftp-relay container for testing.
	 * Without overriding this method, we could not verify that transferred
	 * data matches the test data.
	 * @param prefix the prefix string to be used in generating the file's name. may be {@code null}
	 * @param suffix  the suffix string to be used in generating the file's name. may be {@code null}
	 * @return
	 * @throws IOException
	 */
	@Override
	protected Path _createTempFile(String prefix, String suffix) throws IOException {
		Path mountedRelayServerDataDirectory = Paths.get("target/test-classes/transfer");
		return Files.createTempFile(mountedRelayServerDataDirectory, prefix, suffix);
	}

	@Override
	@Test(groups={"proxy"}, retryAnalyzer= TransferTestRetryAnalyzer.class)
	public void isPermissionMirroringRequired() {
		_isPermissionMirroringRequired();
	}

	@Override
	@Test(groups={"proxy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isThirdPartyTransferSupported()
	{
		_isThirdPartyTransferSupported();
	}

	@Override
	@Test(groups={"stat"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getFileInfoReturnsRemoteFileInfoForFile() {
		_getFileInfoReturnsRemoteFileInfoForFile();
	}

	@Override
	@Test(groups={"stat"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getFileInfoReturnsRemoteFileInfoForDirectory() {
		_getFileInfoReturnsRemoteFileInfoForDirectory();
	}

	@Override
	@Test(groups={"stat"}, expectedExceptions = FileNotFoundException.class, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getFileInfoReturnsErrorOnMissingPath() throws FileNotFoundException {
		_getFileInfoReturnsErrorOnMissingPath();
	}

	@Override
	@Test(groups={"exists"}, dataProvider="doesExistProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doesExist(String remotedir, boolean shouldExist, String message) {
		_doesExist(remotedir, shouldExist, message);
	}

	@Override
	@Test(groups={"size"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void length()
	{
		_length();
	}

	@Override
	@Test(groups={"mkdir"}, dataProvider="mkdirProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void mkdir(String remotedir, boolean shouldReturnFalse, boolean shouldThrowException, String message) {
		_mkdir(remotedir, shouldReturnFalse, shouldThrowException, message);
	}

	@Override
	@Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void mkdirWithoutRemotePermissionThrowsRemoteDataException() {
		_mkdirWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Override
	@Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists() {
		_mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists();
	}

	@Override
	@Test(groups={"mkdir"}, dataProvider="mkdirsProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void mkdirs(String remotedir, boolean shouldThrowException, String message) {
		_mkdirs(remotedir, shouldThrowException, message);
	}

	@Override
	@Test(groups={"mkdir"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void mkdirsWithoutRemotePermissionThrowsRemoteDataException() {
		_mkdirsWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Override
	@Test(groups={"put"}, dataProvider = "putFileProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message) {
		_putFile(remotePath, expectedRemoteFilename, shouldThrowException, message);
	}

	@Override
	@Test(groups={"put"}, dataProvider="putFileOutsideHomeProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message) {
		_putFileOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
	}

	@Override
	@Test(groups={"put"}, dataProvider="putFolderProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderCreatesRemoteFolder(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message) {
		_putFolderCreatesRemoteFolder(remotePath, expectedRemoteFilename, shouldThrowException, message);
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists() {
		_putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderMergesContentsWhenRemoteFolderExists()
	{
		_putFolderMergesContentsWhenRemoteFolderExists();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFileOverwritesExistingFile()
	{
		_putFileOverwritesExistingFile();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFileWithoutRemotePermissionThrowsRemoteDataException() {
		_putFileWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderWithoutRemotePermissionThrowsRemoteDataException() {
		_putFolderWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Override
	@Test(groups={"put"}, dataProvider="putFolderProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message) {
		_putFolderOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFileFailsToMissingDestinationPath()
	{
		_putFileFailsToMissingDestinationPath();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderFailsToMissingDestinationPath()
	{
		_putFolderFailsToMissingDestinationPath();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFailsForMissingLocalPath()
	{
		_putFailsForMissingLocalPath();
	}

	@Override
	@Test(groups={"put"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void putFolderFailsToRemoteFilePath()
	{
		_putFolderFailsToRemoteFilePath();
	}

	@Override
	@Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void delete()
	{
		_delete();
	}

	@Override
	@Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void deleteFailsOnMissingDirectory()
	{
		_deleteFailsOnMissingDirectory();
	}

	@Override
	@Test(groups={"delete"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void deleteThrowsExceptionWhenNoPermission()
	{
		_deleteThrowsExceptionWhenNoPermission();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isDirectoryTrueForDirectory()
	{
		_isDirectoryTrueForDirectory();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isDirectoryFalseForFile()
	{
		_isDirectoryFalseForFile();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isDirectorThrowsExceptionForMissingPath()
	{
		_isDirectorThrowsExceptionForMissingPath();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isFileFalseForDirectory()
	{
		_isFileFalseForDirectory();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isFileTrueForFile()
	{
		_isFileTrueForFile();
	}

	@Override
	@Test(groups={"is"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void isFileThrowsExceptionForMissingPath()
	{
		_isFileThrowsExceptionForMissingPath();
	}

	@Override
	@Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void ls()
	{
		_ls();
	}

	@Override
	@Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void lsFailsOnMissingDirectory()
	{
		_lsFailsOnMissingDirectory();
	}

	@Override
	@Test(groups={"list"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void lsThrowsExceptionWhenNoPermission()
	{
		_lsThrowsExceptionWhenNoPermission();
	}

	@Override
	@Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getThrowsExceptionOnMissingRemotePath()
	{
		_getThrowsExceptionOnMissingRemotePath();
	}

	@Override
	@Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getThrowsExceptionWhenDownloadingFolderToLocalFilePath() {
		_getThrowsExceptionWhenDownloadingFolderToLocalFilePath();
	}

	@Override
	@Test(groups={"get"}, expectedExceptions = FileNotFoundException.class, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath() throws FileNotFoundException {
		_getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath();
	}

	@Override
	@Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath() {
		_getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath();
	}

	@Override
	@Test(groups={"get"}, dataProvider="getDirectoryRetrievesToCorrectLocationProvider", retryAnalyzer = TransferTestRetryAnalyzer.class)
	public void getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message) {
		_getDirectoryRetrievesToCorrectLocation(localdir, createTestDownloadFolder, expectedDownloadPath, message);
	}

	@Test
	public void copyRemoteDirectory() {
		String remoteBasePath = null;
		Path tmpDir = null;
		Path testDownloadPath = null;
		try
		{
			tmpDir = _createTempDirectory(null);
			testDownloadPath = tmpDir.resolve("copyRemoteDirTest");

			testDownloadPath.toFile().mkdirs();

			remoteBasePath = createRemoteTestDir();

			getClient().put(LOCAL_BINARY_FILE, remoteBasePath);
			getClient().put(LOCAL_TXT_FILE, remoteBasePath);
			String remoteSubpath = UUID.randomUUID().toString();
			getClient().mkdir(remoteBasePath + "/" + remoteSubpath);
			getClient().put(LOCAL_BINARY_FILE, remoteBasePath + "/" + remoteSubpath);
			getClient().put(LOCAL_TXT_FILE, remoteBasePath + "/" + remoteSubpath);

			Assert.assertTrue(getClient().doesExist(remoteBasePath),
					"Test directory not found on remote test system after put.");

			((SftpRelay)getClient()).copyRemoteDirectory(remoteBasePath, testDownloadPath.toString(),
					true, true, null);

			Assert.assertTrue(testDownloadPath.resolve(LOCAL_BINARY_FILE_NAME).toFile().exists(),
							"Binary file not found on local system after copyRemoteDirectory.");
			Assert.assertTrue(testDownloadPath.resolve(LOCAL_TXT_FILE_NAME).toFile().exists(),
					"Text file not found on local system after copyRemoteDirectory.");
			Assert.assertTrue(testDownloadPath.resolve(remoteSubpath).toFile().exists(),
					"Subdirectory not found on local system after copyRemoteDirectory.");
			Assert.assertTrue(testDownloadPath.resolve(remoteSubpath).toFile().exists(),
					"Remote directory not present as directory on local system after copyRemoteDirectory.");
			Assert.assertTrue(testDownloadPath.resolve(remoteSubpath + "/" + LOCAL_BINARY_FILE_NAME).toFile().exists(),
					"Nested binary file not found on local system after copyRemoteDirectory.");
			Assert.assertTrue(testDownloadPath.resolve(remoteSubpath + "/" + LOCAL_TXT_FILE_NAME).toFile().exists(),
					"Nested text file not found on local system after copyRemoteDirectory.");
		}
		catch (Exception e) {
			Assert.fail("get should not throw unexpected exception", e);
		}
		finally {
			try { getClient().delete(remoteBasePath); } catch (Exception ignore) {}
			try {
				if (testDownloadPath != null)
					FileUtils.deleteQuietly(testDownloadPath.toFile());
			} catch (Exception ignore) {}
			try { if (tmpDir != null) FileUtils.deleteQuietly(tmpDir.toFile()); } catch (Exception ignore) {}
		}
	}

	@Override
	@Test(groups={"get"}, dataProvider="getFileRetrievesToCorrectLocationProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getFileRetrievesToCorrectLocation(Path localPath, Path expectedDownloadPath, String message) {
		_getFileRetrievesToCorrectLocation(localPath, expectedDownloadPath, message);
	}

	@Override
	@Test(groups={"get"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getFileOverwritesExistingFile()
	{
		_getFileOverwritesExistingFile();
	}

	@Override
	@Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getOutputStream()
	{
		_getOutputStream();
	}

	@Override
	@Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getOutputStreamFailsWhenRemotePathIsNullOrEmpty()
	{
		_getOutputStreamFailsWhenRemotePathIsNullOrEmpty();
	}

	@Override
	@Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getOutputStreamFailsWhenRemotePathIsDirectory()
	{
		_getOutputStreamFailsWhenRemotePathIsDirectory();
	}

	@Override
	@Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getOutputStreamFailsOnMissingPath()
	{
		_getOutputStreamFailsOnMissingPath();
	}

	@Override
	@Test(groups={"putstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getOutputStreamThrowsExceptionWhenNoPermission()
	{
		_getOutputStreamThrowsExceptionWhenNoPermission();
	}

	@Override
	@Test(groups={"getstream"}, dataProvider="getInputStreamProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getInputStream(String localFile, String message)
	{
		_getInputStream(localFile, message);
	}

	@Override
	@Test(groups={"getstream"}, dataProvider="getInputStreamOnRemoteDirectoryThrowsExceptionProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getInputStreamOnRemoteDirectoryThrowsException(String remotedir, String message) {
		_getInputStreamOnRemoteDirectoryThrowsException(remotedir, message);
	}

	@Override
	@Test(groups={"getstream"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void getInputStreamThrowsExceptionWhenNoPermission()
	{
		_getInputStreamThrowsExceptionWhenNoPermission();
	}

	@Override
	@Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void checksumDirectoryFails()
	{
		_checksumDirectoryFails();
	}

	@Override
	@Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void checksumMissingPathThrowsException()
	{
		_checksumMissingPathThrowsException();
	}

	@Override
	@Test(groups={"checksum"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void checksum()
	{
		_checksum();
	}

	@Override
	@Test(groups={"rename"}, dataProvider="doRenameProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doRename(String oldpath, String newpath, boolean shouldThrowException, String message) {
		_doRename(oldpath, newpath, shouldThrowException, message);
	}

	@Override
	@Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doRenameThrowsRemotePermissionExceptionToRestrictedSource() {
		_doRenameThrowsRemotePermissionExceptionToRestrictedSource();
	}

	@Override
	@Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doRenameThrowsRemotePermissionExceptionToRestrictedDest() {
		_doRenameThrowsRemotePermissionExceptionToRestrictedDest();
	}

	@Override
	@Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doRenameThrowsFileNotFoundExceptionOnMissingDestPath() {
		_doRenameThrowsFileNotFoundExceptionOnMissingDestPath();
	}

	@Override
	@Test(groups={"rename"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void doRenameThrowsFileNotFoundExceptionOnMissingSourcePath() {
		_doRenameThrowsFileNotFoundExceptionOnMissingSourcePath();
	}

	@Override
	@Test(enabled=false)
	public void getUrlForPath()
	{
		_getUrlForPath();
	}

	@Override
	@Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyFile()
	{
		_copyFile();
	}

	@Override
	@Test(groups={"copy"}, dataProvider="copyIgnoreSlashesProvider", retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message) {
		_copyDir(src, dest, createDest, expectedPath, shouldThrowException, message);
	}

	@Override
	@Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyThrowsRemoteDataExceptionToRestrictedDest()
	{
		_copyThrowsRemoteDataExceptionToRestrictedDest();
	}

	@Override
	@Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyThrowsFileNotFoundExceptionOnMissingDestPath() {
		_copyThrowsFileNotFoundExceptionOnMissingDestPath();

	}

	@Override
	@Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyThrowsFileNotFoundExceptionOnMissingSourcePath() {
		_copyThrowsFileNotFoundExceptionOnMissingSourcePath();
	}

	@Override
	@Test(groups={"copy"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void copyThrowsRemoteDataExceptionToRestrictedSource()
	{
		_copyThrowsRemoteDataExceptionToRestrictedSource();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteFile()
	{
		_syncToRemoteFile();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteFolderAddsMissingFiles()
	{
		_syncToRemoteFolderAddsMissingFiles();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteFolderLeavesExistingFilesUntouched()
	{
		_syncToRemoteFolderLeavesExistingFilesUntouched();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteFolderOverwritesFilesWithDeltas()
	{
		_syncToRemoteFolderOverwritesFilesWithDeltas();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteFolderReplacesOverlappingFilesAndFolders() {
		_syncToRemoteFolderReplacesOverlappingFilesAndFolders();
	}

	@Override
	@Test(groups={"sync"}, retryAnalyzer=TransferTestRetryAnalyzer.class)
	public void syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException {
		_syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem();
	}

}

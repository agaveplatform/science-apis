/**
 * 
 */
package org.iplantc.service.transfer.s3;

import java.io.*;
import java.util.*;

import org.testng.collections.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.transfer.AbstractRemoteDataClientTest;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.s3.S3Client;
import org.jclouds.s3.blobstore.S3BlobStore;
import org.jclouds.s3.domain.ObjectMetadata;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.*;

/**
 * @author dooley
 *
 */
@Test(singleThreaded=true, groups= {"s3","s3.filesystem","integration"})
public class S3RemoteDataClientTest extends RemoteDataClientTestUtils
{
	protected String containerName;
	
	@Override
	@BeforeClass
    protected void beforeSubclass() throws Exception {
    	super.beforeSubclass();
    	containerName = system.getStorageConfig().getContainerName();
		system.getStorageConfig().setHomeDir(
				StringUtils.strip(system.getStorageConfig().getHomeDir(), "/") );
    	// create test container if not present
		if (!((S3Jcloud)getClient()).getBlobStore().containerExists(containerName)) {
			((S3Jcloud) getClient()).getBlobStore().createContainerInLocation(null, containerName);
		}
	}
    
	@BeforeMethod
    public void beforeMethod() throws FileNotFoundException, RemoteDataException {
    	FileUtils.deleteQuietly(new File(getLocalDownloadDir()));

		String resolvedPath = StringUtils.strip(getClient().resolvePath(""), "/");

		BlobStore blobStore = ((S3Jcloud)getClient()).getBlobStore();
		if (!blobStore.containerExists(containerName)) {
			blobStore.createContainerInLocation(null, containerName);
		}

		// ensure home directory is present
		BlobMetadata objectMetadata = blobStore.blobMetadata(containerName, resolvedPath);
		if (objectMetadata != null) {
			if ( objectMetadata.getContentMetadata().getContentType().equals("application/directory") ) {
				blobStore.deleteDirectory(containerName, resolvedPath);
			}
			else {
				throw new RemoteDataException("Home directory path exists, but is not a directory. Test directory cannot be created.");
			}
		}

		blobStore.createDirectory(containerName, resolvedPath);
    }
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#afterMethod()
	 */
	@Override
	@AfterMethod
	public void afterMethod() throws Exception
	{
		_cleanupTestBucket();
	}

	/**
	 * Deletes the contents of the test bucket so the environment is clean for hte next test.
	 * @throws RemoteDataException
	 */
	protected void _cleanupTestBucket() throws RemoteDataException {
		S3Jcloud client = (S3Jcloud)getClient();

		ListContainerOptions listContainerOptions = new ListContainerOptions();
		listContainerOptions = listContainerOptions.recursive().maxResults(100);
		PageSet<? extends StorageMetadata> pageSet = client.getBlobStore().list(containerName, listContainerOptions);

		Set<String> names = Sets.newHashSet();
		for (StorageMetadata storageMetadata: pageSet.toArray(new StorageMetadata[]{})) {
			if (storageMetadata == null) {
				continue;
			} else {
				names.add(storageMetadata.getName());
			}
		}

		client.getBlobStore().removeBlobs(containerName, names);
	}

	@Override
	@AfterClass(alwaysRun=true)
	protected void afterClass() throws Exception {
		try
		{
			FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
			FileUtils.deleteQuietly(tmpFile);
			FileUtils.deleteQuietly(tmpFile);
		}
		finally {
			try { getClient().disconnect(); } catch (Exception ignore) {}
		}

		try
		{
			_cleanupTestBucket();
		}
		catch (Exception e) {
			Assert.fail("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
		}
		finally {
			try { getClient().disconnect(); } catch (Exception ignore) {}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#getSystemJson()
	 */
	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "s3.example.com.json");
	}
	
	@Override
    protected void _isPermissionMirroringRequired()
	{
		Assert.assertFalse(getClient().isPermissionMirroringRequired(), 
				"S3 permission mirroring should not be enabled.");
		
	}
	
    @Override
	protected void _isThirdPartyTransferSupported()
	{
		Assert.assertFalse(getClient().isThirdPartyTransferSupported());
	}

	@Override
	protected String getForbiddenDirectoryPath(boolean shouldExist) throws RemoteDataException {
		if (shouldExist) {
			throw new RemoteDataException("Bypassing test for s3 forbidden file/folder");
		} else {
			throw new RemoteDataException("Bypassing test for S3 missing file/folder");
		}
	}
	
	@Override
	protected void _copyThrowsRemoteDataExceptionToRestrictedSource()
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

	@Test(groups={"s3.proxy"})
	public void isPermissionMirroringRequired() {
		_isPermissionMirroringRequired();
	}

	@Test(groups={"s3.proxy"}, dependsOnMethods= {"isPermissionMirroringRequired"})
	public void isThirdPartyTransferSupported()
	{
		_isThirdPartyTransferSupported();
	}

	@Test(dataProvider="doesExistProvider", dependsOnMethods={"isThirdPartyTransferSupported"})
	public void doesExist(String remotedir, boolean shouldExist, String message)
	{
		_doesExist(remotedir, shouldExist, message);
	}

	@Test(groups="s3.size", dependsOnMethods= {"doesExist"})
	public void length()
	{
		_length();
	}

	@Test(dataProvider="mkdirProvider", groups={"s3.mkdir"}, dependsOnMethods={"length"})
	public void mkdir(String remotedir, boolean shouldReturnFalse, boolean shouldThrowException, String message)
	{
		_mkdir(remotedir, shouldReturnFalse, shouldThrowException, message);
	}

	@Test(groups={"s3.mkdir"}, dependsOnMethods={"mkdir"})
	public void mkdirWithoutRemotePermissionThrowsRemoteDataException()
	{
		_mkdirWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Test(groups={"s3.mkdir"}, dependsOnMethods={"mkdirWithoutRemotePermissionThrowsRemoteDataException"})
	public void mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists()
	{
		_mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists();
	}

	@Test(dataProvider="mkdirsProvider", groups={"s3.mkdir"}, dependsOnMethods={"mkdirThrowsRemoteDataExceptionIfDirectoryAlreadyExists"})
	public void mkdirs(String remotedir, boolean shouldThrowException, String message)
	{
		_mkdirs(remotedir, shouldThrowException, message);
	}

	@Test(groups={"s3.mkdir"}, dependsOnMethods={"mkdirs"})
	public void mkdirsWithoutRemotePermissionThrowsRemoteDataException()
	{
		_mkdirsWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Test(dataProvider="putFileProvider", groups={"s3.put", "s3.upload"}, dependsOnMethods={"mkdirsWithoutRemotePermissionThrowsRemoteDataException"})//, invocationCount=25, threadPoolSize=5 )
	public void putFile(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
	{
		_putFile(remotePath, expectedRemoteFilename, shouldThrowException, message);
	}

	@Test(dataProvider="putFileOutsideHomeProvider", groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFile"})
	public void putFileOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
	{
		_putFileOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
	}

	@Test(dataProvider="putFolderProvider", groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFileOutsideHome"})
	public void putFolderCreatesRemoteFolder(String remotePath, String expectedRemoteFilename, boolean shouldThrowException, String message)
	{
		_putFolderCreatesRemoteFolder(remotePath, expectedRemoteFilename, shouldThrowException, message);
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFolderCreatesRemoteFolder"})
	public void putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists()
	{
		_putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFolderCreatesRemoteSubFolderWhenNamedRemotePathExists"})
	public void putFolderMergesContentsWhenRemoteFolderExists()
	{
		_putFolderMergesContentsWhenRemoteFolderExists();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFolderMergesContentsWhenRemoteFolderExists"})
	public void putFileOverwritesExistingFile()
	{
		_putFileOverwritesExistingFile();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFileOverwritesExistingFile"})
	public void putFileWithoutRemotePermissionThrowsRemoteDataException()
	{
		_putFileWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFileWithoutRemotePermissionThrowsRemoteDataException"})
	public void putFolderWithoutRemotePermissionThrowsRemoteDataException()
	{
		_putFolderWithoutRemotePermissionThrowsRemoteDataException();
	}

	@Test(groups={"s3.put", "s3.upload"}, dataProvider="putFolderProvider", dependsOnMethods= {"putFolderWithoutRemotePermissionThrowsRemoteDataException"})
	public void putFolderOutsideHome(String remoteFilename, String expectedRemoteFilename, boolean shouldThrowException, String message)
	{
		_putFolderOutsideHome(remoteFilename, expectedRemoteFilename, shouldThrowException, message);
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFolderOutsideHome"})
	public void putFileFailsToMissingDestinationPath()
	{
		_putFileFailsToMissingDestinationPath();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFileFailsToMissingDestinationPath"})
	public void putFolderFailsToMissingDestinationPath()
	{
		_putFolderFailsToMissingDestinationPath();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFolderFailsToMissingDestinationPath"})
	public void putFailsForMissingLocalPath()
	{
		_putFailsForMissingLocalPath();
	}

	@Test(groups={"s3.put", "s3.upload"}, dependsOnMethods={"putFailsForMissingLocalPath"})
	public void putFolderFailsToRemoteFilePath()
	{
		_putFolderFailsToRemoteFilePath();
	}

	@Test(groups={"s3.delete"}, dependsOnMethods={"putFolderFailsToRemoteFilePath"})
	public void delete()
	{
		_delete();
	}

	@Test(groups={"s3.delete"}, dependsOnMethods={"delete"})
	public void deleteFailsOnMissingDirectory()
	{
		_deleteFailsOnMissingDirectory();
	}

	@Test(groups={"s3.delete"}, dependsOnMethods={"deleteFailsOnMissingDirectory"})
	public void deleteThrowsExceptionWhenNoPermission()
	{
		_deleteThrowsExceptionWhenNoPermission();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"deleteThrowsExceptionWhenNoPermission"})
	public void isDirectoryTrueForDirectory()
	{
		_isDirectoryTrueForDirectory();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"isDirectoryTrueForDirectory"})
	public void isDirectoryFalseForFile()
	{
		_isDirectoryFalseForFile();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"isDirectoryFalseForFile"})
	public void isDirectorThrowsExceptionForMissingPath()
	{
		_isDirectorThrowsExceptionForMissingPath();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"isDirectorThrowsExceptionForMissingPath"})
	public void isFileFalseForDirectory()
	{
		_isFileFalseForDirectory();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"isFileFalseForDirectory"})
	public void isFileTrueForFile()
	{
		_isFileTrueForFile();
	}

	@Test(groups={"s3.is"}, dependsOnMethods={"isFileTrueForFile"})
	public void isFileThrowsExceptionForMissingPath()
	{
		_isFileThrowsExceptionForMissingPath();
	}

	@Test(groups={"s3.list"}, dependsOnMethods={"isFileThrowsExceptionForMissingPath"})
	public void ls()
	{
		_ls();
	}

	@Test(groups={"s3.list"}, dependsOnMethods={"ls"})
	public void lsFailsOnMissingDirectory()
	{
		_lsFailsOnMissingDirectory();
	}

	@Test(groups={"s3.list"}, dependsOnMethods={"lsFailsOnMissingDirectory"})
	public void lsThrowsExceptionWhenNoPermission()
	{
		_lsThrowsExceptionWhenNoPermission();
	}

	@Test(groups={"s3.get", "s3.download"}, dependsOnMethods={"lsThrowsExceptionWhenNoPermission"})
	public void getThrowsExceptionOnMissingRemotePath()
	{
		_getThrowsExceptionOnMissingRemotePath();
	}

	@Test(groups={"s3.get", "s3.download"}, dependsOnMethods={"getThrowsExceptionOnMissingRemotePath"})
	public void getThrowsExceptionWhenDownloadingFolderToLocalFilePath()
	{
		_getThrowsExceptionWhenDownloadingFolderToLocalFilePath();
	}

	@Test(groups={"s3.get", "s3.download"}, dependsOnMethods={"getThrowsExceptionWhenDownloadingFolderToLocalFilePath"})
	public void getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath()
	{
		_getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath();
	}

	@Test(groups={"s3.get", "s3.download"}, dependsOnMethods={"getDirectoryThrowsExceptionWhenDownloadingFolderToNonExistentLocalPath"})
	public void getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath()
	{
		_getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath();
	}

	@Test(groups={"s3.get", "s3.download"}, dataProvider="getDirectoryRetrievesToCorrectLocationProvider", dependsOnMethods={"getThrowsExceptionWhenDownloadingFileToNonExistentLocalPath"})
	public void getDirectoryRetrievesToCorrectLocation(String localdir, boolean createTestDownloadFolder, String expectedDownloadPath, String message)
	{
		_getDirectoryRetrievesToCorrectLocation(localdir, createTestDownloadFolder, expectedDownloadPath, message);
	}

	@Test(groups={"s3.get", "s3.download"}, dataProvider="getFileRetrievesToCorrectLocationProvider", dependsOnMethods={"getDirectoryRetrievesToCorrectLocation"})
	public void getFileRetrievesToCorrectLocation(String localPath, String expectedDownloadPath, String message)
	{
		_getFileRetrievesToCorrectLocation(localPath, expectedDownloadPath, message);
	}

	@Test(groups={"s3.get", "s3.download"}, dependsOnMethods={"getFileRetrievesToCorrectLocation"})
	public void getFileOverwritesExistingFile()
	{
		_getFileOverwritesExistingFile();
	}

    @Test(groups={"s3.stream","s3.put"}, dependsOnMethods = {"getFileOverwritesExistingFile"})
	public void getOutputStream()
	{
		_getOutputStream();
	}

	@Test(groups={"s3.stream","s3.put"}, dependsOnMethods={"getOutputStream"})
	public void getOutputStreamFailsWhenRemotePathIsNullOrEmpty()
	{
		_getOutputStreamFailsWhenRemotePathIsNullOrEmpty();
	}

	@Test(groups={"s3.stream","s3.put"}, dependsOnMethods={"getOutputStreamFailsWhenRemotePathIsNullOrEmpty"})
	public void getOutputStreamFailsWhenRemotePathIsDirectory()
	{
		_getOutputStreamFailsWhenRemotePathIsDirectory();
	}

	@Test(groups={"s3.stream","s3.put"}, dependsOnMethods={"getOutputStreamFailsWhenRemotePathIsDirectory"})
	public void getOutputStreamFailsOnMissingPath()
	{
		_getOutputStreamFailsOnMissingPath();
	}

	@Test(groups={"s3.stream","s3.put"}, dependsOnMethods={"getOutputStreamFailsOnMissingPath"})
	public void getOutputStreamThrowsExceptionWhenNoPermission()
	{
		_getOutputStreamThrowsExceptionWhenNoPermission();
	}

	@Test(groups={"s3.stream", "s3.get"}, dataProvider="getInputStreamProvider", dependsOnMethods={"getOutputStreamThrowsExceptionWhenNoPermission"})
	public void getInputStream(String localFile, String remotedir, boolean shouldThrowException, String message)
	{
		_getInputStream(localFile, remotedir, shouldThrowException, message);
	}

	@Test(groups={"s3.stream", "s3.get"}, dependsOnMethods= {"getInputStream"})
	public void getInputStreamThrowsExceptionWhenNoPermission()
	{
		_getInputStreamThrowsExceptionWhenNoPermission();
	}

	@Test(groups={"s3.checksum"}, dependsOnMethods={"getInputStreamThrowsExceptionWhenNoPermission"})
	public void checksumDirectoryFails()
	{
		_checksumDirectoryFails();
	}

	@Test(groups={"s3.checksum"}, dependsOnMethods={"checksumDirectoryFails"})
	public void checksumMissingPathThrowsException()
	{
		_checksumMissingPathThrowsException();
	}

	@Test(groups={"s3.checksum"}, dependsOnMethods={"checksumMissingPathThrowsException"})
	public void checksum()
	{
		_checksum();
	}

	@Test(dataProvider="doRenameProvider", groups={"s3.rename"}, dependsOnMethods= {"checksum"})
	public void doRename(String oldpath, String newpath, boolean shouldThrowException, String message)
	{
		_doRename(oldpath, newpath, shouldThrowException, message);
	}

	@Test(groups={"s3.rename"}, dependsOnMethods= {"doRename"})
	public void doRenameThrowsRemotePermissionExceptionToRestrictedSource()
	{
		_doRenameThrowsRemotePermissionExceptionToRestrictedSource();
	}

	@Test(groups={"s3.rename"}, dependsOnMethods= {"doRenameThrowsRemotePermissionExceptionToRestrictedSource"})
	public void doRenameThrowsRemotePermissionExceptionToRestrictedDest()
	{
		_doRenameThrowsRemotePermissionExceptionToRestrictedDest();
	}

	@Test(groups={"s3.rename"}, dependsOnMethods= {"doRenameThrowsRemotePermissionExceptionToRestrictedDest"})
	public void doRenameThrowsFileNotFoundExceptionOnMissingDestPath()
	{
		_doRenameThrowsFileNotFoundExceptionOnMissingDestPath();
	}

	@Test(groups={"s3.rename"}, dependsOnMethods= {"doRenameThrowsFileNotFoundExceptionOnMissingDestPath"})
	public void doRenameThrowsFileNotFoundExceptionOnMissingSourcePath()
	{
		_doRenameThrowsFileNotFoundExceptionOnMissingSourcePath();
	}

	@Test(enabled=false)
	public void getUrlForPath()
	{
		_getUrlForPath();
	}

	@Test(groups={"s3.copy"}, dependsOnMethods={"doRenameThrowsFileNotFoundExceptionOnMissingSourcePath"})
	public void copyFile()
	{
		_copyFile();
	}

	@Test(groups={"s3.copy"}, dataProvider="copyIgnoreSlashesProvider", dependsOnMethods= {"copyFile"})
	public void copyDir(String src, String dest, boolean createDest, String expectedPath, boolean shouldThrowException, String message)
	{
		_copyDir(src, dest, createDest, expectedPath, shouldThrowException, message);
	}

	@Test(groups={"s3.copy"}, dependsOnMethods={"copyDir"})
	public void copyThrowsRemoteDataExceptionToRestrictedDest()
	{
		_copyThrowsRemoteDataExceptionToRestrictedDest();
	}

	@Test(groups={"s3.copy"}, dependsOnMethods={"copyThrowsRemoteDataExceptionToRestrictedDest"})
	public void copyThrowsFileNotFoundExceptionOnMissingDestPath()
	{
		_copyThrowsFileNotFoundExceptionOnMissingDestPath();

	}

	@Test(groups={"s3.copy"}, dependsOnMethods={"copyThrowsFileNotFoundExceptionOnMissingDestPath"})
	public void copyThrowsFileNotFoundExceptionOnMissingSourcePath()
	{
		_copyThrowsFileNotFoundExceptionOnMissingSourcePath();
	}

	@Test(groups={"s3.copy"}, dependsOnMethods= {"copyThrowsFileNotFoundExceptionOnMissingSourcePath"})
	public void copyThrowsRemoteDataExceptionToRestrictedSource()
	{
		_copyThrowsRemoteDataExceptionToRestrictedSource();
	}

	@Test(groups={"s3.sync"}, dependsOnMethods={"copyThrowsRemoteDataExceptionToRestrictedSource"})
	public void syncToRemoteFile()
	{
		_syncToRemoteFile();
	}

	@Test(groups={"s3.sync"}, dependsOnMethods={"syncToRemoteFile"})
	public void syncToRemoteFolderAddsMissingFiles()
	{
		_syncToRemoteFolderAddsMissingFiles();
	}

	@Test(groups={"s3.sync"}, dependsOnMethods={"syncToRemoteFolderAddsMissingFiles"})
	public void syncToRemoteFolderLeavesExistingFilesUntouched()
	{
		_syncToRemoteFolderLeavesExistingFilesUntouched();
	}

	@Test(groups={"s3.sync"}, dependsOnMethods={"syncToRemoteFolderLeavesExistingFilesUntouched"})
	public void syncToRemoteFolderOverwritesFilesWithDeltas()
	{
		_syncToRemoteFolderOverwritesFilesWithDeltas();
	}

	@Test(groups={"s3.sync"}, dependsOnMethods={"syncToRemoteFolderOverwritesFilesWithDeltas"})
	public void syncToRemoteFolderReplacesOverlappingFilesAndFolders()
	{
		_syncToRemoteFolderReplacesOverlappingFilesAndFolders();
	}
	
	@Test(groups={"s3.sync"}, dependsOnMethods={"syncToRemoteFolderReplacesOverlappingFilesAndFolders"})
	public void syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem() throws IOException, RemoteDataException {
		_syncToRemoteSyncesSubfoldersRatherThanEmbeddingThem();
	}
}

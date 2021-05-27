package org.iplantc.service.transfer;

import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.gridftp.GridFTP;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Interface for interacting with remote file systems. This provides a common 
 * abstraction for data operations while extending the traditional POSIX 
 * commands with behavior for authentication, monitoring and synchronization.
 * 
 * @author dooley
 *
 */
public interface RemoteDataClient extends RemoteDataClientPermissionProvider {

	/**
	 * Creates the directory specified by <code>remotePath</code>. If the parent
	 * does not exist, this will throw a {@link FileNotFoundException}. Use the
	 * {@link #mkdirs(String)} method in this situation. Note that depending
	 * on the underlying implementation and remote system protocol, the directory
	 * may or may not be available for writing immediately. In the case of Azure,
	 * bucket creation can take up to 30 minutes.
	 * 
	 * @param remotePath the virtual path to create
	 * @return true on success, false if the directory already exists
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    boolean mkdir(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Creates the directory(ies) specified by <code>remotePath</code>. Any missing
	 * directories are created automatically. Note that depending
	 * on the underlying implementation and remote system protocol, the directory
	 * may or may not be available for writing immediately. In the case of Azure,
	 * bucket creation can take up to 30 minutes.
	 * 
	 * @param remotePath the virtual path to create
	 * @return true on success, false if the directory already exists
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    boolean mkdirs(String remotePath) throws IOException, RemoteDataException;
	
	/**
     * Creates the directory(ies) specified by <code>remotePath</code>. Any missing
     * directories are created automatically. The {@code authorizedUser} is assigned
     * permissions to all directories created during this operation. 
     * 
     * Note that depending
     * on the underlying implementation and remote system protocol, the directory
     * may or may not be available for writing immediately. In the case of Azure,
     * bucket creation can take up to 30 minutes.
     * 
     * @param remotePath the virtual path to create
     * @return true on success, false if the directory already exists
     * @throws IOException
     * @throws RemoteDataException
     */
    boolean mkdirs(String remotePath, String authorizedUser) throws IOException, RemoteDataException;
	
	/**
	 * Authenticates to the remote system using the system credentials. Note
	 * that the authentication credentials used in this call will often be 
	 * different than the actual API user's platform credentials.
	 *
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void authenticate() throws IOException, RemoteDataException;

    /**
     * Returns the maximum buffer size for the given protocol. This varies 
     * from implementation to implementation and is tuned based on benchmarks
     * performed periodically as individual protocol libraries are updated and
     * real-world peformance is sampled. 
     * 
     * @return maximum buffer size in bytes
     */
    int getMaxBufferSize();

    /**
	 * Opens a pre-authenticated input stream to a remote file. If the file does not exist, a 
	 * 404 is thrown. 
	 *  
	 * @param remotePath the virtual path to which to write content 
	 * @param passive should the connection be obtained actively or passively
	 * @return a pre-authenticated output stream to the remote file item
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    RemoteInputStream<?> getInputStream(String remotePath, boolean passive)
			throws IOException, RemoteDataException;
	
	/**
	 * Opens a pre-authenticated output stream to a remote file. If the file does not exist, a 
	 * placeholder is created for writing. 
	 *  
	 * @param remotePath the virtual path to which to write content 
	 * @param passive should the connection be obtained actively or passively
	 * @param append should the contents be appended to the end of the file
	 * @return a pre-authenticated output stream to the remote file item
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    RemoteOutputStream<?> getOutputStream(String remotePath, boolean passive, boolean append)
			throws IOException, RemoteDataException;
	
	/**
	 * List a remote file item. If the remote file item is not a directory, this is 
	 * essentially equivalent to a stat, save that the response is a single element
	 * {@link List}. When a directory is given, the response will <em>not</em> contain  
	 * refrences to the directory itself or its parent. Thus the UNIX convention of 
	 * including "." and ".." entries in directory listings will not be present.
	 * 
	 * @param remotePath the virtual path to list. 
	 * @return a collection of {@link RemoteFileInfo} representing the contents of the folder or file info
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    List<RemoteFileInfo> ls(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Fetches a remote file or folder at <code>remotePath</code> to the local file system at the 
	 * given <code>localPath</code>. If the destination is a folder, the contents will be placed 
	 * inside. If the destination is a file, the file will be overwritten. The method delegates its 
	 * behavior to {@link #get(String, String, RemoteTransferListener)}
	 * 
	 * @param remotePath the virtual path to write the file item on the remote system
	 * @param localPath path on local file system of the file item to transfer
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void get(String remotePath, String localPath) throws IOException, RemoteDataException;
	
	/**
	 * Fetches a remote file or folder at <code>remotePath</code> to the local file system at the 
	 * given <code>localPath</code>. If the destination is a folder, the contents will be placed 
	 * inside. If the destination is a file, the file will be overwritten. 
	 * 
	 * @param remotePath the virtual path to write the file item on the remote system
	 * @param localPath path on local file system of the file item to transfer
	 * @param listener callback receiving status events about the file action
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void get(String remotePath, String localPath, RemoteTransferListener listener)
			throws IOException, RemoteDataException;
	
	/**
	 * Transfers a local file or folder to the remote system at the given <code>remotePath</code>.
	 * If the destination is a folder, the contents will be placed inside. If the destination is
	 * a file, the file will be overwritten. The delegates its behavior to
	 * {@link #put(String, String, RemoteTransferListener)}
	 *
	 * @param localPath path on local file system of the file item to transfer
	 * @param remotePath the virtual path to write the file item on the remote system
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void put(String localPath, String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Transfers a local file or folder to the remote system at the given <code>remotePath</code>.
	 * If the destination is a folder, the contents will be placed inside. If the destination is
	 * a file, the file will be overwritten.
	 *
	 * @param localPath path on local file system of the file item to transfer
	 * @param remotePath the virtual path to write the file item on the remote system
	 * @param listener callback receiving status events about the file action
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void put(String localPath, String remotePath, RemoteTransferListener listener)
			throws IOException, RemoteDataException;
	
	/**
	 * Returns true if the remote file item referenced by <code>remotePath</code> is a
	 * directory (folder, collection, bucket, virtual folder object, etc). False otherwise. 
	 * While the concept of a directory does not technically apply to not POSIX systems,
	 * the concept is extended across all supported {@link StorageProtocolType}s.
	 *  
	 * @param remotePath the virtual path to check
	 * @return true if the file item is directory-ish, false otherwise 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    boolean isDirectory(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Returns true if the remote file item referenced by <code>remotePath</code> is a
	 * file (object, etc). False otherwise. This will generally respect the referenced
	 * file and directory items when the <code>remotePath</code> points to a  
	 * link or file system mount.
	 *  
	 * @param remotePath the virtual path to check
	 * @return true if the file item is file-ish, false otherwise 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    boolean isFile(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Get the length of the remote file item in bytes. This will not return cumulative 
	 * size of directory contents. The length of a directory will vary from protocol to
	 * protocol.
	 * @param remotePath the virtual path to measure
	 * @return the legnth of the remote file item bytes
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    long length(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Generate a MD5 checksum of the remote file item. This is not supported by all protocols.
	 * 
	 * @param remotePath the virtual path to checksum
	 * @return a MD5 checksum of the file item
	 * @throws IOException
	 * @throws RemoteDataException
	 * @throws NotImplementedException When not supported by the underlying implementation,
	 */
    String checksum(String remotePath) throws IOException, RemoteDataException, NotImplementedException;
	
	/**
	 * Renames a file item from the virtual sourcePath to virtual destPath. This is
	 * identical to a move operation. Specific implementations may perform a two-phase
	 * copy and delete to accomplish this behavior. Some implementations may not preserve
	 * the original data if a failure occurs.
	 * 
	 * @param sourcePath the virtual source path to copy
	 * @param remoteDestPath the virtual destination for the sourcePath
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void doRename(String sourcePath, String remoteDestPath)
	        throws IOException, RemoteDataException, RemoteDataSyntaxException;
	
	/**
	 * Copy a file item from one place to another on the same system, overwriting 
	 * anything in the destination. This method defaults to
	 * {@link #copy(String, String, RemoteTransferListener)}
	 * 
	 * @param sourcePath the virtual source path to copy
	 * @param remoteDestPath the virtual destination for the sourcePath
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void copy(String sourcePath, String remoteDestPath)
	        throws IOException, RemoteDataException, RemoteDataSyntaxException;
	
	/**
	 * Copy a file item from one place to another on the same system, overwriting 
	 * anything in the destination.
	 * 
	 * @param remoteSourcePath the virtual source path to copy
	 * @param remoteDestPath the virtual destination for the sourcePath
	 * @param listener callback receiving status events about the file action
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void copy(String remoteSourcePath, String remoteDestPath, RemoteTransferListener listener)
			throws IOException, RemoteDataException, RemoteDataSyntaxException;
	
	/**
	 * Returns the native protocol-specific URI for a given path.
	 * 
	 * @param remotePath the virtual path to resolve for a remote URI
	 * @return protocol specific uri to the remote file item
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    URI getUriForPath(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Recursively force-deletes a file or folder.
	 * 
	 * @param remotePath the virtual path to delete on the remote system.
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void delete(String remotePath) throws IOException, RemoteDataException;
	
	/**
	 * Used to determine when a direct third-party transfer can be made between two 
	 * systems. Only possible when both {@link RemoteDataClient#isThirdPartyTransferSupported()}
	 * calls return true
	 *
	 * @return true if possible, false otherwise.
	 * @see {@link GridFTP}
	 */
    boolean isThirdPartyTransferSupported();
	
	/**
	 * Closes connection to remote system. Transfers in progress may or may not be killed.
	 * After calling this method, the client will need to call {@link #authenticate()} again
	 * before it is useable.
	 */
    void disconnect();

	/**
	 * Existence check on remote file item
	 * @param remotePath the virtual path for which to check existence
	 * @return true if it exists, false otherwise
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    boolean doesExist(String remotePath) throws IOException, RemoteDataException;

	/**
	 * Resolves a virtualized system path provided by a user to an absolute system path relative
	 * to the system.storage.rootDir. Paths that do not begin with a slash are treated as 
	 * relative paths and are resolved against the system.storage.homeDir.
	 *  
	 * @param remotePath the virtual path to resolve to an absolute system path
	 * @return absolute path on the remote system of the provided virtual path 
	 * @throws FileNotFoundException
	 */
    String resolvePath(String remotePath) throws FileNotFoundException;

	/**
	 * Returns filesystem-agnostic stat information.
	 * 
	 * @param remotePath the virtual path for which to obtain metadata
	 * @return remote file item metadata
	 * @throws RemoteDataException
	 * @throws IOException
	 */
    RemoteFileInfo getFileInfo(String remotePath) throws RemoteDataException, IOException;

	/**
	 * Username used to authenticate to the remote system
	 * @return username or null if other auth mechanisms are used
	 */
    String getUsername();
	
	/**
	 * Hostname of the remote system
	 * @return hostname
	 */
    String getHost();
	
	/**
	 * Updates the virtual home and root directories for this
	 * client's connections.
	 * 
	 * @param rootDir virtual root directory on the system
	 * @param homeDir home directory for relative paths off rootDir
	 */
    void updateSystemRoots(String rootDir, String homeDir);
	
	/**
	 * Returns the home directory relative to the rootDir for
	 * this client
	 * @return virtual home directory of the client 
	 */
    String getHomeDir();
	
	/**
	 * Returns the root directory for this client.
	 * @return virtual root directory of the client
	 */
    String getRootDir();
	
	/**
	 * Synchronizes a local path to a remote system skipping files that already exist with identical
	 * size on the remote system. Note that you must specify the parent folder of the file item you wish
	 * to sync. 
	 *  
	 * @param localAbsolutePath absolute path to local file item
	 * @param remoteParentPath the virtual path on the remote system to the parent directory of the item you want to sync.
	 * @param listener callback receiving status events about the sync
	 * @throws IOException
	 * @throws RemoteDataException
	 */
    void syncToRemote(String localAbsolutePath, String remoteParentPath, RemoteTransferListener listener) throws IOException, RemoteDataException;

	/**
     * Appends the current file to the remote file via the delegated streaming api. This
     * method delegates its call to {@link #append(String, String, RemoteTransferListener).
     * 
     * @param localpath absolute path to local file
     * @param remotepath Agave relative path to remote file
     * @throws IOException
     * @throws FileNotFoundException
     * @throws RemoteDataException upon remote failure or if {@code remotepath} is a directory
     */
    void append(String localpath, String remotepath)
            throws IOException, RemoteDataException;

    /**
     * Appends the current file to the remote file via the delegated streaming api.
     * 
     * @param localpath absolute path to local file
     * @param remotepath Agave relative path to remote file
     * @param listener progress listener
     * @throws IOException
     * @throws FileNotFoundException
     * @throws RemoteDataException upon remote failure or if {@code remotepath} is a directory
     */
    void append(String localpath, String remotepath, RemoteTransferListener listener)
            throws IOException, FileNotFoundException, RemoteDataException;
    
    
}
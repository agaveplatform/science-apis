package org.iplantc.service.transfer.s3;

import static org.jclouds.Constants.PROPERTY_RELAX_HOSTNAME;
import static org.jclouds.Constants.PROPERTY_TRUST_ALL_CERTS;
import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;
import static org.jclouds.io.Payloads.newByteArrayPayload;
import static org.jclouds.s3.reference.S3Constants.PROPERTY_S3_SERVICE_PATH;
import static org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.activation.MimeType;
import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.core.EntityTag;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.RemoteInputStream;
import org.iplantc.service.transfer.RemoteOutputStream;
import org.iplantc.service.transfer.RemoteTransferListener;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.RemoteFilePermission;
import org.iplantc.service.transfer.model.enumerations.PermissionType;
import org.jclouds.ContextBuilder;
//import org.jclouds.aws.s3.AWSS3AsyncClient;
import org.jclouds.atmos.domain.UserMetadata;
import org.jclouds.aws.s3.AWSS3Client;
//import org.jclouds.aws.s3.blobstore.AWSS3BlobStore;
import org.jclouds.azureblob.AzureBlobClient;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.http.HttpException;
import org.jclouds.http.options.GetOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteSourcePayload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
//import org.jclouds.rest.RestContext;
//import org.jclouds.s3.S3AsyncClient;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.S3Client;
import org.jclouds.s3.blobstore.S3BlobStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Module;

public class S3Jcloud implements RemoteDataClient
{
	public static final Logger log = Logger.getLogger(S3Jcloud.class);
	
	public static final String AZURE_STORAGE_PROVIDER = "azureblob";
	public static final String AMAZON_STORAGE_PROVIDER = "aws-s3";
	public static final String OPENSTACK_STORAGE_PROVIDER = "swift";
	public static final String MEMORY_STORAGE_PROVIDER = "transient";
	
	protected String cloudProvider;
	protected String rootDir = "";
	protected String homeDir = "";
	protected String containerName = "";
	protected BlobStoreContext context;
	protected S3BlobStore blobStore = null;
	protected MimetypesFileTypeMap mimetypesFileTypeMap = null;
	private String accountKey = null;
	private String accountSecret = null;
	private String host = null;
	private int port = 443;
	private Map<String, BlobMetadata> fileInfoCache = new ConcurrentHashMap<String, BlobMetadata>();
    
    protected static final int MAX_BUFFER_SIZE = 1*1024*1024;
    
    public S3Jcloud(String accountKey, String accountSecret, String rootDir, String homeDir, String containerName, String host, int port) 
	{
		this.accountSecret = accountSecret;
		this.accountKey = accountKey;
		this.cloudProvider = "aws-s3";
		this.containerName = containerName;
		
		updateEndpoint(host,port);
		
		updateSystemRoots(rootDir, homeDir);
	}
    
	public S3Jcloud(String accountKey, String accountSecret, String rootDir, String homeDir, String containerName) 
	{
		this(accountKey, accountSecret, rootDir, homeDir, containerName, null, -1);
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.RemoteDataClient#getHomeDir()
	 */
	@Override
	public String getHomeDir() {
		return this.homeDir;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.RemoteDataClient#getRootDir()
	 */
	@Override
	public String getRootDir() {
		return this.rootDir;
	}	
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.RemoteDataClient#updateSystemRoots(java.lang.String, java.lang.String)
	 */
	@Override
	public void updateSystemRoots(String rootDir, String homeDir)
	{
		rootDir = FilenameUtils.normalize(rootDir);
        if (!StringUtils.isEmpty(rootDir)) {
			this.rootDir = rootDir;
			if (!this.rootDir.endsWith("/")) {
				this.rootDir += "/";
			}
		} else {
			this.rootDir = "/";
		}

        homeDir = FilenameUtils.normalize(homeDir);
        if (!StringUtils.isEmpty(homeDir)) {
            this.homeDir = this.rootDir +  homeDir;
            if (!this.homeDir.endsWith("/")) {
                this.homeDir += "/";
            }
        } else {
            this.homeDir = this.rootDir;
        }

        this.homeDir = this.homeDir.replaceAll("/+", "/");
        this.rootDir = this.rootDir.replaceAll("/+", "/");
	}
	
	private void updateEndpoint(String host, int port)
	{
		if (StringUtils.isNotEmpty(host))
		{
		    URL endpoint = null;
			
		    try 
		    {
				endpoint = new URL(host);
				if (port > 0) {
					this.port = port;
				} else if (endpoint.getPort() > 0) {
					this.port = endpoint.getPort();
				} else { 
					if (endpoint.getDefaultPort() > 0) {
						this.port = endpoint.getDefaultPort();					
					} else {
						this.port = 80;
					}
					this.host = host;
					return;
				}
				
				this.host = String.format("%s://%s:%d%s", 
						endpoint.getProtocol(),
						endpoint.getHost(),
						this.port,
						endpoint.getPath());
			}
			catch (Exception e) {}
		} else {
			this.host = null;
			this.port = 443;
		}
	}

	@Override
	public void authenticate() throws IOException, RemoteDataException
	{
		if (StringUtils.isEmpty(host) 
		        || !StringUtils.startsWith(host, "http") 
		        || StringUtils.endsWith(host, "amazonaws.com")
		        || StringUtils.endsWith(host, "amazonaws.com:443")) 
		{
			context = ContextBuilder.newBuilder(cloudProvider)
				.credentials(accountKey, accountSecret)
				
                .buildView(BlobStoreContext.class);
		} 
		else 
		{
			try 
			{
			    this.cloudProvider = "s3";
				URL endpoint = new URL(host);
				Properties overrides = new Properties();
				overrides.setProperty(PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
		        overrides.setProperty(PROPERTY_TRUST_ALL_CERTS, "true"); 
		        overrides.setProperty(PROPERTY_RELAX_HOSTNAME, "true");

		        Iterable<Module> modules = ImmutableSet.<Module> of(new SLF4JLoggingModule());
		        
		        
				if (StringUtils.isNotEmpty(endpoint.getPath()) && !StringUtils.equals(endpoint.getPath(), "/")) {
					overrides.setProperty(PROPERTY_S3_SERVICE_PATH, endpoint.getPath());
				} 
				else {
				    overrides.setProperty(PROPERTY_S3_SERVICE_PATH, "/");
				}
				
				context = ContextBuilder.newBuilder(this.cloudProvider)
						.endpoint(endpoint.toString())
						.overrides(overrides)
						.credentials(accountKey, accountSecret)
						.modules(modules)
						.buildView(BlobStoreContext.class);
			} catch (Exception e) {
				throw new RemoteDataException("Failed to parse service endpoint provided in the system.storage.host field.", e);
			}
		}
//		
//		PageSet<? extends StorageMetadata> pageSet = context.getBlobStore().list();
//        for (StorageMetadata storageMetadata: pageSet.toArray(new StorageMetadata[]{})) {
//            System.out.println(storageMetadata.getName());
//        }
        
		if (!getBlobStore().containerExists(containerName)) {
			if (!getBlobStore().createContainerInLocation(null, containerName)) {
				throw new RemoteDataException("Bucket " + containerName + " was not present and could not be created. "
						+ "No data operations can be performed until the bucket is created.");
			}
		}
	}
	
	/**
	 * Returns singleton of current blobstore.
	 * @return
	 */
	public S3BlobStore getBlobStore() throws RemoteDataException
	{
		if (context == null) {
			try {
				authenticate();
//			} catch (RemoteAuthenticationException e) {
			} catch (IOException e) {
				throw new RemoteDataException("Failed to authenticated to S3", e);
			}
		}
		
		if (blobStore == null) {
			blobStore = (S3BlobStore)context.getBlobStore();
		}
		
		return blobStore;
	}

	@Override
	public boolean mkdir(String remotepath)
	throws IOException, RemoteDataException
	{
		if ( remotepath != null && FilenameUtils.getName(remotepath).equals("{") ) {
			throw new RemoteDataException("'{' is not supported as a directory name.");
		}

		String resolvedPath = _doResolvePath(remotepath);


		fileInfoCache.remove(resolvedPath);

		// if the resolved path is empty, that represents the bucket root, which had
		// to have been created for this class to be initialized.
		if (StringUtils.isEmpty(resolvedPath)) {
			// nothing to be done. Return false
			return false;
		}
		else
		{
			// fetch RemoteFileInfo so we only pull metadata once
			RemoteFileInfo fileInfo = null;
			try { fileInfo = getFileInfo(remotepath); } catch (FileNotFoundException ignore){}

			// if not found, check parent and create directory
			if (fileInfo == null) {
				// resolve parent path
				String resolvedParentPath = _doResolvePath(getParentPath(remotepath));
				// if the parent exists, we can create the directory. This is the only scenario in which
				// true can be resolved.
				if (_directoryExists(resolvedParentPath)) {
					try {
//						String resp = getBlobStore().putBlob(
//								containerName,
//								blobStore.blobBuilder(resolvedPath + "/")
//										.type(StorageType.FOLDER)
//										.payload(newByteArrayPayload(new byte[] {}))
//										.contentType("application/x-directory").build());
//
//						log.debug(String.format("Response from creating directory %s: %s", resolvedPath, resp));
						getBlobStore().createDirectory(containerName, resolvedPath);
						return true;
					}
					catch(Throwable t) {
						throw new RemoteDataException("Failed to create " + remotepath, t);
					}
				}
				// no parent, so we throw an exception
				else {
					throw new FileNotFoundException("No such file or directory");
				}
			}
			// if present, but the remote is a file object, throw exception
			else if (fileInfo.isFile()) {
				// handle this by returning false rather than throwing exception for consistency with other
				// RemoteDataClient implementations.
				return false;
			}

			return false;
		}
	}
	
	@Override
    public boolean mkdirs(String remotepath) 
    throws IOException, RemoteDataException 
    {
	    return mkdirs(remotepath, null);
    }

	/**
	 * Safe check for directory existence. This is used rather than the native jcloud check due to the
	 * spotty support for trailing characters and other delimiters in various implementations.
	 * @param resolvedPath resolved path within bucket
	 * @return true if the path is a directory marker, false otherwise
	 * @throws FileNotFoundException
	 * @throws RemoteDataException
	 */
    protected boolean _directoryExists(String resolvedPath)
	throws FileNotFoundException, RemoteDataException {
		BlobMetadata md;
		try {

			if (StringUtils.isEmpty(resolvedPath)) {
				return true;
			}
			else {
				md = getFileMeta(resolvedPath);
				return (md != null && _isDirectoryMarker(md));
			}
		}
		catch (FileNotFoundException | RemoteDataException ignore) {}

		return false;
	}

	@Override
	public boolean mkdirs(String remotepath, String authorizedUsername) 
	throws IOException, RemoteDataException 
	{
		try 
		{	
			String resolvedPath = _doResolvePath(remotepath);

			// nothing to do for empty resolved path. This is the bucket root
			if (StringUtils.isEmpty(resolvedPath)) {
				return false;
			}
			else
			{
				RemoteFileInfo remoteFileInfo = null;
				try { remoteFileInfo = getFileInfo(remotepath); } catch (Exception ignore) {}

				if (remoteFileInfo != null)
				{
					if (remoteFileInfo.isFile()) {
						throw new RemoteDataException("Failed to create " + remotepath + ". File already exists.");
					} else {
						return false;
					}
				}
				else
				{
					String[] pathTokens = StringUtils.split(_doResolvePath(remotepath), "/");
					StringBuilder newdirectories = new StringBuilder();
					
					for(int i=0;i<ArrayUtils.getLength(pathTokens); i++)
					{
						if (StringUtils.isEmpty(pathTokens[i])) continue;
						
						newdirectories.append(pathTokens[i]);
						 
						// if path doesn't exist
						BlobMetadata meta = getFileMeta(newdirectories.toString());
						if (meta == null)
						{
							getBlobStore().createDirectory(containerName, newdirectories.toString());

							if (isPermissionMirroringRequired() && StringUtils.isNotEmpty(authorizedUsername)) {
			                    setOwnerPermission(authorizedUsername, containerName, true);
			                }
						}
						// trying to create directory that is a file
						else if (! _isDirectoryMarker(meta)) {
						    throw new RemoteDataException("Failed to create " + newdirectories + ". File already exists.");
					    }
						else {
							// skipping existing directory
						}
						newdirectories.append("/");
					}
					
					return true;
				}
			}
		} catch (FileNotFoundException e) {
			getBlobStore().createDirectory(containerName, _doResolvePath(remotepath));
			return true;
		} catch (RemoteDataException e) {
			throw e;
		} catch (Exception e) {
			throw new RemoteDataException("Failed to create " + remotepath + " due to error on remote server", e);
		}
	}

	@Override
	public int getMaxBufferSize() {
		return MAX_BUFFER_SIZE;
	}

	@Override
	public RemoteInputStream<?> getInputStream(String remotePath, boolean passive)
	throws IOException, RemoteDataException 
	{
		try 
		{
			if (isFile(remotePath)) {
				return new S3InputStream(getBlobStore().getBlob(containerName, _doResolvePath(remotePath)));
			} else {
				throw new RemoteDataException("Cannot open input stream for directory " + remotePath);
			}
		} 
		catch (RemoteDataException | FileNotFoundException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RemoteDataException("Failed to open input stream to " + remotePath, e);
		}	
	}

	@Override
	public RemoteOutputStream<?> getOutputStream(String remotePath, boolean passive, boolean append)
	throws IOException, RemoteDataException
	{
		try 
		{
			if (doesExist(remotePath)) 
			{
				if (isDirectory(remotePath))
				{
					throw new RemoteDataException("Cannot open output stream to directory " + remotePath);
				}
				else
				{
					Blob currentBlob = getBlobStore().getBlob(containerName, _doResolvePath(remotePath));
					if (currentBlob != null) {
						return new S3OutputStream(this, currentBlob);
					} else {
						throw new RemoteDataException("Failed to open input stream to " + remotePath);
					}
				}
			}
			else if (doesExist(getParentPath(remotePath)))
			{
				return new S3OutputStream(this, _doResolvePath(remotePath));
			}
			else 
			{
				throw new FileNotFoundException("No such file or directory");
			}
		} 
		catch (RemoteDataException|FileNotFoundException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RemoteDataException("Failed to open input stream to " + remotePath, e);
		}	
	}
	
	public RemoteOutputStream<?> getOutputStream(String remotePath, InputStream in)
	throws IOException, RemoteDataException
	{
		try 
		{
			if (doesExist(remotePath)) 
			{
				if (isDirectory(remotePath))
				{
					throw new RemoteDataException("Cannot open output stream to directory " + remotePath);
				}
				else
				{
					Blob currentBlob = getBlobStore().getBlob(containerName, _doResolvePath(remotePath));
					if (currentBlob != null) {
						return new S3OutputStream(this, currentBlob);
					} else {
						throw new RemoteDataException("Failed to open output stream to " + remotePath);
					}
				}
			}
			else if (doesExist(getParentPath(remotePath)))
			{
				return new S3OutputStream(this, _doResolvePath(remotePath));
			}
			else 
			{
				throw new FileNotFoundException("No such file or directory");
			}
		}
		catch (RemoteDataException|FileNotFoundException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RemoteDataException("Failed to open input stream to " + remotePath, e);
		}	
	}

	@Override
	public List<RemoteFileInfo> ls(String remotepath)
	throws IOException, RemoteDataException 
	{
		try 
		{
			List<RemoteFileInfo> listing = new ArrayList<RemoteFileInfo>();
			
			if (isFile(remotepath)) 
			{
				RemoteFileInfo fileInfo = getFileInfo(remotepath);
				listing.add(fileInfo);
			}
			else
			{
				PageSet<? extends StorageMetadata> pageSet = null;
				do 
				{
					String resolvedPath = _doResolvePath(remotepath);
					if (!StringUtils.isEmpty(resolvedPath) && !StringUtils.endsWith(resolvedPath, "/")) {
						resolvedPath += "/";
					}
					
					ListContainerOptions listContainerOptions = new ListContainerOptions();
					if (StringUtils.isNotEmpty(resolvedPath)) {
						listContainerOptions.prefix(resolvedPath);
//						listContainerOptions.inDirectory(resolvedPath);
					}
					listContainerOptions.maxResults(Integer.MAX_VALUE);
					
					if (pageSet != null && pageSet.getNextMarker() != null) {
						listContainerOptions.afterMarker(pageSet.getNextMarker());
					}

					listContainerOptions.withDetails();

					pageSet = getBlobStore().list(containerName, listContainerOptions);
//					pageSet = getBlobStore().list(containerName);
//					String folderName = StringUtils.removeEnd(resolvedPath, "/");
					for (StorageMetadata storageMetadata: pageSet.toArray(new StorageMetadata[]{})) {
						if (storageMetadata == null) {
						    continue;
						} else if (StringUtils.isEmpty(storageMetadata.getName()) ||
								StringUtils.equalsIgnoreCase("/", resolvedPath) ||
								StringUtils.equals(resolvedPath, storageMetadata.getName())) {
						    continue;
						} else {
//						    log.debug(storageMetadata.getType() + " - " + storageMetadata.getName());
							RemoteFileInfo rfi = new RemoteFileInfo((BlobMetadata)storageMetadata);
							listing.add(rfi);
						}
					}
					
				} while (pageSet.getNextMarker() != null);
			}
			
			return listing;	
		} 
		catch (FileNotFoundException e) {
			throw e;
		}
		catch (ContainerNotFoundException e) {
			throw new FileNotFoundException("No such file or directory");
		} 
		catch (RemoteDataException e) {
			throw new RemoteDataException("Failed to list contents of " + remotepath, e);
		}
	}

	@Override
	public void get(String remotedir, String localdir)
	throws IOException, RemoteDataException 
	{
		get(remotedir, localdir, null);
	}

	@Override
	public void get(String remotepath, String localpath, RemoteTransferListener listener) 
	throws IOException, RemoteDataException 
	{
		RemoteFileInfo remoteFileInfo = null;
    	InputStream in = null;
		try
		{
			remoteFileInfo = getFileInfo(remotepath);
		
			if (listener == null) {
//				listener = new RemoteTransferListener(null);
				// we should thorw an exception or force a transfer listener here
				// to capture the info
			}
			
			File localDir = new File(localpath);
			
			if (remoteFileInfo.isDirectory())
			{
				if (!localDir.exists()) 
				{
					// if parent is not there, throw exception
					if (!localDir.getParentFile().exists()) {
						throw new FileNotFoundException("No such file or directory");
					}
					// otherwise we will download folder and give it a new name locally
					else {
						//localDir = new File(localDir, FilenameUtils.getName(remotedir));
					}
				} 
				// can't download folder to an existing file
				else if (localDir.isFile()) 
				{
					throw new RemoteDataException("Cannot download file to " + localpath + ". Local path is a file.");
				}
				// target directory already exists, so we will copy the remote folder into a folder of the same name
				// within the target directory.
				else
				{

					// get the name of the remote directory
					String remoteDirectoryName = FilenameUtils.getName(StringUtils.removeEnd(remotepath, "/"));
					localDir = new File(localDir, remoteDirectoryName);
					
					// create the child directory under the target directory if not already present
					if (!localDir.exists() && !localDir.mkdir()) {
						throw new IOException("Failed to create local download directory");
					}
				}
				
				if (listener != null) {
					listener.started(0, remotepath);
				}
				
				// recursively copy files into the local folder since irods won't let you specify 
				// the target folder name 
				for (RemoteFileInfo fileInfo : ls(remotepath))
				{
					String remoteChild = remotepath + "/" + fileInfo.getName();
					String localChild = localDir.getAbsolutePath() + "/" + fileInfo.getName();
				
					if (fileInfo.isFile()) 
					{	
						
						Blob blob = getBlobStore().getBlob(containerName, _doResolvePath(remoteChild));
						if (blob == null) {
							throw new RemoteDataException("Failed to retrieve remote file " + remoteChild );
						} 
						else 
						{
							if (listener != null) {
								listener.started(blob.getMetadata().getContentMetadata().getContentLength(), remoteChild);
							}
							
							File localFile = new File(localChild);
							in = blob.getPayload().openStream();
							FileUtils.copyInputStreamToFile(in, localFile);
							
							if (listener != null) {
								listener.progressed(localFile.length());
							}
							
						}
					}
					else
					{
						get(remoteChild, localChild, listener); 
					}
				}
				
				if (listener != null) {
					listener.completed();
				}
			}
			else 
			{
				if (!localDir.exists()) 
				{
					if(!localDir.getParentFile().exists()) {
						throw new FileNotFoundException("No such file or directory");
					}
				}
				else if (!localDir.isDirectory()) {
					// nothing to do here. handling links
				} else {
					localDir = new File(localDir.getAbsolutePath(), remoteFileInfo.getName());
				}

				Blob blob = getBlobStore().getBlob(containerName, _doResolvePath(remotepath));
				if (blob == null) {
					throw new RemoteDataException("Failed to get file from " + remotepath );
				} 
				else 
				{
					if (listener != null) {
						listener.started(blob.getMetadata().getContentMetadata().getContentLength(), remotepath);
					}
					
					in = blob.getPayload().openStream();
					FileUtils.copyInputStreamToFile(in, localDir);
					// TODO: set the unix permissions based on the source remote file item's permissino
					// java.nio.file.Files.setPosixFilePermissions(localDir.toPath(), remoteFileInfo.getPosixFilePermissions());

					if (listener != null) {
						listener.progressed(localDir.length());
						listener.completed();
					}
					
				}
			}
		} 
		catch (FileNotFoundException e) {
			if (listener != null) {
				listener.failed();
			}
			throw new FileNotFoundException("No such file or directory");
		}
		catch (ContainerNotFoundException e) {
			if (listener != null) {
				listener.failed();
			}
			throw new FileNotFoundException("No such file or directory");
		} 
		catch (IOException|RemoteDataException e) {
			if (listener != null) {
				listener.failed();
			}
			throw e;
		}
		catch (Exception e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException("Failed to copy file from S3.", e);
		}
		finally {
			try { in.close(); } catch (Exception ignore) {}
		}
	}
	
	/* (non-Javadoc)
     * @see org.iplantc.service.transfer.RemoteDataClient#append(java.lang.String, java.lang.String)
     */
    @Override
    public void append(String localpath, String remotepath) throws IOException,
    RemoteDataException
    {
        append(localpath, remotepath, null);
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.RemoteDataClient#append(java.lang.String, java.lang.String)
     */
    @Override
    public void append(String localpath, String remotepath, RemoteTransferListener listener)  
    throws IOException, RemoteDataException
    {
        File localFile = new File(localpath);
        
        try 
        {
            if (!doesExist(remotepath)) 
            {
                put(localpath, remotepath, listener);
            }
            else if (localFile.isDirectory()) {
                throw new RemoteDataException("cannot append directory");
            }
            else {
                // TODO: implement file appends functionality
                throw new NotImplementedException();
            }
        } 
        catch (IOException | RemoteDataException e) {
            throw e;
        } catch (Exception e) {
            throw new RemoteDataException("Failed to append data to " + remotepath, e);
        }
    }

	@Override
	public void put(String localdir, String remotedir) 
	throws IOException, RemoteDataException 
	{
		put(localdir, remotedir, null);
	}

	@Override
	public void put(String localdir, String remotedir, RemoteTransferListener listener)
 	throws IOException, RemoteDataException 
	{
		try
		{
			File localFile = new File(localdir);
			if (!localFile.exists()) {
				throw new FileNotFoundException("No such file or directory");
			}

			RemoteFileInfo destFileInfo = null;
			// if a file not found exception is thrown, we'll deal with a null object below
			try { destFileInfo = getFileInfo(remotedir); } catch (FileNotFoundException ignore) {}

			if (destFileInfo != null)
			{
				// can't put dir to file
				if (localFile.isDirectory() && destFileInfo.isFile()) {
					throw new RemoteDataException("cannot overwrite non-directory: " + remotedir + " with directory " + localFile.getName());
				} 
				else 
				{
					remotedir += (StringUtils.isEmpty(remotedir) ? "" : "/") + localFile.getName();
				
					if (localFile.isDirectory()) {
						mkdir(remotedir);
					}
				}
			}
			else if (doesExist(getParentPath(remotedir)))
			{
				if (localFile.isDirectory())
					mkdir(remotedir);
			}
			else
			{
				// upload and keep name.
				throw new FileNotFoundException("No such file or directory");
			}
			
			if (localFile.isDirectory()) {
				// TODO: we can speed this up using a stream to avoid the overhead of recursive calls.
//				java.nio.file.Files.find(localFile.toPath(), Integer.MAX_VALUE, (path, basicFileAttributes) -> basicFileAttributes.isDirectory())
//						.forEach(d -> ...);

				for (File child: localFile.listFiles()) {
					String remoteChildDir = remotedir + "/" + child.getName();
					if (child.isDirectory())
					{
						// create the remote directory regardless. this save a roundtrip
						// for metadata on the path
						mkdir(remoteChildDir);

						// clear the cache and recursively process the directory.
						fileInfoCache.remove(containerName + "/" + remoteChildDir);

						put(child.getAbsolutePath(), remoteChildDir, listener);
					} else {
					    fileInfoCache.remove(containerName + "/" + remotedir);
						put(child.getAbsolutePath(), remotedir, listener);
					}
				}
			} else { 
			
				ByteSource byteSource = Files.asByteSource(localFile);
				Payload payload = new ByteSourcePayload(byteSource);
				String mimeType = resolveMimeTypeOfFile(localFile);

				String resolvedPath = _doResolvePath(remotedir);

				Blob blob = getBlobStore().blobBuilder(resolvedPath)
					.payload(payload)
				 	.contentLength(localFile.length())
					.contentDisposition(FilenameUtils.getName(resolvedPath))
					.contentMD5(payload.getContentMetadata().getContentMD5AsHashCode())
					.contentType(mimeType)
					.build();

				if (destFileInfo != null) {
					blob.getMetadata().setCreationDate(new Date());
				}

				if (listener != null) {
					listener.started(localFile.length(), remotedir);
				}
				
				fileInfoCache.remove(containerName + "/" + resolvedPath);
				getBlobStore().putBlob(containerName, blob, multipart());
				
				if (listener != null) {
					listener.progressed(localFile.length());
				}
			}
			
			if (listener != null) {
				listener.completed();
			}
		} 
		catch (FileNotFoundException|RemoteDataException e) {
			if (listener != null) {
				listener.failed();
			}
			throw e;
		} 
		catch (IllegalArgumentException e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException("cannot overwrite non-directory: " + remotedir + " with directory " + localdir);
		} 
		catch (Exception e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException("Remote put failed.", e);
		}
	}
	
	@Override
	public void syncToRemote(String localdir, String remotedir, RemoteTransferListener listener) 
	throws IOException, RemoteDataException
	{
		File sourceFile = new File(localdir);
		if (!sourceFile.exists()) {
			throw new FileNotFoundException("No such file or directory");
		}
		
		try
		{
            if (!doesExist(remotedir)) 
			{
            	put(localdir, remotedir, listener);
				return;
			}
            else if (sourceFile.isDirectory()) 
			{	
				String adjustedRemoteDir = remotedir;
				
				// can't put dir to file
				if (!isDirectory(adjustedRemoteDir)) {
					delete(adjustedRemoteDir);
					put(localdir, adjustedRemoteDir, listener);
					return;
				} 
				else 
				{
					adjustedRemoteDir += (StringUtils.isEmpty(remotedir) ? "" : "/") + sourceFile.getName();
				}
				
				for (File child: sourceFile.listFiles())
				{
					String childRemotePath = adjustedRemoteDir + "/" + child.getName();
					if (child.isDirectory()) 
					{
						// local is a directory, remote is a file. delete remote file. we will replace with local directory
						try {
							if (isFile(childRemotePath)) {
								delete(childRemotePath);
							}
						} catch (FileNotFoundException ignore) {}
						
						// now create the remote directory
						mkdir(childRemotePath);
						
						// sync the folder now that we've cleaned up
						syncToRemote(child.getAbsolutePath(), adjustedRemoteDir, listener);
					} 
					else
					{
						syncToRemote(child.getAbsolutePath(), childRemotePath, listener);
					}
				}
			} 
			else 
			{
				// sync if file is not there
				if (!doesExist(remotedir))  
				{	
					ByteSource payload = Files.asByteSource(sourceFile);
					String resolvedPath = _doResolvePath(remotedir);
					// TODO: sync unix permission info if available
					Blob blob = getBlobStore().blobBuilder(resolvedPath)
							  .payload(payload)
							  .contentLength(sourceFile.length())
							  .contentType(new MimetypesFileTypeMap().getContentType(sourceFile))
							  .contentMD5((HashCode)null)
							  .build();
					
					if (listener != null) {
						listener.started(sourceFile.length(), remotedir);
					}
					
					fileInfoCache.remove(containerName + "/" + resolvedPath);
	                getBlobStore().putBlob(containerName, blob, multipart());
					
					if (listener != null) {
						listener.progressed(sourceFile.length());
					}
				}
				else 
				{	
				    String resolvedPath = _doResolvePath(remotedir);
					BlobMetadata blobMeta = getFileMeta(resolvedPath);
					if (blobMeta == null)
					{
						ByteSource payload = Files.asByteSource(sourceFile);
						// TODO: sync unix permission info if available
						Blob blob = getBlobStore().blobBuilder(resolvedPath)
								  .payload(payload)
								  .contentLength(sourceFile.length())
								  .contentType(new MimetypesFileTypeMap().getContentType(sourceFile))
								  .contentMD5((HashCode)null)
								  .build();
						
						if (listener != null) {
							listener.started(sourceFile.length(), remotedir);
						}
						
						fileInfoCache.remove(containerName + "/" + resolvedPath);
	                    getBlobStore().putBlob(containerName, blob, multipart());
						
						if (listener != null) {
							listener.progressed(sourceFile.length());
						}
					}
					// if the types mismatch, delete remote, use current
					else if (sourceFile.isDirectory() && ! _isDirectoryMarker(blobMeta) ||
							sourceFile.isFile() && _isDirectoryMarker(blobMeta))
					{
						delete(remotedir);
						ByteSource payload = Files.asByteSource(sourceFile);
						// TODO: sync unix permission info if available
						Blob blob = getBlobStore().blobBuilder(resolvedPath)
								  .payload(payload)
								  .contentLength(sourceFile.length())
								  .contentType(new MimetypesFileTypeMap().getContentType(sourceFile))
								  .contentMD5((HashCode)null)
								  .build();
						
						if (listener != null) {
							listener.started(sourceFile.length(), remotedir);
						}
						
						fileInfoCache.remove(containerName + "/" + resolvedPath);
	                    getBlobStore().putBlob(containerName, blob, multipart());
						
						if (listener != null) {
							listener.progressed(sourceFile.length());
						}
					}
					// or if the hashes or file sizes are different,  use current
					else if (sourceFile.length() != blobMeta.getContentMetadata().getContentLength())  
					{
						ByteSource payload = Files.asByteSource(sourceFile);
						// TODO: sync unix permission info if available
						Blob blob = getBlobStore().blobBuilder(resolvedPath)
								  .payload(payload)
								  .contentLength(sourceFile.length())
								  .contentType(new MimetypesFileTypeMap().getContentType(sourceFile))
								  .contentMD5((HashCode)null)
								  .build();
						
						if (listener != null) {
							listener.started(sourceFile.length(), remotedir);
						}
						
						fileInfoCache.remove(containerName + "/" + resolvedPath);
	                    getBlobStore().putBlob(containerName, blob, multipart());
						
						if (listener != null) {
							listener.progressed(sourceFile.length());
						}
					} 
					else 
					{
						log.debug("Skipping transfer of " + sourceFile.getPath() + " to " + 
								remotedir + " because file is present and of equal size.");
					}
				}
				
				if (listener != null) {
					listener.completed();
				}
			}
		} 
		catch (FileNotFoundException | RemoteDataException e) {
			if (listener != null) {
				listener.failed();
			}
			throw e;
		} 
		catch (IllegalArgumentException e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException("cannot overwrite non-directory: " + remotedir + " with directory " + localdir);
		} catch (Exception e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException("Remote put failed.", e);
		}
	}

	@Override
	public boolean isDirectory(String path)
 	throws IOException, RemoteDataException 
	{
		return getFileInfo(path).isDirectory();
	}

	@Override
	public boolean isFile(String path)
	throws IOException, RemoteDataException
	{
		return getFileInfo(path).isFile();
	}

	@Override
	public long length(String path)
 	throws IOException, RemoteDataException 
	{
		return getFileInfo(path).getSize();
	}

	@Override
	public String checksum(String remotepath)
 	throws IOException, RemoteDataException, NotImplementedException
	{
		try
		{
			if (isDirectory(remotepath)) {
				throw new RemoteDataException("Directory cannot be checksummed");
			} else {
				// TODO: enable checksum on s3 files. Need to ensure these are present by default in s3
//				BlobMetadata blobMetadata = getFileMeta(remotepath, true);
//				ContentMetadata contentMetadata = blobMetadata.getContentMetadata();
//				if (contentMetadata != null) {
//					HashCode hashCode = contentMetadata.getContentMD5AsHashCode();
//					if (hashCode != null) {
//						return hashCode.toString();
//					} else {
//						throw new RemoteDataException("No checksum found for " + remotepath);
//					}
//				} else {
//					throw new RemoteDataException("Content metadata is not available");
//				}
				throw new NotImplementedException();
			}
		}
		catch (IOException|RemoteDataException|NotImplementedException e) {
			throw e;
		}
	}

	@Override
	public void doRename(String srcPath, String destPath) 
	throws IOException, RemoteDataException 
	{
		try {
			// check fileinfo for the destPath. If not present, it throws a file not found
			// exception.
			RemoteFileInfo destFileInfo = getFileInfo(destPath);

			// If a value is returned, we return a message describing the conflict.
			if (destFileInfo.isFile()) {
				throw new RemoteDataException(String.format("Cannot rename %s to %s. " +
						"A file already exists at the destination path.", srcPath, destPath));
			}
			else {
				throw new RemoteDataException(String.format("Cannot rename %s to %s. " +
						"A directory already exists at the destination path.", srcPath, destPath));
			}
		} catch (FileNotFoundException ignore) {
			// No object found at destPath, rename is possible. Perform a copy operation
			doCopy(srcPath, destPath, null, true);
		}
	}

	@Override
	public void copy(String remotedir, String localdir)  
	throws IOException, RemoteDataException 
	{
		copy(remotedir, localdir, null);
	}

	@Override
	public void copy(String srcPath, String destPath, RemoteTransferListener listener) 
	throws IOException, RemoteDataException 
	{
		doCopy(srcPath, destPath, listener, false);
	}
	
	@SuppressWarnings({ "unused", "deprecation" })
	private void doCopy(String srcPath, String destPath, RemoteTransferListener listener, boolean deleteSource)
	throws IOException, RemoteDataException 
	{
		RemoteFileInfo sourceFileInfo = null;
		RemoteFileInfo destFileInfo = null;
		try  
		{
			String resolvedSourcePath = _doResolvePath(srcPath); 
			resolvedSourcePath = StringUtils.removeEnd(resolvedSourcePath, "/");
			String resolvedSourceParentPath = _doResolvePath(getParentPath(srcPath));
			String destParentPath = getParentPath(destPath);
			String resolvedDestParentPath = _doResolvePath(destParentPath);
			resolvedDestParentPath = StringUtils.removeEnd(resolvedDestParentPath, "/");

			sourceFileInfo = getFileInfo(srcPath);
			// if a file not found exception is thrown, we'll deal with a null object below
			try { destFileInfo = getFileInfo(destPath); } catch (FileNotFoundException ignore) {}

			if (sourceFileInfo.isFile())
			{
				if (destFileInfo != null && ! destFileInfo.isFile()) {
					throw new RemoteDataException(String.format("Cannot copy %s to %s. " +
							"A directory already exists with the same name.", srcPath, destPath));
				}
			}
			// source is directory and destination path exists
			else if (destFileInfo != null)
			{
				// dest is a file and source is a folder. that cannot happen on a rename
				if (destFileInfo.isFile()) {
					throw new RemoteDataException(String.format("Cannot copy %s to %s. " +
							"A file already exists with the same name.", srcPath, destPath));
				}
				// dest is a dir and source is  a folder. that cannot happen on a rename
				else {
					if (StringUtils.isEmpty(destPath)) {
						destPath = sourceFileInfo.getName();
//						resolvedDestPath = _doResolvePath(sourceFileInfo.getName());
					} else {
						destParentPath = destPath;
						destPath += "/" + sourceFileInfo.getName();
//					    resolvedDestParentPath = resolvedDestPath;
//						resolvedDestPath += "/" + sourceFileInfo.getName();
					}
				}
			}
			else if (!doesExist(destParentPath))
			{
				throw new FileNotFoundException("No such file or directory");
			}
			else 
			{
				// if the source directory is being copied to the home directory, use the name
				// of the source directory as the name of the new folder.
				if (StringUtils.isEmpty(destParentPath)
						&& !StringUtils.endsWith(_doResolvePath(srcPath), "/"))
				{
					destPath = sourceFileInfo.getName();
//					resolvedDestPath = _doResolvePath(sourceFileInfo.getName());
				}
			}

			String resolvedDestPath = StringUtils.removeEnd(_doResolvePath(destPath), "/");

			if (StringUtils.equals(resolvedSourcePath, resolvedDestPath)) {
                throw new RemoteDataException(String.format("Cannot rename %s to %s. The original and new names must be different within the same bucket.", srcPath, destPath));
            }
            else if (!StringUtils.equals(resolvedDestParentPath, resolvedSourceParentPath) && 
			        StringUtils.startsWith(resolvedDestParentPath, resolvedSourcePath)) {
				throw new RemoteDataException(String.format("Cannot rename %s to %s. A file or director cannot be moved into its own subtree within the same bucket.", srcPath, destPath));
			}

        	if (sourceFileInfo.isDirectory()) 
        	{
        		// create the target directory prior to copying over all the assets
				mkdirs(destPath);

                PageSet<? extends StorageMetadata> pageset = null;
                do 
        		{
        			// we take this approach to avoid getting a preceding slash or  trailing double slash
					if (!StringUtils.isEmpty(resolvedSourcePath) && !StringUtils.endsWith(resolvedSourcePath, "/")) {
                		resolvedSourcePath += "/";
					}

        			ListContainerOptions options = new ListContainerOptions();
        			options.inDirectory(resolvedSourcePath);
        			options.withDetails();
        			options.recursive();
        			if (pageset != null && StringUtils.isEmpty(pageset.getNextMarker())) {
        				options.afterMarker(pageset.getNextMarker());
        			}
        			
        			pageset = getBlobStore().list(containerName, options);
        			
        			for (StorageMetadata meta: pageset.toArray(new StorageMetadata[]{})) {
        				if (meta == null) continue;
//        				log.debug(meta.getType().name() + " - " + meta.getName());
        				String destsubfolder = StringUtils.replaceOnce(meta.getName(), resolvedSourcePath, resolvedDestPath);
        				
        				if (_isDirectoryMarker((BlobMetadata)meta))
        				{
        					// creating remote destination folder
        					mkdirs(destsubfolder);
        					if (deleteSource) 
        						getBlobStore().deleteDirectory(containerName, meta.getName());
        				}
        				else 
        				{
        					if (listener != null) {
								listener.started(((BlobMetadata)meta).getContentMetadata().getContentLength(), destsubfolder + "/" + meta.getName());
							}
        					
        					// copying source file
        					getBlobStore().copyBlob(containerName, meta.getName(), containerName, destsubfolder, CopyOptions.NONE);
        					
        					if (listener != null) {
        						listener.progressed(((BlobMetadata)meta).getContentMetadata().getContentLength());
        					}
        					
        					if (deleteSource) 
        						getBlobStore().removeBlob(containerName, meta.getName());
        				}
        			}
        		} 
        		while (!StringUtils.isEmpty(pageset.getNextMarker()));
        	}
            else {
				if (listener != null) {
					listener.started(sourceFileInfo.getSize(), resolvedDestPath);
				}

				// Copy over the root item.
				getBlobStore().copyBlob(containerName, resolvedSourcePath, containerName, resolvedDestPath, CopyOptions.NONE);

				if (listener != null) {
					listener.progressed(sourceFileInfo.getSize());
				}
			}
            
            if (deleteSource) 
            	getBlobStore().deleteDirectory(containerName, resolvedSourcePath);
    		
            if (listener != null) {
				listener.completed();
			}
		}
		catch (RemoteDataException | IOException e) {
			if (listener != null) {
				listener.failed();
			}
			throw e;
		}
		catch (Throwable e) {
			if (listener != null) {
				listener.failed();
			}
			throw new RemoteDataException(String.format("Internal error when attempting to copy %s to %s on the remote server.", srcPath, destPath), e);
		}
		finally {}
	}

	@Override
	public URI getUriForPath(String path)
	throws IOException, RemoteDataException 
	{
		try
		{
			return new URI("s3://" + host + (port == 80 || port == 443 ? "" : ":" + port) + "/" + path);
		}
		catch (URISyntaxException e)
		{
			throw new IOException(e);
		}
	}

	@Override
	public void delete(String path) 
	throws IOException, RemoteDataException 
	{
		String resolvedPath = null;
//		String encodedPath = null;
		try 
		{
			resolvedPath = _doResolvePath(path);
			if (isFile(path))
			{
			    fileInfoCache.remove(containerName + "/" + resolvedPath);
			    getBlobStore().removeBlob(containerName, resolvedPath);
			}
			else
			{
				PageSet<? extends StorageMetadata> pageset = null;
//				encodedPath = urlEncodeResolvedPathComponents(resolvedPath);

				ListContainerOptions options = new ListContainerOptions();
				options.prefix(resolvedPath);
				options.delimiter("/");
				options.recursive();

				ArrayList<String> fileItems = new ArrayList<String>();

				do
				{
					fileItems.clear();

					if (pageset != null && StringUtils.isEmpty(pageset.getNextMarker())) {
						options.afterMarker(pageset.getNextMarker());
					}

					pageset = getBlobStore().list(containerName, options);

					for (StorageMetadata meta: pageset.toArray(new StorageMetadata[]{}))  {
						fileItems.add(meta.getName());
						fileInfoCache.remove(containerName + "/" + meta.getName());

//						if (_isDirectoryMarker((BlobMetadata)meta)) {
//						    fileInfoCache.remove(containerName + "/" + meta.getName());
//							getBlobStore().deleteDirectory(containerName, meta.getName());
//						}
//						else {
//						    fileInfoCache.remove(containerName + "/" + meta.getName());
//						    getBlobStore().removeBlob(containerName, meta.getName());
//						}
					}

					// perform a bulk delete operation
					getBlobStore().removeBlobs(containerName, fileItems);
				} 
				while (!StringUtils.isEmpty(pageset.getNextMarker()));

				fileInfoCache.remove(containerName + "/" + resolvedPath);
				getBlobStore().deleteDirectory(containerName, resolvedPath);
//				getBlobStore().deleteDirectory(containerName, encodedPath);
			}
		}
		catch (FileNotFoundException e) {
			throw e;
		}
		catch (ContainerNotFoundException e) {
			throw new FileNotFoundException("No such file or directory");
		} 
		catch (Exception e)
		{
			throw new RemoteDataException("Failed to delete " + path, e);
		}
	}

//	/**
//	 * URL encodes supported special charaters in the path. This handles characters on a char
//	 * by char basis, so use of url-encoded paths will be double encoded.
//	 *
//	 * @param resolvedPath
//	 * @return
//	 */
//	private String urlEncodeResolvedPathComponents(String resolvedPath) {
////		String[] SPECIAL_CHARS = new String[]{ " ", "_", "-", "!", "@", "#", "$", "%", "^", "*", "(", ")",
////				"+", "[", "]", "{", "}", ":", "."};
//		Map<String, String> specialChars = Map.ofEntries(
//				new AbstractMap.SimpleEntry<String, String>(" ","+"),
//				new AbstractMap.SimpleEntry<String, String>("!", "%21"),
//				new AbstractMap.SimpleEntry<String, String>("@", "%40"),
//				new AbstractMap.SimpleEntry<String, String>("#", "%23"),
//				new AbstractMap.SimpleEntry<String, String>("$", "%24"),
//				new AbstractMap.SimpleEntry<String, String>("%", "%25"),
//				new AbstractMap.SimpleEntry<String, String>("^", "%5E"),
//				new AbstractMap.SimpleEntry<String, String>("(", "%28"),
//				new AbstractMap.SimpleEntry<String, String>(")", "%29"),
//				new AbstractMap.SimpleEntry<String, String>("+", "%2B"),
//				new AbstractMap.SimpleEntry<String, String>("[", "%5B"),
//				new AbstractMap.SimpleEntry<String, String>("]", "%5D"),
//				new AbstractMap.SimpleEntry<String, String>("{", "%7B"),
//				new AbstractMap.SimpleEntry<String, String>("}", "%7D"),
//				new AbstractMap.SimpleEntry<String, String>(":", "%3A")
//		);
//
//		StringBuilder sb = new StringBuilder(resolvedPath.length());
//		Charset charset;
//		CharArrayWriter charArrayWriter = new CharArrayWriter();
//
//		for (char c: resolvedPath.toCharArray()) {
//			String s = String.valueOf(c);
//			sb.append(specialChars.getOrDefault(s, s));
//		}
//
//		return sb.toString();
//	}

	/**
	 * Checks whether the BlobMetadata object has a content type of application/directory.
	 * This is the default directory marker across BlobStore implementations.
	 *
	 * @param blobMetadata
	 * @return
	 */
	protected boolean _isDirectoryMarker(BlobMetadata blobMetadata) {
		return ArrayUtils.contains(new String[]{"application/directory", "application/x-directory"},
			blobMetadata.getContentMetadata().getContentType());
	}

	@Override
	public boolean isThirdPartyTransferSupported() 
	{
		// TODO: support server side copies
		return false;
	}

	@Override
	public void disconnect() {
		if (context != null) {
			context.close();
		}
		blobStore = null;
		context = null;
	}

	@Override
	public boolean doesExist(String remotePath) 
	throws IOException, RemoteDataException 
	{
		String resolvedPath = null;
		
		try 
		{
			resolvedPath = _doResolvePath(remotePath);
			if (StringUtils.isEmpty(resolvedPath)) {
				return true;
			} 
			else 
			{
				BlobMetadata meta = getFileMeta(resolvedPath, true);
				return meta != null;
			}
		}
		catch (HttpException e) {
			return false;
		}
		catch (FileNotFoundException e) {
			throw e;
		}
		catch (ContainerNotFoundException e) {
			throw new FileNotFoundException("No such file or directory");
		} 
		catch (UncheckedExecutionException e) {
		    if (e.getCause() instanceof org.jclouds.rest.AuthorizationException) {
		        return true;
		    } else {
		        throw new RemoteDataException("Failed to retrieve information for " + remotePath, e);
		    }
		}
		catch (Exception e)
		{
			throw new RemoteDataException("Failed to retrieve information for " + remotePath, e);
		}
	}

	protected String _doResolvePath(String path) throws FileNotFoundException {
	    return StringUtils.strip(resolvePath(path), "/");
	}
	
	@Override
    public String resolvePath(String path) throws FileNotFoundException {
		if (StringUtils.isBlank(path)) {
		    return homeDir;
//			return StringUtils.removeStart(homeDir, "/");
		}
		else if (path.startsWith("/")) 
		{
			path = rootDir + path.replaceFirst("/", "");
		}
		else
		{
			path = homeDir + path;
		}
		
		String adjustedPath = path;
		if (adjustedPath.endsWith("/..") || adjustedPath.endsWith("/.")) {
			adjustedPath += File.separator;
		}
		
		if (adjustedPath.startsWith("/")) {
		    path = org.codehaus.plexus.util.FileUtils.normalize(adjustedPath);
		} else {
			path = FilenameUtils.normalize(adjustedPath);
		}
		
		if (path == null) {
			throw new FileNotFoundException("The specified path " + path + 
					" does not exist or the user does not have permission to view it.");
		} else if (!path.startsWith(rootDir)) {
			if (!path.equals(StringUtils.removeEnd(rootDir, "/"))) {
				throw new FileNotFoundException("The specified path " + path + 
					" does not exist or the user does not have permission to view it.");
			}
		}
		
		return StringUtils.trimToEmpty(path);
	}
	
	public String getParentPath(String path) {
	    
		if (StringUtils.isEmpty(path)) {
			return "../";
		}
		else if (StringUtils.contains(path, '/')) {
		    return path + "/..";
		}
		else {
			return "";
		}
	}

	/**
	 * Convenience method to work around AWS being picky about trailing slashes
	 * and to cache the result for quicker complex operations.
	 * 
	 * @param resolvedPath resolved path within the s3 container
	 * @return
	 */
	private BlobMetadata getFileMeta(String resolvedPath) 
    throws FileNotFoundException, RemoteDataException
    {
	    return getFileMeta(resolvedPath, false);
    }
	
	/**
     * Convenience method to work around AWS being picky about trailing slashes.
     * @param resolvedPath the actual remote path to the file
     * @param forceCheck whether to break the cache and force a remote check
     * @return
     */
    private BlobMetadata getFileMeta(String resolvedPath, boolean forceCheck) 
	throws RemoteDataException
	{
	    BlobMetadata blobMeta = fileInfoCache.get(containerName + "/" + resolvedPath);
		if (blobMeta == null || forceCheck)
		{
		    try 
		    {
		    	blobMeta = getBlobStore().blobMetadata(containerName, resolvedPath);
		    	// if a value was returned and the name matches the path, then we're good. otherwise, it could
				// be a shadow object that s3 delegated to the parent bucket. Happens when requesting an object
				// of name "{"
    		    if (blobMeta != null) {
        			// cool, found it
        		    fileInfoCache.put(containerName + "/" + resolvedPath, blobMeta);
        		} else if (!StringUtils.endsWith(resolvedPath, "/")) {
        		    blobMeta = getBlobStore().blobMetadata(containerName, resolvedPath + "/");
        		    if (blobMeta != null) {
//        		    	if (!blobMeta.getUri().getPath().substring(1).equals(blobMeta.getName())) {
//							log.debug(String.format("False object returned by S3 for %s/%s. " +
//											"Object name does not reflect the URL. This is a ghost metadata item",
//									containerName, resolvedPath));
//							return null;
//						}
//        		    	else {
							fileInfoCache.put(containerName + "/" + resolvedPath, blobMeta);
//						}
        		    }
        		    else {
        		    	return null;
					}
        		} else {
        			return null;
        		}
		    } catch (ContainerNotFoundException e) {
		        // this should not be reached due to the check during auth
		    	fileInfoCache.remove(containerName + "/" + resolvedPath);
		        // throw new FileNotFoundException("No such file or directory");
		    }
		}
		return blobMeta;
	}
	
	@Override
	public RemoteFileInfo getFileInfo(String path) 
	throws RemoteDataException, FileNotFoundException
	{
	    String resolvedPath = _doResolvePath(path);
		try 
		{
			RemoteFileInfo fileInfo = null;
			if (StringUtils.isEmpty(resolvedPath)) {
				if (getBlobStore().containerExists(containerName)) {
					fileInfo = new RemoteFileInfo();
					fileInfo.setName("/");
					fileInfo.setFileType(RemoteFileInfo.DIRECTORY_TYPE);
					fileInfo.setLastModified(new Date());
					fileInfo.setOwner(RemoteFileInfo.UNKNOWN_STRING);
					fileInfo.setSize(0);
				} else {
					throw new FileNotFoundException("No such file or directory");
				}
			} 
			else
			{
				BlobMetadata blobMetadata = getFileMeta(resolvedPath);
				if (blobMetadata == null) 
				{
					throw new FileNotFoundException("No such file or directory");
				} else {
					fileInfo = new RemoteFileInfo(blobMetadata);
					String remoteDirectoryName = FilenameUtils.getName(StringUtils.removeEnd(resolvedPath, "/"));
					fileInfo.setName(remoteDirectoryName);
				}
			}
			
			return fileInfo;
		}
		catch (FileNotFoundException e) {
			throw e;
		}
		catch (Exception e)
		{
			throw new RemoteDataException("Failed to retrieve information for " + path, e);
		}
	}

	@Override
	public String getUsername() {
		return this.accountKey;
	}

	@Override
	public String getHost() {
		return this.host;
	}

	@Override
	public List<RemoteFilePermission> getAllPermissionsWithUserFirst(String path, String username) 
	throws RemoteDataException 
	{
		
		return Arrays.asList(new RemoteFilePermission(username, null, PermissionType.ALL, true));
	}

	@Override
	public List<RemoteFilePermission> getAllPermissions(String path)
	throws RemoteDataException 
	{
		return new ArrayList<RemoteFilePermission>();
	}

	@Override
	public PermissionType getPermissionForUser(String username, String path)
	throws RemoteDataException 
	{	
		return PermissionType.ALL;
	}

	@Override
	public boolean hasReadPermission(String path, String username)
	throws RemoteDataException 
	{
		return true;
	}

	@Override
	public boolean hasWritePermission(String path, String username)
	throws RemoteDataException 
	{
		return true;
	}

	@Override
	public boolean hasExecutePermission(String path, String username)
	throws RemoteDataException 
	{
		return true;
	}

	@Override
	public void setPermissionForUser(String username, String path, PermissionType type, boolean recursive) 
	throws RemoteDataException 
	{
		
	}

	@Override
	public void setOwnerPermission(String username, String path, boolean recursive) 
	throws RemoteDataException 
	{	
	}

	@Override
	public void setReadPermission(String username, String path, boolean recursive)
	throws RemoteDataException 
	{
	}

	@Override
	public void removeReadPermission(String username, String path, boolean recursive)
	throws RemoteDataException 
	{	
	}

	@Override
	public void setWritePermission(String username, String path, boolean recursive)
	throws RemoteDataException 
	{	
	}

	@Override
	public void removeWritePermission(String username, String path, boolean recursive)
	throws RemoteDataException 
	{	
	}

	@Override
	public void setExecutePermission(String username, String path, boolean recursive)
	throws RemoteDataException 
	{
	}

	@Override
	public void removeExecutePermission(String username, String path, boolean recursive) 
	throws RemoteDataException 
	{
	}

	@Override
	public void clearPermissions(String username, String path, boolean recursive)
	throws RemoteDataException 
	{
	}

	@Override
	public String getPermissions(String path) throws RemoteDataException 
	{
		RemoteFileInfo file;
		try 
		{
			file = getFileInfo(path);
			
			if (file == null) {
				throw new RemoteDataException("No file found at " + path);
			} else {
				return file.getPermissionType().name();
			}
		} 
		catch (IOException e) 
		{
			throw new RemoteDataException("No file found at " + path);
		}
	}

	@Override
	public boolean isPermissionMirroringRequired() {
		return false;
	}

	/**
	 * Determines the mime type for a local file. The file must already exist on disk.
	 * Mime type is determined by the file name. If the file extension is not recognized,
	 * a value of "application/octet-stream" is returned.
	 *
	 * @param localFile the local file to for which to get the mimetype.
	 * @return the mime type of the file item or "application/octet-stream" if unknown
	 */
	protected String resolveMimeTypeOfFile(File localFile){
    	return getMimetypesFileTypeMap().getContentType(localFile.getAbsolutePath());
	}

	/**
	 * Loads the known mime types as a map. Values are read from a mime.types file in the classpath.
	 * If the file cannot be found, the default system file is loaded. If that is not found, all
	 * Files wind up being resolved as "application/octet-stream"
	 * @return
	 */
	protected MimetypesFileTypeMap getMimetypesFileTypeMap() {
    	if (mimetypesFileTypeMap == null) {
			InputStream mimeTypesStream = null;
			try {
				mimeTypesStream = this.getClass().getClassLoader().getResourceAsStream("mime.types");
				mimetypesFileTypeMap = new MimetypesFileTypeMap(mimeTypesStream);
			} catch (Exception e) {
				log.error("Unable to load bundled mime.types file. Falling back on system file.", e);
				// Try again on a different map.
				mimetypesFileTypeMap = new MimetypesFileTypeMap();
			} finally {
				if (mimeTypesStream != null) try {mimeTypesStream.close();} catch (Exception ignore){}
			}
		}

    	return mimetypesFileTypeMap;
	}
}

package org.iplantc.service.transfer;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.ietf.jgss.GSSCredential;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.AgaveUriRegex;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.systems.util.ApiUriUtil;
import org.iplantc.service.transfer.azure.AzureJcloud;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.ftp.FTP;
import org.iplantc.service.transfer.gridftp.GridFTP;
import org.iplantc.service.transfer.http.HTTP;
import org.iplantc.service.transfer.irods.IRODS;
import org.iplantc.service.transfer.irods4.IRODS4;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.s3.S3Jcloud;
import org.iplantc.service.transfer.sftp.SftpRelay;
import org.irods.jargon.core.connection.AuthScheme;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Date;

public class RemoteDataClientFactory {
    
    private static final Logger log = Logger.getLogger(RemoteDataClientFactory.class);

	/**
	 * Creates a pre-configured {@link RemoteDataClient} for the given {@link RemoteSystem} 
	 * that can be used to connect to remote systems.
	 * 
	 * @param system the system from which to generate a {@link RemoteDataClient}
	 * @param internalUsername the internal username of the user requesting the client.
	 * @return a valid instance of a {@link RemoteDataClient} for the schema of the given {@code uri}.
	 * @throws RemoteCredentialException if the credentials for the system represented by the URI cannot be found/refreshed/obtained
	 * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
	 */
	public RemoteDataClient getInstance(RemoteSystem system, String internalUsername) 
	throws RemoteDataException, RemoteCredentialException
	{
		try 
		{
			AuthConfig userAuthConfig = system.getStorageConfig().getAuthConfigForInternalUsername(internalUsername);
			if (userAuthConfig != null) 
			{  
				String salt = system.getSystemId() + system.getStorageConfig().getHost() + userAuthConfig.getUsername();
				String username = userAuthConfig.getUsername();
				String password = userAuthConfig.getClearTextPassword(salt);
				String host = system.getStorageConfig().getHost();
				int port = system.getStorageConfig().getPort();
				
				String rootDir = system.getStorageConfig().getRootDir();
				String homeDir = system.getStorageConfig().getHomeDir();
				
				switch (system.getStorageConfig().getProtocol()) 
				{
					case FTP: 
						if (StringUtils.isBlank(username)) {
							username = "anonymous";
						}
						if (StringUtils.isBlank(password)) {
							password = "anonymous";
						}
						return new FTP(host, port, username, password, rootDir, homeDir);
					case AZURE: 
						return new AzureJcloud(
								userAuthConfig.getClearTextPublicKey(salt),
								userAuthConfig.getClearTextPrivateKey(salt),
								rootDir, 
								homeDir, 
								system.getStorageConfig().getContainerName(),
								AzureJcloud.AZURE_STORAGE_PROVIDER);
					case S3: 
						return new S3Jcloud(
								userAuthConfig.getClearTextPublicKey(salt),
								userAuthConfig.getClearTextPrivateKey(salt),
								rootDir, homeDir, system.getStorageConfig().getContainerName(),
								host, port);
					case SWIFT: 
						return new AzureJcloud(
								userAuthConfig.getClearTextPublicKey(salt),
								userAuthConfig.getClearTextPrivateKey(salt),
								rootDir, homeDir, system.getStorageConfig().getContainerName(),
								AzureJcloud.OPENSTACK_STORAGE_PROVIDER);
					case SFTP: 
						if (system.getStorageConfig().getProxyServer() == null) 
						{
							if (userAuthConfig.getType().equals(AuthConfigType.SSHKEYS)) {
								return new SftpRelay(host, port, username, password, rootDir, homeDir,
										null, 0,
										userAuthConfig.getClearTextPublicKey(salt),
										userAuthConfig.getClearTextPrivateKey(salt),
										Settings.SFTP_RELAY_HOST, Settings.SFTP_RELAY_PORT);
							} else {
								return new SftpRelay(host, port, username, password, rootDir, homeDir,
										null, 0, null, null,
										Settings.SFTP_RELAY_HOST, Settings.SFTP_RELAY_PORT);
							}
						} 
						else 
						{
							if (userAuthConfig.getType().equals(AuthConfigType.SSHKEYS)) {
								return new SftpRelay(host, port, username, password, rootDir, homeDir,
										system.getStorageConfig().getProxyServer().getHost(),
										system.getStorageConfig().getProxyServer().getPort(),
										userAuthConfig.getClearTextPublicKey(salt),
										userAuthConfig.getClearTextPrivateKey(salt),
										Settings.SFTP_RELAY_HOST, Settings.SFTP_RELAY_PORT);
							} 
							else
							{
								return new SftpRelay(host, port, username, password, rootDir, homeDir,
										system.getStorageConfig().getProxyServer().getHost(),
										system.getStorageConfig().getProxyServer().getPort(),
										null, null,
										Settings.SFTP_RELAY_HOST, Settings.SFTP_RELAY_PORT);
							}
						}
//					case SFTP:
//						if (system.getStorageConfig().getProxyServer() == null)
//						{
//							if (userAuthConfig.getType().equals(AuthConfigType.SSHKEYS)) {
//								return new MaverickSFTP(host, port, username, password, rootDir, homeDir,
//										userAuthConfig.getClearTextPublicKey(salt),
//										userAuthConfig.getClearTextPrivateKey(salt));
//							} else {
//								return new MaverickSFTP(host, port, username, password, rootDir, homeDir);
//							}
//						}
//						else
//						{
//							if (userAuthConfig.getType().equals(AuthConfigType.SSHKEYS)) {
//								return new MaverickSFTP(host, port, username, password, rootDir, homeDir,
//										system.getStorageConfig().getProxyServer().getHost(),
//										system.getStorageConfig().getProxyServer().getPort(),
//										userAuthConfig.getClearTextPublicKey(salt),
//										userAuthConfig.getClearTextPrivateKey(salt));
//							}
//							else
//							{
//								return new MaverickSFTP(host, port, username, password, rootDir, homeDir,
//										system.getStorageConfig().getProxyServer().getHost(),
//										system.getStorageConfig().getProxyServer().getPort());
//							}
//						}
					case GRIDFTP: 
						GSSCredential credential = (GSSCredential)userAuthConfig.retrieveCredential(salt);
						
						if (credential != null && userAuthConfig.getCredentialServer() != null) {
							// saved the new credential for repeated use down the road
							userAuthConfig.setLastUpdated(new Date());
							new SystemDao().updateAuthConfig(userAuthConfig);
						}
						
						try {
							return new GridFTP(host, port, username, credential, rootDir, homeDir);
						} catch (UnknownHostException e) {
							throw new RemoteDataException("Connection refused: Unknown host " + host);
						} catch (Throwable e) {
							throw new RemoteDataException("Unable to connect to the GRIDFTP server at " + host + ":" + port, e);
						}
					case IRODS:
						String resource = system.getStorageConfig().getResource();
						String zone = system.getStorageConfig().getZone();
						IRODS irods = null;
						if (userAuthConfig.getType().equals(AuthConfigType.X509)) 
						{
							GSSCredential irodsCredential = (GSSCredential)userAuthConfig.retrieveCredential(salt);
							if (irodsCredential != null && userAuthConfig.getCredentialServer() != null) {
								// saved the new credential for repeated use down the road
								userAuthConfig.setLastUpdated(new Date());
								new SystemDao().updateAuthConfig(userAuthConfig);
							}
							irods = new IRODS(host, port, irodsCredential, resource, zone, rootDir, internalUsername, homeDir);
						}
						else if (userAuthConfig.getType().equals(AuthConfigType.PAM))
						{
							irods = new IRODS(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.PAM);
						}
						else if (userAuthConfig.getType().equals(AuthConfigType.KERBEROSE))
						{
							irods = new IRODS(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.STANDARD);
						}
						else
						{
							irods = new IRODS(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.STANDARD);
						}
						
						if (!system.getStorageConfig().isMirrorPermissions()) {
							irods.setPermissionMirroringRequired(false);
						}
						
						return irods;
					case IRODS4:
                        resource = system.getStorageConfig().getResource();
                        zone = system.getStorageConfig().getZone();
                        IRODS4 irods4 = null;
                        if (userAuthConfig.getType().equals(AuthConfigType.X509)) 
                        {
                            GSSCredential irodsCredential = (GSSCredential)userAuthConfig.retrieveCredential(salt);
                            if (irodsCredential != null && userAuthConfig.getCredentialServer() != null) {
                                // saved the new credential for repeated use down the road
                                userAuthConfig.setLastUpdated(new Date());
                                new SystemDao().updateAuthConfig(userAuthConfig);
                            }
                            irods4 = new IRODS4(host, port, irodsCredential, resource, zone, rootDir, internalUsername, homeDir);
                        }
                        else if (userAuthConfig.getType().equals(AuthConfigType.PAM))
                        {
                            irods4 = new IRODS4(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.PAM);
                        }
                        else if (userAuthConfig.getType().equals(AuthConfigType.KERBEROSE))
                        {
                            irods4 = new IRODS4(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.STANDARD);
                        }
                        else
                        {
                            irods4 = new IRODS4(host, port, username, password, resource, zone, rootDir, internalUsername, homeDir, AuthScheme.STANDARD);
                        }
                        
                        if (!system.getStorageConfig().isMirrorPermissions()) {
                            irods4.setPermissionMirroringRequired(false);
                        }
                        
                        return irods4;
					case LOCAL:
					default: 
						return new Local(system, rootDir, homeDir);
				}
			} 
			else 
			{
				throw new RemoteCredentialException("No credentials associated with " + system.getSystemId() + ". " +
						"Please set a default credential" + 
						(StringUtils.isNotEmpty(internalUsername) ? " or a credential for " + internalUsername : "") +
						" in order to access data on this system.");
			}
		}
		catch (EncryptionException e) {
			throw new RemoteDataException("Unable to decrypt credentials for remote authentication.", e);
		}
		
	}
	
	/**
	 * Creates a pre-configured {@link RemoteDataClient} from the given {@link URI} 
	 * that can be used to connect to remote systems. Both Agave URI, self-referencing 
	 * URL to existing Agave endpoints, and standard URL are supported.
	 * 
	 * If an Agave URL is given, {@code agave://my-demo-system//some/file/path}, the 
	 * hostname will be used to look up an existing registered system. The path will be 
	 * resolved to obtain the path on the system. This is logically the same as specifying
	 * {@code https://dev.tenants.agaveplatform.org/files/media/system/my-demo-system//some/file/path}.
	 * 
	 * In the event the URL self-references an Agave endpoint, it is resolved 
	 * into an Agave URI and parsed to get the {@link RemoteSystem} and path. As with
	 * standard Agave URI, no auth is needed. There are three kinds of self-referencing 
	 * URL. 
	 * <ol>
	 * <li>Canonical Files API URL: these are full file endpoint URL as given above. 
	 * (ex.{@code https://dev.tenants.agaveplatform.org/files/media/system/my-demo-system//some/file/path})</li>
	 * <li>Convenience Files API URL: these are shortened forms of the canonical file URL, but 
	 * exclude the system id and use the implied default storage system for the requesting user.</li>
	 * <li>Job output URL: these are convenience URL which reference the output directories of 
	 * existing jobs. These will be resolved at runtime to get the current output folder for the job.</li>
	 * </ol> 
	 * 
	 * Standard URL are also supported which provide both public and authenticated access to
	 * resources. See {@link #isSchemeSupported(URI)} for more information on supported schema. No authentication
	 * checks will be made for standard URL, thus
	 * 
	 * @see #isSchemeSupported(URI)
	 * @param apiUsername the user making the requet
	 * @param internalUsername the internal user of the {@code apiUsername} making the request
	 * @param uri the {@link URI} for which a {@link RemoteDataClient} will be generated
	 * @return a valid instance of a {@link RemoteDataClient} for the schema of the given {@code uri}.
	 * @throws SystemUnknownException if the sytem is unknown
	 * @throws AgaveNamespaceException if the URI does match any known agave uri pattern
	 * @throws RemoteCredentialException if the credentials for the system represented by the URI cannot be found/refreshed/obtained
	 * @throws PermissionException when the user does not have permission to access the {@code target}
	 * @throws FileNotFoundException when the remote {@code target} does not exist
	 * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
	 * @throws NotImplementedException when the schema is not supported
	 */
	public RemoteDataClient getInstance(String apiUsername, String internalUsername, URI uri) 
	throws RemoteDataException, RemoteCredentialException, PermissionException,
		   SystemUnknownException, AgaveNamespaceException, FileNotFoundException
	{
		// first look for internal URI
	    if (ApiUriUtil.isInternalURI(uri)) 
	    {
	        RemoteSystem system = ApiUriUtil.getRemoteSystem(apiUsername, uri);
	        RemoteDataClient client = system.getRemoteDataClient(internalUsername);
	        
	        // if this is a job output uri, we need to verify the user has access to the
	        // underlying system or else adjust the system root to point just to the 
	        // output folder.
	        if (AgaveUriRegex.JOBS_URI.matches(uri)) 
	        {
	            // user does not have system access, so we need to restrict system access to 
	            // the job output folder.
	            if (!system.getUserRole(apiUsername).canRead()) 
	            {
	                // we will look up the job ouput folder and update the rootDir and homeDir 
	                // to the work directory path.
	                String jobUuid = AgaveUriRegex.getMatcher(uri).group(1);
	                String jobWorkDirectory = new SystemDao().getRelativePathForJobOutput(jobUuid, apiUsername, TenancyHelper.getCurrentTenantId());
	                String newRootDirectory = client.resolvePath(jobWorkDirectory);
	                client.updateSystemRoots(newRootDirectory, "/");
	            }
	        }
	        
	        return client;
	    }
	    // next check that the schema is supported
	    else if (!RemoteDataClientFactory.isSchemeSupported(uri))
	    {
	        String msg = "Schema not supported: " + uri;
//	        log.error(msg);
	        throw new NotImplementedException(msg);
	    }
	    else 
	    {
    	    String host = uri.getHost();
    		int port = uri.getPort();
    		String username = null;
    		String password = null;
    		
    		if (!StringUtils.isEmpty(uri.getUserInfo())) {
    			String[] tokens = uri.getUserInfo().split(":");
    			if (tokens.length > 0) username = tokens[0];
    			if (tokens.length > 1) password = tokens[1];
    		}
    		
    		String scheme = uri.getScheme();
    		if (StringUtils.equalsIgnoreCase(scheme,"http")) {
                return new HTTP(host, port, username, password, false);
            } 
            else if (StringUtils.equalsIgnoreCase(scheme,"https")) {
                return new HTTP(host, port, username, password, true);
            } 
            else if (StringUtils.equalsIgnoreCase(scheme,"ftp")) {
    			if (StringUtils.isBlank(username)) {
    				username = "anonymous";
    			}
    			if (StringUtils.isBlank(password)) {
    				password = "anonymous";
    			}
    			return new FTP(host, port, username, password, null, null);
    		} 
    		else if (StringUtils.equalsIgnoreCase(scheme,"sftp")) {
    			if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
					throw new RemoteCredentialException("Invalid credentials provided for URI, " + uri +
							". SFTP URL  must have valid username and password provided in the user info section of " +
							"the URI in order to authenticate. Please consider registering your SFTP server as a system " +
							"with Agave to avoid unintentionally exposing your credentials.");
				} else if (StringUtils.isEmpty(host)) {
					throw new RemoteDataException("Invalid hostname provided for URI, " + uri);
				}

//    			return new MaverickSFTP(host, port, username, password, null, null);
				return new SftpRelay(host, port, username, password, null, null,
						Settings.SFTP_RELAY_HOST, Settings.SFTP_RELAY_PORT);
    		}
    		else {
    		    // should not ever happen;
                String msg = "Invalid URI: " + uri;
//                log.error(msg);
    		    throw new NotImplementedException(msg);
    		}
	    }
	    
	}
	
	/**
	 * Tells whether the URI is supported based on the scheme. {@code agave} schema are resolved to
	 * {@link RemoteSystem}. URI with {@code http} and {@code https} schema are checked against known resource URI
	 * within the current tenant and accepted if a match is found. For all other schemas, the response is based upon
	 * agave's ability to authenticate to the remote system using basic username and password found in the standard
	 * {@link URI#getAuthority()} content. Empty and null schema are rejected.
	 * 
	 * @param uri the {@link URI} for which to check the schema
	 * @return true if the schema is supported directly or through reference to an internal api URI
	 */
	public static boolean isSchemeSupported(URI uri) {
		String scheme = uri.getScheme();
		if (StringUtils.isEmpty(scheme)) return false;

		switch (scheme.toLowerCase()) {
			case "agave":
			case "http":
			case "https":
			case "ftp":
			case "sftp":
				return true;
			case "gsiftp":
			case "gridftp":
			case "azure":
			case "s3":
			case "irods":
				return false;
			default:
				return ApiUriUtil.isInternalURI(uri);
		}
	}

	/**
	 * Tells whether the URI is supported based on the scheme. {@code agave} schema are resolved to
	 * {@link RemoteSystem}. URI with {@code http} and {@code https} schema are checked against known resource URI
	 * within the current tenant and accepted if a match is found. For all other schemas, the response is based upon
	 * agave's ability to authenticate to the remote system using basic username and password found in the standard
	 * {@link URI#getAuthority()} content.
	 *
	 * @param uri the {@link URI} for which to check the schema
	 * @return true if the schema is supported directly or through reference to an internal api URI
	 */
	public static boolean isSchemeSupported(URI uri, String tenantId)
	{
		String scheme = uri.getScheme();
		switch (scheme.toLowerCase()) {
			case "agave":
			case "http":
			case "https":
			case "ftp":
//			case "ftps":
			case "sftp":
				return true;
			case "gsiftp":
			case "gridftp":
			case "azure":
			case "s3":
			case "irods":
//			case "irods4":
//			case "irods3":
				return false;
			default:
				try { return AgaveUriRegex.matchesAny(uri, tenantId); }
				catch (Throwable t) { return false; }
		}
	}
}

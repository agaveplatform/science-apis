/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author dooley
 *
 */
@Test(singleThreaded=true, groups= {"sftp", "sftp-sshkeys", "filesystem", "integration"})
public class SftpRelaySshKeysRemoteDataClientTest extends SftpPasswordRemoteDataClientTest {

	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#getSystemJson()
	 */
	@Override
	@Test(enabled=false)
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-sshkeys.example.com.json");
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
				client.updateSystemRoots(client.getRootDir(), system.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
				threadClient.set(client);
			}
		} catch (EncryptionException e) {
			Assert.fail("Failed to get client", e);
		}

		return threadClient.get();
	}
}

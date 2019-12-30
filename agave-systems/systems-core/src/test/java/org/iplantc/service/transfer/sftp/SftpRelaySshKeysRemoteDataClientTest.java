/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author dooley
 *
 */
@Test(groups={"sftp-sshkeys.operations"})
public class SftpRelaySshKeysRemoteDataClientTest extends SftpRelayPasswordRemoteDataClientIT {

	@Override
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

}

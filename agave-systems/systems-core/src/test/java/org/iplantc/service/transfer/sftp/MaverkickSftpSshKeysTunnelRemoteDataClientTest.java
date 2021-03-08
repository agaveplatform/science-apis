/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.model.AuthConfig;
import org.iplantc.service.systems.model.ProxyServer;
import org.iplantc.service.transfer.RemoteDataClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * @author dooley
 *
 */
@Test(enabled=false, groups={"sftp","sftp-sshkeys-tunnel.operation","broken"})
public class MaverkickSftpSshKeysTunnelRemoteDataClientTest extends MaverkickSftpSshKeysRemoteDataClientIT {

	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-sshkeys-tunnel.example.com.json");
	}

	@Override
	protected RemoteDataClient getClient()
	{
		RemoteDataClient client;
		try {
			if (threadClient.get() == null) {
				AuthConfig userAuthConfig = system.getStorageConfig().getDefaultAuthConfig();
				String salt = system.getSystemId() + system.getStorageConfig().getHost() + userAuthConfig.getUsername();
				String username = userAuthConfig.getUsername();
				String password = userAuthConfig.getClearTextPassword(salt);
				String privateKey = userAuthConfig.getClearTextPrivateKey(salt);;
				String publicKey = userAuthConfig.getClearTextPublicKey(salt);
				String host = system.getStorageConfig().getHost();
				int port = system.getStorageConfig().getPort();
				String rootDir = system.getStorageConfig().getRootDir();
				String threadHomeDir = String.format("%s/thread-%s-%d",
						system.getStorageConfig().getHomeDir(),
						UUID.randomUUID().toString(),
						Thread.currentThread().getId());
				ProxyServer proxy = system.getStorageConfig().getProxyServer();

				client = new MaverickSFTP(host, port, username, password,
						rootDir, threadHomeDir,
						proxy.getHost(), proxy.getPort(),
						publicKey, privateKey);
				threadClient.set(client);
			}
		} catch (EncryptionException e) {
			Assert.fail("Failed to get client", e);
		}

		return threadClient.get();
	}
}

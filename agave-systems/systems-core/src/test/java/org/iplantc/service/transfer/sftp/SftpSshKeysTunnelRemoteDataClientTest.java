/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

/**
 * @author dooley
 *
 */
@Test(groups= {"sftp-sshkeys-tunnel.operation","disabled"})
public class SftpSshKeysTunnelRemoteDataClientTest extends SftpSshKeysRemoteDataClientIT {

	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-sshkeys-tunnel.example.com.json");
	}
}

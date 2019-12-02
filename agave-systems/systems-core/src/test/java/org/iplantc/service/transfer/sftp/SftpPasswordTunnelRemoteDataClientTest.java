/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import java.io.IOException;

import org.iplantc.service.transfer.AbstractRemoteDataClientTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

/**
 * @author dooley
 *
 */
@Test(groups= {"sftp-password-tunnel.operations","disabled"})
public class SftpPasswordTunnelRemoteDataClientTest extends SftpPasswordRemoteDataClientIT {

	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-password-tunnel.example.com.json");
	}
}

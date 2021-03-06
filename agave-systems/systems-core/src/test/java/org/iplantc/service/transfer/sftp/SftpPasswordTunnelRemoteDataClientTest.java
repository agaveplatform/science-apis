/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author dooley
 *
 */
@Test(groups= {"sftp","sftp-password-tunnel.operations","broken"})
public class SftpPasswordTunnelRemoteDataClientTest extends SftpPasswordRemoteDataClientIT {

	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-password-tunnel.example.com.json");
	}
}

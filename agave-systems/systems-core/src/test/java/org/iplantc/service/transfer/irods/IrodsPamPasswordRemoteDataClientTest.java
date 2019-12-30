/**
 * 
 */
package org.iplantc.service.transfer.irods;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

/**
 * @author dooley
 *
 */
@Test(groups={"irods3","irods3-pam.operations"})
public class IrodsPamPasswordRemoteDataClientTest extends IrodsPasswordRemoteDataClientIT {

	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#getSystemJson()
	 */
	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods3-pam.example.com.json");
	}
}

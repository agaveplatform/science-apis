/**
 * 
 */
package org.iplantc.service.transfer.irods;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author dooley
 *
 */
@Test(groups={"integration", "irods3","irods3-pam.operations"})
public class IrodsPamPasswordRemoteDataClientIT extends IrodsPasswordRemoteDataClientIT {

	/* (non-Javadoc)
	 * @see org.iplantc.service.transfer.AbstractRemoteDataClientTest#getSystemJson()
	 */
	@Override
	protected JSONObject getSystemJson() throws JSONException, IOException {
		return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods3-pam.example.com.json");
	}
}

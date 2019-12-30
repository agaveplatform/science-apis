/**
 * 
 */
package org.iplantc.service.transfer.irods4;

import java.io.IOException;

import org.iplantc.service.transfer.irods.IrodsRemoteDataClientPermissionProviderTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

/**
 * Test for all IRODS4 client permission implementations. This inherits nearly
 * all it's functionality from the parent class.
 * 
 * @author dooley
 *
 */
@Test(groups= {"irods4","irods4.permissions"})
public class Irods4RemoteDataClientPermissionProviderIT extends IrodsRemoteDataClientPermissionProviderTest
{
    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.RemoteDataClientPermissionProviderTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods4-password.example.com.json");
    }
}
/**
 * 
 */
package org.iplantc.service.transfer.sftp;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.BaseTransferTestCase;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientTestUtils;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.sftp.MaverickSFTP;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author dooley
 *
 */
@Test(groups={"sftp-sshkeys-kbi.auth","broken"})
public class SftpSshKeysKBIAuthTest extends RemoteDataClientTestUtils {

private static final Logger log = Logger.getLogger(SftpSshKeysKBIAuthTest.class);
    
    protected ThreadLocal<RemoteDataClient> threadClient = new ThreadLocal<RemoteDataClient>();
    
    /**
     * Returns a {@link JSONObject} representing the system to test.
     * 
     * @return 
     * @throws JSONException
     * @throws IOException
     */
    protected JSONObject getSystemJson() throws JSONException, IOException {
    	return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "sftp-sshkeys-kbi.example.com.json");
    }

    @Override
    protected String getForbiddenDirectoryPath(boolean shouldExist) {
        if (shouldExist) {
            return "/root";
        } else {
            return "/root/helloworld";
        }
    }


    @Test
    public void stat() 
	throws FileNotFoundException 
    {	
    	try {
    		MaverickSFTP client = (MaverickSFTP)getClient();
    		client.authenticate();
	    	client.getFileInfo("/");
	    	
	    	Assert.assertTrue(true, "Authentication should succeed wiht KBI auth");
    	}
    	catch (RemoteDataException e) {
    		Assert.fail("Authentication should succeed with KBI auth", e);
    	}
    	catch (IOException e) {
			Assert.fail("IO Exception should not be thrown an KBI auth", e);
		} 
    	finally {
    		
    	}
    }
}

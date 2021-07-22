package org.iplantc.service.io.clients;

import com.fasterxml.jackson.databind.JsonNode;
import org.iplantc.service.common.Settings;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.exceptions.TaskException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URISyntaxException;

import static org.testng.Assert.*;

@Test(singleThreaded = true, groups={"integration.clients.transferService"}, dependsOnGroups = "integration.dao.logicalFileDao")
public class TransferServiceIT extends BaseTestCase {
    String source;
    String dest;
    String transferUuid;

    @BeforeClass
    public void setUp() throws URISyntaxException {
        clearQueues();
        clearLogicalFiles();

        source = "https://httpd:8443/public/test_upload.bin";
        // Transfers api is an internal service. The destination must be a valid location.
        dest = "file:/dev/null";
    }

    @AfterClass
    public void cleanUp() throws Exception {
        clearQueues();
        clearLogicalFiles();
    }

    @Test
    public void sendPost(){
        try {
            TransferService service = new TransferService(SYSTEM_OWNER, TENANT_ID);
            APIResponse postResponse = service.post(source, dest);
                assertNotNull(postResponse, "Response from post request should return queued transfer task.");
            transferUuid = postResponse.getResult().get("uuid").asText();
            assertNotNull(transferUuid, "Response from successful request to transfer service should return valid uuid.");

            JsonNode jsonResponse = postResponse.getResult();

            assertEquals(jsonResponse.get("source").asText(), source, "Source from transfer api should match source from request");
            assertEquals(jsonResponse.get("dest").asText(), dest, "Dest from transfer api should match dest from request");
            assertEquals(jsonResponse.get("tenant_id").asText(), TENANT_ID, "Tenant id from transfer api should match tenant id from request");
        } catch (TaskException e){
            fail("TransferService should connect and send requests to " + Settings.IPLANT_TRANSFER_SERVICE + " without throwing an exception: " + e.getMessage());
        }
    }

    @Test (dependsOnMethods = {"sendPost"})
    public void sendGet(){
        try {
        TransferService service = new TransferService(SYSTEM_OWNER, TENANT_ID);
        APIResponse getResponse = service.get(transferUuid);
        assertNotNull(getResponse, "Response from successful request from transfer service should return transfer task corresponding to uuid");

        JsonNode jsonResponse = getResponse.getResult();

        assertEquals(jsonResponse.get("source").asText(), source, "Source from transfer api should match source from request");
        assertEquals(jsonResponse.get("tenant_id").asText(), TENANT_ID, "Tenant id from transfer api should match tenant id from request");
        } catch (TaskException e){
            fail("TransferService should connect and send requests to " + Settings.IPLANT_TRANSFER_SERVICE + " without throwing an exception.");
        }
    }
}

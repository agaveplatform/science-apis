package org.iplantc.service.io.clients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.io.BaseTestCase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TransferServiceIT extends BaseTestCase {
    String source;
    String dest;
    String transferUuid;
    ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public void beforeClass() throws URISyntaxException {
        source = "https://httpd:8443/public/test_upload.bin";
        dest = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
    }

    @Test
    public void sendPost(){
        TransferService service = new TransferService(SYSTEM_OWNER, TENANT_ID);
        APIResponse postResponse = service.post(source, dest);
        assertNotNull(postResponse);
        transferUuid = postResponse.getResult().get("/uuid").asText();
        assertNotNull(transferUuid, "Response from successful request to transfer service should return valid uuid.");

        JsonNode jsonResponse = postResponse.getResult();

        assertEquals(jsonResponse.get("source").asText(), source, "Source from transfer api should match source from request");
        assertEquals(jsonResponse.get("tenantId").asText(), TENANT_ID, "Tenant id from transfer api should match tenant id from request");
    }

    @Test (dependsOnMethods = {"sendPost"})
    public void sendGet(){
        TransferService service = new TransferService(SYSTEM_OWNER, TENANT_ID);
        APIResponse getResponse = service.get(transferUuid);
        assertNotNull(getResponse, "Response from successful request from transfer service should return transfer task corresponding to uuid");

        JsonNode jsonResponse = getResponse.getResult();

        assertEquals(jsonResponse.get("source").asText(), source, "Source from transfer api should match source from request");
        assertEquals(jsonResponse.get("tenantId").asText(), TENANT_ID, "Tenant id from transfer api should match tenant id from request");
    }
}

package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.*;

public class MetadataRequestPermissionProcessorTest {
    private final String TEST_USER = "TEST_USER";
    ObjectMapper mapper = new ObjectMapper();

    public ArrayNode createSimpleArrayNode(String username, String permission) {
        ArrayNode node = mapper.createArrayNode();

                node.addObject()
                    .put("username", username)
                    .put("permission", permission);
        return node;
    }

    @DataProvider(name = "initSimplePermissionProvider")
    public Object[][] initSimplePermissionProvider() {
        return new Object[][]{
                {TEST_USER, PermissionType.READ_WRITE.toString(), "Valid Permission should be created from ArrayNode.", false},
                {"TEST_SHARE_USER", PermissionType.READ.toString(), "Valid Permission should be created from ArrayNode.", false},
                {"NONE_USER", PermissionType.NONE.toString(), "No Permission should be created from if PermissionType is NONE.", false},
                {"INVALID_USER", "INVALID_PERMISSION", "Unknown/invalid Permission should not create permission and throw MetadataException.", true},
                {"INVALID_USER", null, "Missing permission should not create permission and throw MetadataException.", true},
                {null, PermissionType.READ.toString(), "Missing username should not create permission and throw MetadataException.", true},
        };
    }

    @Test(dataProvider = "initSimplePermissionProvider")
    public void processSimpleArrayTest(String username, String permission, String message, boolean shouldThrowException) throws MetadataException {
        ArrayNode node = createSimpleArrayNode(username, permission);
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setOwner(TEST_USER);

        MetadataRequestPermissionProcessor permissionProcessor = new MetadataRequestPermissionProcessor(metadataItem);

        try {
            permissionProcessor.process(node);
            if (shouldThrowException) {
                fail(message);
            }

            List<MetadataPermission> addedPermissions = permissionProcessor.getMetadataItem().getPermissions();
            assertEquals(permissionProcessor.getMetadataItem().getTenantId(), metadataItem.getTenantId(), "MetadataItem tenantid should match the tenantId of the original metadata item.");
            assertEquals(permissionProcessor.getMetadataItem().getUuid(), metadataItem.getUuid(),"MetadataItem uuid should match the tenantId of the original metadata item.");
            assertEquals(permissionProcessor.getMetadataItem().getOwner(), metadataItem.getOwner(),"MetadataItem owner should match the tenantId of the original metadata item.");

            if (permission.equals(PermissionType.NONE.toString())) {
                assertEquals(addedPermissions.size(),0, message);
            } else {
                assertEquals(addedPermissions.size(), node.size(), "Permissions should be the same size as the original json object");
                for (MetadataPermission pem : addedPermissions) {
                    boolean found = false;
                    for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                        JsonNode pemNode = it.next();
                        if (pem.getUsername().equals(pemNode.get("username").asText())) {
                            assertEquals(pem.getPermission().toString(), pemNode.get("permission").asText());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        fail("All permissions from the original json should be present in the parsed metadata item");
                    }
                }
            }
        } catch (Exception e) {
            if (!shouldThrowException) {
                fail(message);
            }
        }
    }


}

package org.iplantc.service.tags.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.tags.TestDataHelper;
import org.iplantc.service.tags.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public class TagPermissionIT {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void toJSON()
    {
        try
        {
            Tag tag = new Tag("foo", TestDataHelper.TEST_USER, new String[]{ new AgaveUUID(UUIDType.TENANT).toString() });
            TagPermission permission = new TagPermission(tag, TestDataHelper.TEST_SHAREUSER, PermissionType.READ);

            ObjectNode json = (ObjectNode) mapper.readTree(permission.toJSON());

            Assert.assertTrue(json.get("_links").get("self").has("href"), "No hypermedia found in serialized response");
            Assert.assertTrue(json.get("_links").has("tag"), "No permissions reference found in serialized response");
            Assert.assertTrue(json.get("_links").has("profile"), "No owner reference found in serialized response");
        }
        catch (Exception e) {
            Assert.fail("Permission serialization should never throw exception", e);
        }
    }
}

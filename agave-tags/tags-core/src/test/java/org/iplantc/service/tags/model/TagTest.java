package org.iplantc.service.tags.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.tags.TestDataHelper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.iplantc.service.tags.TestDataHelper.TEST_USER;

@Test(groups={"unit"})
public class TagTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private TestDataHelper dataHelper;

    @BeforeClass
    public void beforeClass() {
        dataHelper = TestDataHelper.getInstance();
    }

    @Test
    public void constructTag() {
        Tag tag = new Tag();
        Assert.assertNotNull(tag.getUuid(), "UUID not set on instantiation.");
        Assert.assertNotNull(tag.getTenantId(), "Tenant id not set on instantiation.");
        Assert.assertNotNull(tag.getCreated(), "Creation date not set on instantiation.");
        Assert.assertNotNull(tag.getLastUpdated(), "Last updated date not set on instantiation.");
        Assert.assertNotNull(tag.getTaggedResources(), "taggedResources not set on instantiation.");
    }

    @DataProvider
    private Object[][] setNameProvider() throws IOException {
        ObjectNode json = ((ObjectNode) dataHelper.getTestDataObject(TestDataHelper.TEST_TAG));
        json.put("owner", TEST_USER);

        return new Object[][]{
                {json, "foo", false, "minimum alpha characters should not throw exception"},
                {json, "", true, "empty name should throw exception"},
                {json, null, true, "null name should throw exception"},
                {json, "f", true, "less than 3 alpha characters should throw exception"},
                {json, "ff", true, "less than 3 alpha characters should throw exception"},
        };
    }

    @Test(dataProvider = "setNameProvider")
    public void setName(ObjectNode json, String name, boolean shouldThrowException, String message) {
        try {
            if (name == null) {
                json.putNull("name");
            } else {
                json.put("name", name);
            }
            Tag tag = Tag.fromJSON(json);

            if (shouldThrowException) {
                Assert.fail(message);
            }
        } catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
    }

    @DataProvider
    private Object[][] setTaggedResourcesFailsInvalidAssociatedIdsProvider() throws IOException {
        ObjectNode json = ((ObjectNode) dataHelper.getTestDataObject(TestDataHelper.TEST_TAG));
        json.put("owner", TEST_USER);

        return new Object[][]{
                {json, mapper.createArrayNode(), true, "empty associatedIds array should throw exception"},
                {json, mapper.createArrayNode().addNull(), true, "null associatedIds array should throw exception"},
                {json, mapper.createArrayNode().add(""), true, "empty uuid in associatedIds array should throw exception"},
                {json, mapper.createArrayNode().add("foo"), true, "invalid uuid in associatedIds array should throw exception"},

        };
    }


    @Test(dataProvider = "setTaggedResourcesFailsInvalidAssociatedIdsProvider")
    public void setTaggedResourcesFailsInvalidAssociatedIds(ObjectNode json, ArrayNode uuids, boolean shouldThrowException, String message) {
        try {
            if (uuids == null) {
                json.putNull("associatedUuids");
            } else {
                json.set("associatedUuids", uuids);
            }

            Tag.fromJSON(json);

            if (shouldThrowException) {
                Assert.fail(message);
            }
        } catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
    }
}

package org.iplantc.service.tags.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.tags.AbstractTagTest;
import org.iplantc.service.tags.TestDataHelper;
import org.iplantc.service.tags.exceptions.TagException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups = {"integration"})
public class TagIT extends AbstractTagTest {

    @BeforeClass
    protected void beforeClass() throws Exception {
        super.beforeClass();
    }

    @AfterClass
    protected void afterClass() throws TagException {
        super.afterClass();
    }


    @DataProvider
    private Object[][] setTaggedResourcesProvider() {
        ObjectMapper mapper = new ObjectMapper();

        return new Object[][]{
                {mapper.createArrayNode().add(defaultTenant.getUuid()), new String[]{defaultTenant.getUuid()}, "valid associatedIds array should succeed"},
                {mapper.createArrayNode().add(defaultTenant.getUuid()).add(defaultTenant.getUuid()), new String[]{defaultTenant.getUuid()}, "valid associatedIds should be stripped to unique"},
        };
    }


    @Test(dataProvider = "setTaggedResourcesProvider")
    public void setTaggedResources(ArrayNode provided, String[] expected, String message) {
        try {
            ObjectNode json = ((ObjectNode) dataHelper.getTestDataObject(TestDataHelper.TEST_TAG));
            json.set("associationIds", provided);

            Tag tag = Tag.fromJSON(json);

            Assert.assertEquals(tag.getTaggedResourcesAsArray().length, expected.length, message);
            Assert.assertTrue(Arrays.asList(tag.getTaggedResourcesAsArray()).containsAll(Arrays.asList(expected)), message);
        } catch (Exception e) {
            Assert.fail(message, e);
        }
    }

    @DataProvider
    private Object[][] toJSONProvider() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object[]> testCases = new ArrayList<Object[]>();
        for (UUIDType resourceType : UUIDType.values()) {
            Tag tag = new Tag(resourceType.name() + "_TAG", TestDataHelper.TEST_USER, new String[]{new AgaveUUID(resourceType).toString()});
            testCases.add(new Object[]{tag, resourceType, resourceType.name() + " should deserialize in hypermedia response"});
        }


        return testCases.toArray(new Object[][]{});
    }


    @Test(dataProvider = "toJSONProvider")
    public void toJSON(Tag tag, UUIDType resourceType, String message) {
        try {
            ObjectNode json = new ObjectMapper().valueToTree(tag);

            Assert.assertTrue(json.get("_links").get("self").has("href"), "No hypermedia found in serialized response");
            Assert.assertTrue(json.get("_links").has("permissions"), "No permissions reference found in serialized response");
            Assert.assertTrue(json.get("_links").has("associations"), "No resources reference found in serialized response");
            Assert.assertTrue(json.get("_links").has("history"), "No history reference found in serialized response");
            Assert.assertTrue(json.get("_links").has("owner"), "No owner reference found in serialized response");
        } catch (Exception e) {
            Assert.fail(message, e);
        }
    }
}

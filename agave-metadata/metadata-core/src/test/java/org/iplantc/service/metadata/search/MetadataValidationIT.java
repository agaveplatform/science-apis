package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

@Test(groups={"integration"})
public class MetadataValidationIT {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static String TEST_USER = "TEST_USER";

    private ObjectNode setupNode() {
        ObjectNode baseNode = mapper.createObjectNode();

        baseNode.put("uuid", new AgaveUUID(UUIDType.METADATA).toString())
                .put("name", "sample name")
                .putObject("value")
                .put("order", "sample order")
                .put("description", "sample description")
                .put("status", "active");
        return baseNode;
    }

    /**
     * Creates a metadata schema item against which to validate the metadata item under test
     * @return id of metadata item to reference in the tests
     */
    private String setupSchema() {
        String schemaJson = "{" +
                "\"title\" : \"order profile\"," +
                "\"type\" : \"object\", " +
                "\"properties\" : {" +
                    "\"profile\" : {" +
                        "\"type\" : \"string\" " +
                    "}, " +
                    "\"description\" : { " +
                        "\"type\" : \"string\" " +
                    "}, " +
                    "\"status\" : {" +
                        "\"type\" : \"string\", " +
                        "\"enum\" : [\"active\", \"retired\", \"disabled\"]" +
                    "}" +
                "}" +
            "}";

        MetadataSchemaItem metadataSchemaItem = null;
        try {
            metadataSchemaItem = new MetadataSchemaItem();
            metadataSchemaItem.setSchema(mapper.readTree(schemaJson));
            metadataSchemaItem.setOwner(TEST_USER);
            MetadataSchemaDao.getInstance().insert(metadataSchemaItem);


        } catch (Exception e) {
            fail("Unable to create test schema for validation test", e);
        }

        return metadataSchemaItem.getUuid();
    }


    @DataProvider(name = "initMetadataNodeDataProvider")
    public Object[][] initMetadataNodeDataProvider() {
        String schemaId = setupSchema();

        return new Object[][]{
                {setupNode(), false, "Valid fields and value complying to the schema should not throw exception."},
                {setupNode().put("name", ""), true, "Empty name field should throw exception."},
                {setupNode().putNull("name"), true, "Null name field should throw exception"},
                {setupNode().put("name", "sample name"), false, "Valid name field should not throw exception"},
                {setupNode().putNull("value"), true, "Null value field should throw exception."},
                {setupNode().put("schemaId", schemaId).putObject("value").put("status", "invalid status"), true, "Value that doesn't conform to schema should throw exception."},
                {setupNode().put("schemaId", schemaId).putObject("value").put("status", "disabled"), true, "Value that does conform to schema should not throw exception."},
                {setupNode().putNull("schemaId"), false, "Null schemaId is valid and should not throw exception."},
                {setupNode().put("schemaId", new AgaveUUID(UUIDType.JOB).toString()), true, "Invalid Agave Uuid type for schemaId should throw exception."},
        };

    }


    @Test(dataProvider = "initMetadataNodeDataProvider")
    public void validateMetadataDocumentFieldsTest(JsonNode node, boolean bolThrowException, String message) {
        try {
            MetadataValidation validation = new MetadataValidation();
            validation.validateMetadataNodeFields(node, TEST_USER);
            if (bolThrowException)
                fail(message);
        } catch (Exception e) {
            if (!bolThrowException)
                fail(message);
        }
    }
}

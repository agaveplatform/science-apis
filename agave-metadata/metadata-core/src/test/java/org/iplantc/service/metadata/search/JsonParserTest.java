package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups = {"integration"})
public class JsonParserTest {

    @Test
    public void parseJsonToMetadataTest() throws MetadataQueryException, IOException {
        System.out.println("WENT THROUGH JSON PARSER TEST! ");
        String strJson = "{" +
                "\"name\": " + this.getClass().getName() + "," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[" + PermissionType.READ_WRITE + "]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertNotNull(jsonParser.getMetadataItem());
    }

    @Test
    public void parseNameToStringTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertEquals(jsonParser.parseNameToString(node), this.getClass().getName());

    }

    @Test
    public void parseMissingNameToStringTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertThrows(MetadataQueryException.class, () -> jsonParser.parseNameToString(node));
    }

    @Test
    public void parseValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        jsonParser.parseValueToJsonNode(node);
        Assert.assertEquals(jsonParser.getMetadataItem().getValue().get("testKey"), "testValue");

    }

    @Test
    public void parseMissingValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertThrows(MetadataQueryException.class, () -> jsonParser.parseValueToJsonNode(node));
    }

    @Test
    public void parseAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException, UUIDException, MetadataException {
        String associationId = new AgaveUUID(UUIDType.JOB).toString();
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + associationId + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        jsonParser.parseAssociationIdsToArrayNode(node);
        Assert.assertTrue(jsonParser.getMetadataItem().getAssociations().getAssociatedIds().containsValue(associationId));
    }

    @Test
    public void parseInvalidAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException {
        String associationId = "00000-0000000-00000000";

        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + associationId + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertThrows(UUIDException.class, ()->jsonParser.parseAssociationIdsToArrayNode(node));
    }

    @Test
    public void parseNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertNotNull(jsonParser.parseNotificationToArrayNode(node));
    }

    @Test
    public void parseInvalidNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "\"" + "notifications" + "\"" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertThrows(MetadataQueryException.class, () -> jsonParser.parseNotificationToArrayNode(node));
    }

    @Test
    public void parsePermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertNotNull(jsonParser.parsePermissionToArrayNode(node));
    }

    @Test
    public void parseInvalidPermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "\"" + PermissionType.READ_WRITE + "\"" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertThrows(MetadataQueryException.class, ()->jsonParser.parsePermissionToArrayNode(node));
    }

    @Test
    public void parseSchemaIdToStringTest() throws IOException, MetadataQueryException, PermissionException, MetadataStoreException {
        String schemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + schemaId + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        jsonParser.parseSchemaIdToString(node);
        Assert.assertEquals(jsonParser.getMetadataItem().getSchemaId(), schemaId);
    }

    @Test
    public void parseInvalidSchemaIdToStringTest() throws IOException, MetadataQueryException, PermissionException, MetadataStoreException {
        String schemaId = new AgaveUUID(UUIDType.JOB).toString();

        String strJson = "{" +
                "\"name\": \"" + this.getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + schemaId + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonParser jsonParser = new JsonParser(node);
        Assert.assertTrue(jsonParser.parseSchemaIdToString(node).length() == 0);
    }

    @Test
    public void validateValueAgainstSchemaTest() throws IOException, MetadataQueryException {
        //TODO
//        String strJson = "{" +
//                "\"name\": \"" + this.getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonParser jsonParser = new JsonParser(node);
//        jsonParser.parseNameToString(node);
//        Assert.assertEquals(jsonParser.getMetadataItem().getName(), this.getClass().getName());
    }

    @Test
    public void validateInvalidValueAgainstSchemaTest() throws IOException, MetadataQueryException {
        //TODO
//        String strJson = "{" +
//                "\"name\": \"" + this.getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonParser jsonParser = new JsonParser(node);
//        jsonParser.parseNameToString(node);
//        Assert.assertEquals(jsonParser.getMetadataItem().getName(), this.getClass().getName());
    }

//
//    @DataProvider(name = "listInputProvider")
//    public Object[][] listInputProvider() throws Exception {
//        return new Object[][]{
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: testValue}",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(null,
//                        "{testKey: testValue}",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: ",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: testValue}",
//                        "[" + "000000-000000-00000000" + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: testValue}",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.FILE).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: testValue}",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.UNKNOWN.toString() + "]",
//                        "[" + "notification" + "]")},
//                new Object[]{createJson(this.getClass().getName(),
//                        "{testKey: testValue}",
//                        "[" + new AgaveUUID(UUIDType.JOB).toString() + "]",
//                        new AgaveUUID(UUIDType.SCHEMA).toString(),
//                        "[" + PermissionType.READ_WRITE.toString() + "]",
//                        "[" + "notification" + "]")}
//        };
//    }
//
//
//    public JsonNode createJson(String name, String value, String associationid, String schemaId, String permissions, String notifications) throws IOException {
//
//        String strJson = "{" +
//                "\"name\": " + name + "," +
//                "\"value\": " + value + "," +
//                "\"associationIds\": " + associationid + "," +
//                "\"schemaId\": " + schemaId + "," +
//                "\"permissions\": " + permissions + "," +
//                "\"notifications\": " + notifications + "," +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//        return node;
//
//    }


}

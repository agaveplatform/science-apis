package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.exceptions.MetadataValidationException;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.*;

@Test(groups = {"unit"})
public class JsonHandlerTest {

    ObjectMapper mapper = new ObjectMapper();

    public static final String TEST_USER = "testuser";
    public static final String TEST_SHARE_USER = "testshare";
    public static final String TEST_OTHER_USER = "testotheruser";

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Generates a JsonNode representing a test metadata item
     *
     * @return json representation of a metadata item request value
     */
    protected JsonNode createTestMetadataItem() {
        ObjectNode node = mapper.createObjectNode()
            .put("uuid", new AgaveUUID(UUIDType.METADATA).toString())
                .putNull("schemaId")
//                .put("schemaId", new AgaveUUID(UUIDType.SCHEMA).toString()) // optional, can be null
                .put("name", UUID.randomUUID().toString());
                node.putObject("value")
                        .put("testKey", "testValue");
                node.putArray("associationIds"); // optional and can be empty
//                        .add(new AgaveUUID(UUIDType.SCHEMA).toString());
                node.putArray("permissions")// optional array in request
                        .addObject()
                            .put("username", TEST_SHARE_USER)
                            .put("permission", PermissionType.READ.toString());
                node.putArray("notifications").addObject() // optional array in request
                        .put("url", "foo@example.com")
                        .put("event", "CREATED")
                        .put("persistent", false);
        return node;
    }

    @Test(enabled = false)
    public void parseJsonToMetadataTest() throws MetadataValidationException {
        JsonNode node = createTestMetadataItem();

        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.parseJsonMetadata(node);
        MetadataItem metadataItem = jsonHandler.getMetadataItem();
        assertNotNull(metadataItem,"Metadata item should not be null after handler parses it");
        assertEquals(metadataItem.getUuid(), node.get("uuid").asText(), "Metadata item uuid should be the uuid in the original json object");
        assertNotNull(metadataItem.getValue(), "Metadata item value should not be null");
        assertEquals(metadataItem.getValue().asText(), node.get("value").asText(), "Metadata item value should be the value in the original json object");
        assertEquals(metadataItem.getSchemaId(), node.get("schemaId").textValue(), "Metadata item schemaId should be the schemaId in the original json object");
        assertEquals(metadataItem.getName(), node.get("name").asText(), "Metadata item name should be the name in the original json object");

        assertEquals(metadataItem.getAssociations().size(), node.get("associationIds").size() , "Metadata associated uuid should be the same size as in the original json object");
        if (metadataItem.getAssociations().size() > 0) {
            assertEquals(metadataItem.getAssociations().getRawUuid().iterator().next(), node.get("associationIds").get(0).asText() , "Metadata associated uuid should be the uuid in the original json object");
        }

        assertEquals(jsonHandler.getPermissions().size(), node.get("permissions").size(), "Metadata item permissions should have the same size as the original json object");
//        for (MetadataPermission metadataPermission: metadataItem.getPermissions()) {
//            boolean found = false;
//            for (Iterator<JsonNode> it = node.get("permissions").elements(); it.hasNext(); ) {
//                JsonNode pemNode = it.next();
//                if (metadataPermission.getUsername().equals(pemNode.get("username").textValue())) {
//                    assertEquals(metadataPermission.getTenantId(), metadataItem.getTenantId(),
//                            "Metadata permission tenant id should be identical to metadata item");
//                    assertEquals(metadataPermission.getPermission().name(), pemNode.get("permission").textValue(),
//                            "Metadata item permission value should be identical to the value in the original json object");
//                    found = true;
//                    break;
//                }
//            }
//
//            if (!found) {
//                fail("All permissions from the original json should be present in the parsed metadata item");
//            }
//        }

        assertEquals(jsonHandler.getNotifications().size(), node.get("notifications").size(), "Metadata item notifications should have the same size as the original json object");
//        for (Notification metadataNotification: metadataItem.getNotifications()) {
//            boolean found = false;
//            for (Iterator<JsonNode> it = node.get("notifications").elements(); it.hasNext(); ) {
//                JsonNode pemNode = it.next();
//                if (metadataNotification.getEvent().equals(pemNode.get("event").textValue())) {
//                    assertEquals(metadataNotification.getTenantId(), metadataItem.getTenantId(),
//                            "Metadata notification tenant id should be identical to metadata item");
//                    assertEquals(metadataNotification.getCallbackUrl(), pemNode.get("url").textValue(),
//                            "Metadata notification url value should be identical to the value in the original json object");
//                    assertEquals(metadataNotification.isPersistent(), pemNode.get("persistent").asBoolean(false),
//                            "Metadata notification persistent value should be identical to the value in the original json object");
//                    assertEquals(metadataNotification.getOwner(), metadataItem.getOwner(),
//                            "Metadata notification owner value should be identical to the value in the original json object");
//                    assertNull(metadataNotification.getPolicy(),
//                            "Metadata notification policy should be null in these tests before and after.");
//                    found = true;
//                    break;
//                }
//            }
//
//            if (!found) {
//                fail("All notifications from the original json should be present in the parsed metadata item");
//            }
//        }
    }

    @Test
    public void parseJsonToDocumentTest() throws MetadataException, MetadataQueryException, MetadataStoreException, UUIDException, PermissionException {
        JsonNode node = createTestMetadataItem();

        JsonHandler jsonHandler = new JsonHandler();
        Document document = jsonHandler.parseJsonMetadataToDocument(node);
        assertNotNull(document,"Bson document should not be null after handler parses it");
        assertEquals(document.getString("uuid"), node.get("uuid").asText(), "Bson document uuid should be the uuid in the original json object");
        assertNotNull(document.get("value"), "Bson document value should not be null");
        assertEquals(document.get("value", Document.class).toJson().replaceAll("\\s+",""), node.get("value").toString().replaceAll("\\s+",""), "Bson document value should be the value in the original json object");
        assertEquals(document.getString("schemaId"), node.get("schemaId").textValue(), "Bson document schemaId should be the schemaId in the original json object");
        assertEquals(document.getString("name"), node.get("name").asText(), "Bson document name should be the name in the original json object");

        String[] associationIds = (String[])document.get("associationIds");
        if (node.get("associationIds").size() == 0) {
            assertNull(associationIds, "Bson document associationIds should not be null");
        } else {
            assertEquals(associationIds.length, node.get("associationIds").size(), "Metadata associated uuid should be the same size as in the original json object");
            assertEquals(associationIds[0], node.get("associationIds").get(0).asText(), "Metadata associated uuid should be the uuid in the original json object");
        }
        ArrayNode permissions = document.get("permissions", ArrayNode.class);
        assertEquals(permissions.size(), node.get("permissions").size(), "Bson document permissions should have the same size as the original json object");
//        for (MetadataPermission metadataPermission: metadataItem.getPermissions()) {
//            boolean found = false;
//            for (Iterator<JsonNode> it = node.get("permissions").elements(); it.hasNext(); ) {
//                JsonNode pemNode = it.next();
//                if (metadataPermission.getUsername().equals(pemNode.get("username").textValue())) {
//                    assertEquals(metadataPermission.getTenantId(), metadataItem.getTenantId(),
//                            "Metadata permission tenant id should be identical to metadata item");
//                    assertEquals(metadataPermission.getPermission().name(), pemNode.get("permission").textValue(),
//                            "Bson document permission value should be identical to the value in the original json object");
//                    found = true;
//                    break;
//                }
//            }
//
//            if (!found) {
//                fail("All permissions from the original json should be present in the parsed metadata item");
//            }
//        }

        ArrayNode notifications = document.get("notifications", ArrayNode.class);
        assertEquals(notifications.size(), node.get("notifications").size(), "Bson document notifications should have the same size as the original json object");
//        for (Notification metadataNotification: metadataItem.getNotifications()) {
//            boolean found = false;
//            for (Iterator<JsonNode> it = node.get("notifications").elements(); it.hasNext(); ) {
//                JsonNode pemNode = it.next();
//                if (metadataNotification.getEvent().equals(pemNode.get("event").textValue())) {
//                    assertEquals(metadataNotification.getTenantId(), metadataItem.getTenantId(),
//                            "Metadata notification tenant id should be identical to metadata item");
//                    assertEquals(metadataNotification.getCallbackUrl(), pemNode.get("url").textValue(),
//                            "Metadata notification url value should be identical to the value in the original json object");
//                    assertEquals(metadataNotification.isPersistent(), pemNode.get("persistent").asBoolean(false),
//                            "Metadata notification persistent value should be identical to the value in the original json object");
//                    assertEquals(metadataNotification.getOwner(), metadataItem.getOwner(),
//                            "Metadata notification owner value should be identical to the value in the original json object");
//                    assertNull(metadataNotification.getPolicy(),
//                            "Metadata notification policy should be null in these tests before and after.");
//                    found = true;
//                    break;
//                }
//            }
//
//            if (!found) {
//                fail("All notifications from the original json should be present in the parsed metadata item");
//            }
//        }
    }

    @Test(enabled = false)
    public void parseNameToStringTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertEquals(jsonHandler.parseNameToString(node), this.getClass().getName());

    }

    @Test(enabled = false)
    public void parseMissingNameToStringTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseNameToString(node));
    }

    @Test(enabled = false)
    public void parseValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        JsonNode parsedValue = jsonHandler.parseValueToJsonNode(node);
        Assert.assertEquals(parsedValue.get("testKey").asText(), "testValue");


    }

    @Test(enabled = false)
    public void parseMissingValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseValueToJsonNode(node));
    }

    @Test(enabled = false)
    public void parseAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException, UUIDException, MetadataException {

        AgaveUUID associationId = new AgaveUUID(UUIDType.JOB);
        AgaveUUID schemaId = new AgaveUUID(UUIDType.SCHEMA);

        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + associationId.toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + schemaId.toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        MetadataValidation mockValidation = mock(MetadataValidation.class);

        AssociatedReference associatedReference = new AssociatedReference(associationId, createResponseString(associationId));
        MetadataAssociationList associationList = new MetadataAssociationList();
        associationList.add(associatedReference);


        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIds_uuidApi(mapper.createArrayNode().add(associationId.toString()));
//        Mockito.doReturn(createResponseString(schemaId)).when(mockValidation).getValidationResponse(schemaId.toString());

        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.setMetadataValidation(mockValidation);
        MetadataAssociationList parsedAssociationList = jsonHandler.parseAssociationIdsToMetadataAssociationList(node);
        Assert.assertTrue(parsedAssociationList.getAssociatedIds().containsKey(associationId.toString()));
    }

    @Test(enabled = false)
    public void parseInvalidAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"00000-0000000-00000000\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(UUIDException.class, () -> jsonHandler.parseAssociationIdsToMetadataAssociationList(node));
    }

    @Test(enabled = false)
    public void parseNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        assertNotNull(jsonHandler.parseNotificationToArrayNode(node));
    }

    @Test(enabled = false)
    public void parseInvalidNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "\"" + "notifications" + "\"" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseNotificationToArrayNode(node));
    }

    @Test(enabled = false)
    public void parsePermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        assertNotNull(jsonHandler.parsePermissionToArrayNode(node));
    }

    @Test(enabled = false, expectedExceptions = MetadataQueryException.class)
    public void parseInvalidPermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "\"" + PermissionType.READ_WRITE + "\"" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        jsonHandler.parsePermissionToArrayNode(node);
    }

//    @Test
//    public void parseSchemaIdToStringTest() throws IOException, MetadataQueryException, PermissionException, MetadataStoreException {
//        String schemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
//        String strJson = "{" +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + schemaId + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//
//        MetadataValidation mockMetadataValidation = mock(MetadataValidation.class);
//        MetadataSchemaItem toReturnItem = new MetadataSchemaItem();
//        toReturnItem.setUuid(schemaId);
//        toReturnItem.setOwner("TEST_USER");
//        toReturnItem.setSchema(mapper.createObjectNode().put("Sample Name", "Sample Value"));
//
//        Document schemaDoc = new Document("uuid", schemaId)
//                .append("owner", "TEST_USER")
//                .append("schema", "{" +
//                        "\"title\": \"Example Schema\", " +
//                        "\"type\": \"object\", "+
//                        "\"properties\": {" +
//                        "\"species\": {" +
//                        "\"type\": \"string\"" +
//                        "}" +
//                        "}," +
//                        "\"required\": [" +
//                        "\"species\"" +
//                        "]" +
//                        "}");
//
//
//        Mockito.when(mockMetadataValidation.checkSchemaIdExists(schemaId)).thenReturn(schemaDoc);
//        jsonHandler.setMetadataValidation(mockMetadataValidation);
//
//        String parsedSchemaId = jsonHandler.parseSchemaIdToString(node);
//        Assert.assertEquals(parsedSchemaId, schemaId);
//    }

//    @Test
//    public void parseInvalidSchemaIdToStringTest() throws IOException, MetadataQueryException, PermissionException, MetadataStoreException {
//        String schemaId = new AgaveUUID(UUIDType.JOB).toString();
//
//        String strJson = "{" +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + schemaId + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertNull(jsonHandler.parseSchemaIdToString(node));
////        Assert.assertTrue(jsonHandler.parseSchemaIdToString(node).length() == 0);
//    }

//    @Test
//    public void validateValueAgainstSchemaTest() throws IOException, MetadataQueryException, MetadataSchemaValidationException {
//        String strSchemaJson = "" +
//                "{" +
//                "\"title\": \"Example Schema\", " +
//                "\"type\": \"object\", "+
//                "\"properties\": {" +
//                "\"species\": {" +
//                "\"type\": \"string\"" +
//                "}" +
//                "}," +
//                "\"required\": [" +
//                "\"species\"" +
//                "]" +
//                "}";
//
//
//        String strValue = "{" +
//                "\"title\": \"Some Metadata\", " +
//                "\"properties\": {" +
//                "\"species\": {" +
//                "\"type\": \"Some species type\"" +
//                "}" +
//                "}, " +
//                "\"species\": \"required\"" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        String validatedValue = jsonHandler.validateValueAgainstSchema(strValue, strSchemaJson);
//        Assert.assertEquals(validatedValue, strValue);
//    }
//
//    @Test
//    public void validateInvalidValueAgainstSchemaTest() throws IOException, MetadataQueryException, MetadataSchemaValidationException, MetadataException, PermissionException {
//        String strSchemaJson = "" +
//                "{" +
//                "\"title\": \"Example Schema\", " +
//                "\"type\": \"object\", "+
//                "\"properties\": {" +
//                "\"species\": {" +
//                "\"type\": \"string\"" +
//                "}" +
//                "}," +
//                "\"required\": [" +
//                "\"species\"" +
//                "]" +
//                "}";
//
//
//        String strValue = "{" +
//                "\"properties\": {" +
//                "\"species\": {" +
//                "\"type\": \"Some species type\"" +
//                "}" +
//                "}" +
//                "}";
//
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertThrows(MetadataSchemaValidationException.class, ()-> jsonHandler.validateValueAgainstSchema(strValue, strSchemaJson));
//    }

    public String createResponseString(AgaveUUID uuid) throws UUIDException {
        return "  {" +
                "    \"uuid\": \"" + uuid.toString() + "\"," +
                "    \"type\": \"" + uuid.getResourceType().toString() + "\"," +
                "    \"_links\": {" +
                "      \"self\": {" +
                "        \"href\": \"http://docker.example.com/meta/v2/data/" + uuid.toString() + "\"" +
                "      }" +
                "    }" +
                "  }";
    }

}

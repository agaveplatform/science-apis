package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataValidationException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.*;

@Test(groups = {"unit"})
public class JsonHandlerTest {

    ObjectMapper mapper = new ObjectMapper();

    public static final String TEST_USER = "testuser";
    public static final String TEST_SHARE_USER = "testshare";

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
                .put("schemaId", new AgaveUUID(UUIDType.SCHEMA).toString()) // optional, can be null
                .put("name", UUID.randomUUID().toString());
        node.putObject("value")
                .put("testKey", "testValue");
        node.putArray("associationIds") // optional and can be empty
                .add(new AgaveUUID(UUIDType.SCHEMA).toString());
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


    /**
     * Generates a JsonNode representing a test metadata item
     *
     * @param schema           schemaId String for the schemaId field
     * @param useAssociationId if true, use default associationID, otherwise empty array
     * @param usePermissions   if true, use default permissions for optional array, otherwise array is not added
     * @param useNotification  if true, use default notification for optional array, otherwise array is not added
     * @return json representation of a metadata item request value
     */
    protected JsonNode createTestMetadataItem(String schema, boolean useAssociationId, boolean usePermissions, boolean useNotification) {
        ObjectNode node = mapper.createObjectNode()
                .put("uuid", new AgaveUUID(UUIDType.METADATA).toString())
                .put("name", UUID.randomUUID().toString());
        node.putObject("value")
                .put("testKey", "testValue");

        node.put("schemaId", schema); // optional, can be null

        if (useAssociationId) {
            node.putArray("associationIds")
                    .add(new AgaveUUID(UUIDType.SCHEMA).toString()); //optional and can be empty
        } else {
            node.putArray("associationIds");
        }

        if (usePermissions)         // optional array in request
            node.putArray("permissions").addObject()
                    .put("username", TEST_SHARE_USER)
                    .put("permission", PermissionType.READ.toString());

        if (useNotification)        // optional array in request
            node.putArray("notifications").addObject()
                    .put("url", "foo@example.com")
                    .put("event", "CREATED")
                    .put("persistent", false);

        return node;
    }

    @DataProvider(name = "initMetadataItemsProvider")
    public Object[][] initOtMetadataItemsProvider() {
        String schemaId = new AgaveUUID(UUIDType.SCHEMA).toString();

        return new Object[][]{
                {createTestMetadataItem(schemaId, true, true, true)},
                {createTestMetadataItem(null, true, true, true)},
                {createTestMetadataItem(schemaId, false, true, true)},
                {createTestMetadataItem(schemaId, true, false, true)},
                {createTestMetadataItem(schemaId, true, true, false)},
                {createTestMetadataItem(null, false, false, false)}
        };
    }


    @Test(enabled = false)
    public void parseJsonToMetadataTest() throws MetadataValidationException {
        JsonNode node = createTestMetadataItem();

        JsonHandler jsonHandler = new JsonHandler();
        MetadataItem metadataItem = jsonHandler.parseJsonMetadata(node);
        assertNotNull(metadataItem, "Metadata item should not be null after handler parses it");
        assertEquals(metadataItem.getUuid(), node.get("uuid").asText(), "Metadata item uuid should be the uuid in the original json object");
        assertNotNull(metadataItem.getValue(), "Metadata item value should not be null");
        assertEquals(metadataItem.getValue().asText(), node.get("value").asText(), "Metadata item value should be the value in the original json object");
        assertEquals(metadataItem.getSchemaId(), node.get("schemaId").textValue(), "Metadata item schemaId should be the schemaId in the original json object");
        assertEquals(metadataItem.getName(), node.get("name").asText(), "Metadata item name should be the name in the original json object");

        assertEquals(metadataItem.getAssociations().size(), node.get("associationIds").size(), "Metadata associated uuid should be the same size as in the original json object");
        if (metadataItem.getAssociations().size() > 0) {
            assertEquals(metadataItem.getAssociations().getRawUuid().iterator().next(), node.get("associationIds").get(0).asText(), "Metadata associated uuid should be the uuid in the original json object");
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

    @Test(dataProvider = "initMetadataItemsProvider")
    public void parseJsonToDocumentTest(JsonNode node) throws MetadataQueryException {
//        JsonNode node = createTestMetadataItem();
        JsonHandler jsonHandler = new JsonHandler();
        Document document = jsonHandler.parseJsonMetadataToDocument(node);
        assertNotNull(document, "Bson document should not be null after handler parses it");
        assertEquals(document.getString("uuid"), node.get("uuid").asText(), "Bson document uuid should be the uuid in the original json object");
        assertNotNull(document.get("value"), "Bson document value should not be null");
        assertEquals(document.get("value", Document.class).toJson().replaceAll("\\s+", ""), node.get("value").toString().replaceAll("\\s+", ""), "Bson document value should be the value in the original json object");
        assertEquals(document.getString("schemaId"), node.get("schemaId").textValue(), "Bson document schemaId should be the schemaId in the original json object");
        assertEquals(document.getString("name"), node.get("name").asText(), "Bson document name should be the name in the original json object");

        List<String> associationIds = (List<String>) document.get("associationIds");

        if (node.get("associationIds").size() == 0) {
            assertNull(associationIds, "Bson document associationIds should not be null");
        } else {
            assertEquals(associationIds.size(), node.get("associationIds").size(), "Metadata associated uuid should be the same size as in the original json object");
            assertEquals(associationIds.get(0), node.get("associationIds").get(0).asText(), "Metadata associated uuid should be the uuid in the original json object");
        }

        List<Document> permissions = (List<Document>) document.get("permissions");
        if (node.get("permissions") == null)
            assertNull(permissions, "Bson document permissions should be null if it doesn't exist in original json object");
        else {
            assertEquals(permissions.size(), node.get("permissions").size(), "Bson document permissions should have the same size as the original json object");

            for (Document pemDoc : permissions) {
                boolean found = false;
                for (Iterator<JsonNode> it = node.get("permissions").elements(); it.hasNext(); ) {
                    JsonNode pemNode = it.next();
                    if (pemDoc.getString("username").equals(pemNode.get("username").textValue())) {
                        assertEquals(pemDoc.getString("permission"), pemNode.get("permission").textValue(),
                                "Bson document permission value should be identical to the value in the original json object");
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    fail("All permissions from the original json should be present in the parsed metadata item");
                }
            }
        }

        List<Document> notifications = (List<Document>) document.get("notifications");

        if (node.get("notifications") == null)
            assertNull(notifications, "Bson document notifications should be null if it doesn't exist in original json object");
        else {
            assertEquals(notifications.size(), node.get("notifications").size(), "Bson document notifications should have the same size as the original json object");

            for (Document notifDoc : notifications) {
                boolean found = false;
                for (Iterator<JsonNode> it = node.get("notifications").elements(); it.hasNext(); ) {
                    JsonNode pemNode = it.next();
                    if (notifDoc.getString("event").equals(pemNode.get("event").textValue())) {
                        assertEquals(notifDoc.getString("url"), pemNode.get("url").textValue(),
                                "Metadata notification url value should be identical to the value in the original json object");
                        assertEquals(Boolean.parseBoolean(notifDoc.getString("persistent")), pemNode.get("persistent").asBoolean(false),
                                "Metadata notification persistent value should be identical to the value in the original json object");
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    fail("All notifications from the original json should be present in the parsed metadata item");
                }
            }
        }
    }

    @Test
    public void parseObjectArrayNodeToListTest() {
        JsonHandler jsonHandler = new JsonHandler();
        ArrayNode node = mapper.createArrayNode();
        node.addObject()
                .put("username", TEST_USER)
                .put("permission", PermissionType.READ_WRITE.toString());
        node.addObject()
                .put("username", TEST_SHARE_USER)
                .put("permission", PermissionType.READ.toString());

        List<Document> documents = jsonHandler.parseObjectArrayNodeToList(node);

        assertEquals(documents.size(), node.size(), "Document size should match the original json array size");

        for (Document doc : documents) {
            boolean found = false;

            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                JsonNode pemNode = it.next();
                if (doc.getString("username").equals(pemNode.get("username").asText())) {
                    assertEquals(doc.getString("permission"), pemNode.get("permission").asText(), "Values should match the original json array values.");
                }
            }
        }
    }

    @Test
    public void parseNestedJsonNodeToDocumentTest() {
        JsonHandler jsonHandler = new JsonHandler();
        ObjectNode node = mapper.createObjectNode();
        node.putObject("properties")
                .putObject("description")
                .put("species", "a sample species")
                .put("active", false);

        Document document = jsonHandler.parseNestedJsonNodeToDocument(node);

        assertEquals(document.getEmbedded(List.of("properties", "description", "species"), String.class), node.get("properties").get("description").get("species").asText(),
                "Nested string value in document should match value in original json object.");
        assertEquals(Boolean.parseBoolean(document.getEmbedded(List.of("properties", "description", "active"), String.class)), node.get("properties").get("description").get("active").asBoolean(),
                "Nested boolean value in document should match value in original json object.");
    }

    @Test(enabled = false)
    public void parseNameToStringTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertEquals(jsonHandler.parseNameToString(node), this.getClass().getName());

    }

    @Test(enabled = false)
    public void parseMissingNameToStringTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseNameToString(node));
    }

    @Test(enabled = false)
    public void parseValueToJsonNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        JsonNode parsedValue = jsonHandler.parseValueToJsonNode(node);
//        Assert.assertEquals(parsedValue.get("testKey").asText(), "testValue");


    }

    @Test(enabled = false)
    public void parseMissingValueToJsonNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseValueToJsonNode(node));
    }

    @Test(enabled = false)
    public void parseAssociationIdsToArrayNodeTest() {

//        AgaveUUID associationId = new AgaveUUID(UUIDType.JOB);
//        AgaveUUID schemaId = new AgaveUUID(UUIDType.SCHEMA);
//
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + associationId.toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + schemaId.toString() + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        MetadataValidation mockValidation = mock(MetadataValidation.class);
//
//        AssociatedReference associatedReference = new AssociatedReference(associationId, createResponseString(associationId));
//        MetadataAssociationList associationList = new MetadataAssociationList();
//        associationList.add(associatedReference);
//
//
//        Mockito.doReturn(associationList).when(mockValidation).checkAssociationIdsUuidApi(mapper.createArrayNode().add(associationId.toString()));
////        Mockito.doReturn(createResponseString(schemaId)).when(mockValidation).getValidationResponse(schemaId.toString());
//
//        JsonHandler jsonHandler = new JsonHandler();
//        jsonHandler.setMetadataValidation(mockValidation);
//        MetadataAssociationList parsedAssociationList = jsonHandler.parseAssociationIdsToMetadataAssociationList(node);
//        Assert.assertTrue(parsedAssociationList.getAssociatedIds().containsKey(associationId.toString()));
    }

    @Test(enabled = false)
    public void parseInvalidAssociationIdsToListTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"00000-0000000-00000000\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        List<String> parsedAssociationList = jsonHandler.parseAssociationIdsToList(node);
//        assertEquals(parsedAssociationList.size(), 1, "AssociationId list size should match the json object size.");
//
    }

    @Test(enabled = false)
    public void parseNotificationToArrayNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        assertNotNull(jsonHandler.parseNotificationToArrayNode(node));
    }

    @Test(enabled = false)
    public void parseInvalidNotificationToArrayNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
//                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
//                "\"notifications\": " + "\"" + "notifications" + "\"" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseNotificationToArrayNode(node));
    }

    @Test(enabled = false)
    public void parsePermissionToArrayNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
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
//        JsonHandler jsonHandler = new JsonHandler();
//        assertNotNull(jsonHandler.parsePermissionToArrayNode(node));
    }

    @Test(enabled = false, expectedExceptions = MetadataQueryException.class)
    public void parseInvalidPermissionToArrayNodeTest() {
//        String strJson = "{" +
//                "\"id\": " + "\"" + new AgaveUUID(UUIDType.METADATA).toString() + "\"" + "," +
//                "\"name\": \"" + getClass().getName() + "\"," +
//                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
//                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
//                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
//                "\"permissions\": " + "\"" + PermissionType.READ_WRITE + "\"" + "," +
//                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
//                "}";
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonFactory factory = mapper.getFactory();
//        JsonNode node = factory.createParser(strJson).readValueAsTree();
//
//        JsonHandler jsonHandler = new JsonHandler();
//        jsonHandler.parsePermissionToArrayNode(node);
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

    public String createResponseString(AgaveUUID uuid) {
        return "  {" +
                "    \"uuid\": \"" + uuid.toString() + "\"," +
                "    \"type\": \"" + uuid.getResourceType().toString() + "\"," +
                "    \"_links\": {" +
                "      \"self\": {" +
                "        \"href\": \"http://docker.example.com/meta/v2/data/" + uuid + "\"" +
                "      }" +
                "    }" +
                "  }";
    }

}

package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.Settings;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataSchemaValidationException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.model.serialization.MetadataCollectionSerializerModifier;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.joda.time.DateTime;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.iplantc.service.metadata.Settings.METADATA_DB_SCHEME;
import static org.powermock.api.mockito.PowerMockito.mock;

@Test(groups = {"integration"})
public class JsonHandlerIT {

    @Mock
    MetadataValidation metadataValidation;

    @InjectMocks
    JsonHandler mockJsonHandler;

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void parseJsonToMetadataTest() throws MetadataQueryException, IOException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + new AgaveUUID(UUIDType.JOB).toString() + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE.toString() + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertNotNull(jsonHandler.getMetadataItem());
    }

    @Test
    public void parseNameToStringTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parseNameToString(node));
    }

    @Test
    public void parseValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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

    @Test
    public void parseMissingValueToJsonNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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

    @Test
    public void parseAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException, UUIDException, MetadataException {

        AgaveUUID associationId = new AgaveUUID(UUIDType.JOB);
        AgaveUUID schemaId = new AgaveUUID(UUIDType.SCHEMA);

        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
        MetadataAssociationList parsedAssociationList = jsonHandler.parseAssociationIdsToArrayNode(node);
        Assert.assertTrue(parsedAssociationList.getAssociatedIds().containsKey(associationId.toString()));
    }

    @Test
    public void parseInvalidAssociationIdsToArrayNodeTest() throws IOException, MetadataQueryException {
        String associationId = "00000-0000000-00000000";

        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
                "\"value\": " + "{\"testKey\":\"testValue\"}" + "," +
                "\"associationIds\": " + "[" + "\"" + associationId + "\"" + "]" + "," +
                "\"schemaId\": " + "\"" + new AgaveUUID(UUIDType.SCHEMA).toString() + "\"" + "," +
                "\"permissions\": " + "[\"" + PermissionType.READ_WRITE + "\"]" + "," +
                "\"notifications\": " + "[\"" + "notifications" + "\"]" + "" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();
        Assert.assertThrows(UUIDException.class, () -> jsonHandler.parseAssociationIdsToArrayNode(node));
    }

    @Test
    public void parseNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
        Assert.assertNotNull(jsonHandler.parseNotificationToArrayNode(node));
    }

    @Test
    public void parseInvalidNotificationToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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

    @Test
    public void parsePermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
        Assert.assertNotNull(jsonHandler.parsePermissionToArrayNode(node));
    }

    @Test
    public void parseInvalidPermissionToArrayNodeTest() throws IOException, MetadataQueryException {
        String strJson = "{" +
                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
        Assert.assertThrows(MetadataQueryException.class, () -> jsonHandler.parsePermissionToArrayNode(node));
    }

//    @Test
//    public void parseSchemaIdToStringTest() throws IOException, MetadataQueryException, PermissionException, MetadataStoreException {
//        String schemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
//        String strJson = "{" +
//                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
//                "\"name\": \"" + JsonHandlerIT.class.getName() + "\"," +
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
                "        \"href\": \"" + TenancyHelper.resolveURLToCurrentTenant(uuid.getObjectReference()) + "\"" +
                "      }" +
                "    }" +
                "  }";
    }

}

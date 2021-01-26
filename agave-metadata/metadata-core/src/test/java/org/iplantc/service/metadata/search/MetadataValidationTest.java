package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.util.ServiceUtils;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MetadataValidationTest {
    ObjectMapper mapper = new ObjectMapper();
    String username = "TEST_USER";

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

    private String setupSchema() {
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());
        MongoClient mongoClient = new com.mongodb.MongoClient(
                new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT),
                mongoCredential,
                MongoClientOptions.builder().build());
        MongoDatabase mongoDatabase = mongoClient.getDatabase(org.iplantc.service.metadata.Settings.METADATA_DB_SCHEME);
        MongoCollection schemaCollection = mongoDatabase.getCollection(org.iplantc.service.metadata.Settings.METADATA_DB_SCHEMATA_COLLECTION);


        String schemaUuid = new AgaveUUID(UUIDType.SCHEMA).toString();

        String strItemToAdd = "{" +
                "\"order\" : \"sample order\"," +
                "\"type\" : \"object\", " +
                "\"properties\" : {" +
                "\"profile\" : { \"type\" : \"string\" }, " +
                "\"description\" : { \"type\" : \"string\" }, " +
                "\"status\" : {\"enum\" : [\"active\", \"retired\", \"disabled\"]}" +
                "}" +
                "}";

        Document doc;
        String timestamp = new DateTime().toString();
        doc = new Document("internalUsername", this.username)
                .append("lastUpdated", timestamp)
                .append("schema", ServiceUtils.escapeSchemaRefFieldNames(strItemToAdd))
                .append("uuid", schemaUuid)
                .append("created", timestamp)
                .append("owner", this.username)
                .append("tenantId", TenancyHelper.getCurrentTenantId());

        schemaCollection.insertOne(doc);

        return schemaUuid;
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
            validation.validateMetadataNodeFields(node, username);
            if (bolThrowException)
                fail(message);
        } catch (Exception e) {
            if (!bolThrowException)
                fail(message);
        }
    }
}

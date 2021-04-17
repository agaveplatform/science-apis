package org.iplantc.service.metadata.model.validation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataSchemaValidationException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.iplantc.service.metadata.search.JsonHandler;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertTrue;

public class MetadataSchemaComplianceValidatorIT {
    private String username = "TEST_USER";

    @Test
    public void validMetadataValueConstraintTest() throws IOException, MetadataQueryException, MetadataSchemaValidationException, MetadataStoreException {
        String strSchemaJson = "{" +
                "\"title\": \"Example Schema\", " +
                "\"type\": \"object\", "+
                "\"properties\": {" +
                    "\"species\": {" +
                        "\"type\": \"string\"" +
                    "}" +
                "}," +
                "\"required\": [" +
                    "\"species\"" +
                "]" +
            "}";

        String validSchemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
        Assert.assertNotNull(createSchema(validSchemaId,strSchemaJson));

        String strValue = "{" +
                "\"title\": \"Some Metadata\", " +
                "\"species\": {" +
                    "\"type\": \"Some species type\"" +
                "}, " +
                "\"species\": \"required\"" +
            "}";

        String strJson = "{" +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + strValue + "," +
                "\"schemaId\": " + "\"" + validSchemaId + "\"" +
            "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();

        MetadataItem validItem = new MetadataItem();
        validItem.setValue(jsonHandler.parseValueToJsonNode(node));
        validItem.setName(jsonHandler.parseNameToString(node));
        validItem.setSchemaId(validSchemaId);
        validItem.setTenantId(TenancyHelper.getCurrentTenantId());

        validItem.setOwner(this.username);
        validItem.setInternalUsername(this.username);

        //validator
        Validator validator;
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();

        Set<ConstraintViolation<MetadataItem>> constraintViolations = validator.validate(validItem);

        Assert.assertEquals(constraintViolations.size(), 0);
    }

    @Test
    public void invalidMetadataValueConstraintTest() throws IOException, MetadataQueryException, MetadataSchemaValidationException, MetadataStoreException {
        String strSchemaJson = "{" +
                "\"title\": \"Example Schema\", " +
                "\"type\": \"object\", "+
                "\"properties\": {" +
                    "\"species\": {" +
                        "\"type\": \"string\"" +
                    "}" +
                "}," +
                "\"required\": [" +
                    "\"species\"" +
                "]" +
            "}";

        String schemaId = new AgaveUUID(UUIDType.SCHEMA).toString();
        Assert.assertNotNull(createSchema(schemaId,strSchemaJson));

        String strValue = "{" +
                "\"title\": \"Some Metadata\", " +
                "\"properties\": {" +
                    "\"species\": {" +
                        "\"type\": \"Some species type\"" +
                    "}" +
                "}" +
            "}";

        String strJson = "{" +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + strValue + "," +
                "\"schemaId\": " + "\"" + schemaId + "\"" +
            "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();

        MetadataItem invalidItem = new MetadataItem();
        invalidItem.setValue(jsonHandler.parseValueToJsonNode(node));
        invalidItem.setName(jsonHandler.parseNameToString(node));
        invalidItem.setSchemaId(schemaId);
        invalidItem.setTenantId(TenancyHelper.getCurrentTenantId());

        invalidItem.setOwner(this.username);
        invalidItem.setInternalUsername(this.username);

        //validator
        Validator validator;
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();

        Set<ConstraintViolation<MetadataItem>> constraintViolations = validator.validate(invalidItem);

        Assert.assertEquals(constraintViolations.size(), 1);
        Assert.assertTrue(constraintViolations.iterator().next().getMessage().contains("Metadata value does not conform to schema"));
    }

    @Test
    public void invalidSchemaIDConstraintTest() throws IOException, MetadataStoreException, MetadataQueryException {
        String invalidSchemaId = new AgaveUUID(UUIDType.JOB).toString();

        String strValue = "{" +
                "\"title\": \"Some Metadata\", " +
                "\"properties\": {" +
                    "\"species\": {" +
                        "\"type\": \"Some species type\"" +
                    "}" +
                "}, " +
                "\"required\": [" +
                    "\"species\"" +
                "]" +
            "}";

        String strJson = "{" +
                "\"name\": \"" + getClass().getName() + "\"," +
                "\"value\": " + strValue + "," +
                "\"species\": \"homo sapian\"," +
                "\"schemaId\": " + "\"" + invalidSchemaId + "\"" +
            "}";

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(strJson).readValueAsTree();

        JsonHandler jsonHandler = new JsonHandler();

        MetadataItem validItem = new MetadataItem();
        validItem.setValue(jsonHandler.parseValueToJsonNode(node));
        validItem.setName(jsonHandler.parseNameToString(node));
        validItem.setSchemaId(invalidSchemaId);
        validItem.setTenantId(TenancyHelper.getCurrentTenantId());

        validItem.setOwner(this.username);
        validItem.setInternalUsername(this.username);

        //validator
        Validator validator;
        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();

        Set<ConstraintViolation<MetadataItem>> constraintViolations = validator.validate(validItem);
        try {
            Assert.assertTrue(constraintViolations.stream()
                    .anyMatch(cv -> cv.getMessage().equals(
                            invalidSchemaId + " is not a valid schema id")));
        } catch (Throwable t) {
            System.out.println(constraintViolations.toString());
            throw t;
        }
    }


    public String createSchema(String schemaId, String schemaJson) throws IOException, MetadataStoreException {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonNode node = factory.createParser(schemaJson).readValueAsTree();

        try {
            MetadataSchemaItem toInsert = new MetadataSchemaItem();
            toInsert.setSchema(node);
            toInsert.setOwner(this.username);
            toInsert.setInternalUsername(this.username);
            toInsert.setUuid(schemaId);
            MetadataSchemaDao.getInstance().insert(toInsert);

            return schemaId;
        } catch (Exception e) {
            return null;
        }
    }
}

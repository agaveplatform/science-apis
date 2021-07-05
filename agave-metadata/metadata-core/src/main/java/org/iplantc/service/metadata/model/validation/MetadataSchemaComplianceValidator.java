package org.iplantc.service.metadata.model.validation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataValidationException;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.validation.constraints.MetadataSchemaComplianceConstraint;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MetadataSchemaComplianceValidator implements ConstraintValidator<MetadataSchemaComplianceConstraint, Object> {

    private static final Logger log = Logger.getLogger(MetadataSchemaComplianceValidator.class);
    private String valueFieldName;
    private String schemaFieldName;

    @Override
    public void initialize(final MetadataSchemaComplianceConstraint constraintAnnotation) {
        valueFieldName = constraintAnnotation.valueField();
        schemaFieldName = constraintAnnotation.schemaIdField();
    }

    @Override
    public boolean isValid(Object target, final ConstraintValidatorContext constraintContext) {

        boolean isValid = false;


        try {
            final Object metadataValue = BeanUtils.getProperty(target, valueFieldName);
            final Object schemaId = BeanUtils.getProperty(target, schemaFieldName);

            if (StringUtils.isEmpty((String) schemaId)) {
                isValid = true;
            } else {
                Bson query = and(eq("uuid", (String) schemaId),
                        eq("tenantId", TenancyHelper.getCurrentTenantId()));
                Document schemaDoc = MetadataSchemaDao.getInstance().findOne(query);


                // only care about when there is a schema for the given schemaId
                if (schemaDoc != null) {
                    String owner = schemaDoc.getString("owner");
                    MetadataSchemaPermissionManager schemaPM = new MetadataSchemaPermissionManager((String) schemaId, owner);

                    // now validate the json against the schema
                    Document schema = (Document) schemaDoc.get("schema");

                    ObjectMapper mapper = new ObjectMapper();
                    JsonFactory factory = mapper.getFactory();
//                    ObjectReader reader = mapper.reader(JsonNode.class);

                    String strSchema = schema.toJson();
                    JsonNode jsonSchemaNode = factory.createParser(strSchema).readValueAsTree();

                    JsonNode jsonMetadataNode = null;
                    if (metadataValue instanceof String) {
                        jsonMetadataNode = factory.createParser((String) metadataValue).readValueAsTree();
                    } else if (!(metadataValue instanceof JsonNode)) {
                        jsonMetadataNode = mapper.valueToTree(metadataValue);
                    } else {
                        jsonMetadataNode = (JsonNode) metadataValue;
                    }

                    AgaveJsonValidator validator = AgaveJsonSchemaFactory.byDefault().getValidator();
                    ProcessingReport report = validator.validate(jsonSchemaNode, jsonMetadataNode);

                    isValid = report.isSuccess();
                    if (!isValid) {
                        StringBuilder sb = new StringBuilder();
                        for (ProcessingMessage processingMessage : report) {
                            sb.append(processingMessage.toString());
                            sb.append("\n");
                        }
                        throw new MetadataValidationException(
                                "Metadata value does not conform to schema. \n" + sb);
                    }

                }
            }
        } catch (MetadataValidationException e) {
            constraintContext.disableDefaultConstraintViolation();
            constraintContext.buildConstraintViolationWithTemplate(
                    e.getMessage())
                    .addConstraintViolation();


        } catch (MetadataException e) {
            constraintContext.disableDefaultConstraintViolation();
            constraintContext.buildConstraintViolationWithTemplate(
                    "Unable to fetch metadata schema permissions")
                    .addConstraintViolation();
        } catch (Exception e) {
            constraintContext.disableDefaultConstraintViolation();
            constraintContext.buildConstraintViolationWithTemplate(
                    "Unexpected error while validating metadata value against schema.")
                    .addConstraintViolation();
        }

        return isValid;
    }
}

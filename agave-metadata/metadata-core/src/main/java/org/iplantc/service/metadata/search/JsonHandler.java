package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.mongodb.BasicDBObject;
import io.grpc.Metadata;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataSchemaValidationException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.managers.MetadataSchemaPermissionManager;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.iplantc.service.metadata.model.validation.MetadataSchemaComplianceValidator;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Parse JsonNode and validate
 */
public class JsonHandler {
    private ArrayNode permissions;
    private ArrayNode notifications;
    private MetadataValidation metadataValidation;

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }
    public void setMetadataValidation(MetadataValidation paramValidation) { this.metadataValidation = paramValidation;}

    private MetadataItem metadataItem;
    ObjectMapper mapper = new ObjectMapper();

    public JsonHandler() {
        this.metadataItem = new MetadataItem();
    }


    /**
     * Parse String in Json format to {@link JsonNode}
     * @param strJson String in Json format
     * @return JsonNode of {@code strJson}
     * @throws MetadataQueryException if {@code strJson} is invalid json format
     */
    public JsonNode parseStringToJson(String strJson) throws MetadataQueryException {
        try {
            JsonFactory factory = new ObjectMapper().getFactory();
            return factory.createParser(strJson).readValueAsTree();
        } catch (IOException e){
            throw new MetadataQueryException("Invalid Json format: " + e.getMessage());
        }
    }

    /**
     * Parse JsonString {@code userQuery} to {@link Document}
     * Regex will also be parsed
     *
     * @param strJson JsonString to parse
     * @return {@link Document} of the JsonString {@code userQuery}
     * @throws MetadataQueryException if invalid Json format
     */
    public Document parseStringToDocument(String strJson) throws MetadataQueryException {
        Document doc = new Document();
        if (StringUtils.isNotEmpty(strJson)){
            try {
                doc = Document.parse(strJson);
                for (String key : doc.keySet()){
                    if (doc.get(key) instanceof String){
                        if (((String) doc.get(key)).contains("*")) {
                            Pattern regexPattern = Pattern.compile((String) doc.getString(key),
                                    Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
                            doc.put(key, regexPattern);
                        }
                    }
                }
            } catch (Exception e) {
                throw new MetadataQueryException("Unable to parse query ", e);
            }
        }
        return doc;
    }

    /**
     * Parse {@link JsonNode} to {@link MetadataItem} with verified associatedIds
     *
     * @param jsonMetadata {@link JsonNode} parse from the query string
     * @throws MetadataQueryException if query values are missing or invalid
     */
    public void parseJsonMetadata(JsonNode jsonMetadata) throws MetadataQueryException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode items = mapper.createArrayNode();
            this.permissions = mapper.createArrayNode();
            this.notifications = mapper.createArrayNode();

            metadataItem.setName(parseNameToString(jsonMetadata));
            metadataItem.setValue(parseValueToJsonNode(jsonMetadata));
            metadataItem.setAssociations(parseAssociationIdsToArrayNode(jsonMetadata));
            metadataItem.setSchemaId(parseSchemaIdToString(jsonMetadata));
            this.permissions = parsePermissionToArrayNode(jsonMetadata);
            this.notifications = parseNotificationToArrayNode(jsonMetadata);

        } catch (MetadataQueryException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataQueryException(
                    "Unable to parse form. " + e.getMessage());
        }
    }



    /**
     * Get {@link JsonNode} name field
     *
     * @param nameNode
     * @return
     * @throws MetadataQueryException
     */
    public String parseNameToString(JsonNode nameNode) throws MetadataQueryException {
        if (nameNode.has("name") && nameNode.get("name").isTextual()
                && !nameNode.get("name").isNull()) {
            metadataItem.setName(nameNode.get("name").asText());
        } else {
            throw new MetadataQueryException(
                    "No name attribute specified. Please associate a value with the metadata name.");
        }
        return nameNode.get("name").asText();
    }

    /**
     * Get {@link JsonNode} value field
     *
     * @param valueNode to be parsed
     * @return {@link JsonNode} of {@code value}
     * @throws MetadataQueryException if invalid json format
     */
    public JsonNode parseValueToJsonNode(JsonNode valueNode) throws MetadataQueryException {
        if (valueNode.has("value") && !valueNode.get("value").isNull()) {
            return valueNode.get("value");
        } else
            throw new MetadataQueryException(
                    "No value attribute specified. Please associate a value with the metadata value.");
    }


    /**
     * Get {@JsonNode} associationIds field and validate the uuids
     *
     * @param associationNode
     * @return
     */
    public MetadataAssociationList parseAssociationIdsToArrayNode(JsonNode associationNode) throws UUIDException, MetadataException, MetadataQueryException {
        mapper = new ObjectMapper();

        ArrayNode associationItems = mapper.createArrayNode();

        if (associationNode.get("associationIds").isArray()) {
            associationItems = (ArrayNode) associationNode.get("associationIds");
        } else {
            if (associationNode.get("associationIds").isTextual())
                associationItems.add(associationNode.get("associationIds").asText());
        }

        //validate ids?
        if (metadataValidation == null)
            metadataValidation = new MetadataValidation();
        MetadataAssociationList metadataAssociationList = metadataValidation.checkAssociationIds_uuidApi(associationItems);
        return metadataAssociationList;
    }

    /**
     * Get the {@JsonNode} notifications field
     *
     * @param notificationNode
     * @return
     * @throws MetadataQueryException
     */
    public ArrayNode parseNotificationToArrayNode(JsonNode notificationNode) throws MetadataQueryException {
        if (notificationNode.hasNonNull("notifications")) {
            if (notificationNode.get("notifications").isArray()) {
                return (ArrayNode) notificationNode.get("notifications");
            } else {
                throw new MetadataQueryException(
                        "Invalid notifications value. notifications should be an "
                                + "JSON array of notification objects.");
            }
        }
        return null;
    }

    /**
     * Get {@JsonNode} permissions field
     *
     * @param permissionNode
     * @return
     * @throws MetadataQueryException
     */
    public ArrayNode parsePermissionToArrayNode(JsonNode permissionNode) throws MetadataQueryException {
        if (permissionNode.hasNonNull("permissions")) {
            if (permissionNode.get("permissions").isArray()) {
                return (ArrayNode) permissionNode.get("permissions");
            } else {
                throw new MetadataQueryException(
                        "Invalid permissions value. permissions should be an "
                                + "JSON array of permission objects.");
            }
        }
        return null;
    }

    /**
     * Get {@link JsonNode} schemaId field and verify the metadata schema exists
     *
     * @param schemaNode {@link JsonNode} to get schemaId from
     * @return schemaId if metadata schema exists and {@code username} has permissions to view it
     * @throws MetadataStoreException if unable to connect to the mongo collection
     * @throws PermissionException    if user doesn't have permissions to view the metadata schema
     */
    public String parseSchemaIdToString(JsonNode schemaNode) throws MetadataStoreException, PermissionException {
        if (schemaNode.has("schemaId") && schemaNode.get("schemaId").isTextual()) {

            if (metadataValidation == null)
                metadataValidation = new MetadataValidation();

            Document schemaDoc = metadataValidation.checkSchemaIdExists(schemaNode.get("schemaId").asText());

            //where to check for permission?
            if (schemaDoc != null)
                return schemaNode.get("schemaId").asText();
        }
        return "";
    }

    /**
     * Validate given JsonNode against the schemaId
     */
    public String validateValueAgainstSchema(String value, String schema) throws MetadataQueryException, MetadataSchemaValidationException {
        try {
            JsonFactory factory = new ObjectMapper().getFactory();
            JsonNode jsonSchemaNode = factory.createParser(schema).readValueAsTree();
            JsonNode jsonMetadataNode = factory.createParser(value).readValueAsTree();
            AgaveJsonValidator validator = AgaveJsonSchemaFactory.byDefault().getValidator();

            ProcessingReport report = validator.validate(jsonSchemaNode, jsonMetadataNode);
            if (!report.isSuccess()) {
                StringBuilder sb = new StringBuilder();
                for (ProcessingMessage processingMessage : report) {
                    sb.append(processingMessage.toString());
                    sb.append("\n");
                }
                return sb.toString();
            }
            return value;
        } catch (Exception e) {
            throw new MetadataSchemaValidationException("Metadata does not conform to schema.");
        }
    }
}

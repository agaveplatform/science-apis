package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.main.AgaveJsonSchemaFactory;
import com.github.fge.jsonschema.main.AgaveJsonValidator;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.metadata.exceptions.*;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Parse JsonNode and validate
 */
public class JsonHandler {
    private ArrayNode permissions;
    private ArrayNode notifications;
    private MetadataValidation metadataValidation;
    private MetadataItem metadataItem = new MetadataItem();
    ObjectMapper mapper = new ObjectMapper();


    public ArrayNode getPermissions() {
        return permissions;
    }

    public void setPermissions(ArrayNode permissions) {
        this.permissions = permissions;
    }

    public ArrayNode getNotifications() {
        return notifications;
    }

    public void setNotifications(ArrayNode notifications) {
        this.notifications = notifications;
    }

    public MetadataValidation getMetadataValidation() {
        return metadataValidation;
    }

    public void setMetadataValidation(MetadataValidation paramValidation) {
        this.metadataValidation = paramValidation;
    }

    public MetadataItem getMetadataItem() {
        return metadataItem;
    }

    public void setMetadataItem(MetadataItem metadataItem) {
        this.metadataItem = metadataItem;
    }

    public JsonHandler() {
    }

    /**
     * Parse String in Json format to {@link JsonNode}
     *
     * @param strJson String in Json format
     * @return JsonNode of {@code strJson}
     * @throws MetadataQueryException if {@code strJson} is invalid json format
     * @deprecated
     */
    protected JsonNode parseStringToJson(String strJson) throws MetadataQueryException {
        try {
            JsonFactory factory = this.mapper.getFactory();
            return factory.createParser(strJson).readValueAsTree();
        } catch (IOException e) {
            throw new MetadataQueryException("Invalid Json format: " + e.getMessage());
        }
    }

    /**
     * Update corresponding {@link Document} value for {@code key} with parsed Regex value
     *
     * @param doc {@link Document} to update
     * @return {@link Document} with updated Regex value
     */
    public Document parseRegexValueFromDocument(Document doc, String key) {
        if (((String) doc.get(key)).contains("*")) {
            Pattern regexPattern = Pattern.compile(doc.getString(key),
                    Pattern.LITERAL | Pattern.CASE_INSENSITIVE);
            doc.put(key, regexPattern);
        }
        return doc;
    }

    /**
     * Parse JsonString {@code userQuery} to {@link Document}
     * Regex will also be parsed
     *
     * @param strJson JsonString to parse
     * @return {@link Document} of the JsonString {@code userQuery}
     * @throws MetadataQueryException if invalid Json format
     */
    public Document parseUserQueryToDocument(String strJson) throws MetadataQueryException {
        Document doc = new Document();
        if (StringUtils.isNotEmpty(strJson)) {
            try {
                doc = Document.parse(strJson);
                for (String key : doc.keySet()) {
                    if (doc.get(key) instanceof String) {
                        doc = parseRegexValueFromDocument(doc, key);
                    }
                }
            } catch (Exception e) {
                throw new MetadataQueryException("Unable to parse query ", e);
            }
        }
        return doc;
    }

    /**
     * Parse an {@link ArrayNode} to a list of {@link Document}
     *
     * @param arrayNode containing JsonObjects
     * @return List of {@link Document} matching the ArrayNode
     */
    protected List<Document> parseObjectArrayNodeToList(ArrayNode arrayNode) {
        List<Document> objectList = new ArrayList<>();
        for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode jsonNode = arrayNode.get(i);
                Document doc = parseNestedJsonNodeToDocument(jsonNode);
                objectList.add(doc);
        }
        return objectList;
    }

    /**
     * Recursively parse a JsonNode to handle nested objects to a {@link Document}
     *
     * @param jsonNode {@link JsonNode} to parse
     * @return {@link Document} matching the jsonNode
     */
    protected Document parseNestedJsonNodeToDocument(JsonNode jsonNode) {
        Document doc = new Document();
        for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext(); ) {
            String key = it.next();
            JsonNode node = jsonNode.get(key);
            if (node.isObject()) {
                doc.append(key, parseNestedJsonNodeToDocument(node));
            } else if (node.isBoolean()) {
                doc.append(key, String.valueOf(node.asBoolean()));
            } else {
                doc.append(key, String.valueOf(node.asText()));
            }
        }
        return doc;
    }

    /**
     * Parse JsonNode jsonMetadata containing MetadataItem information to a Document
     *
     * @param jsonMetadata {@link JsonNode}
     * @return {@link Document} matching the JsonNode
     * @throws MetadataQueryException if unable to parse field
     */
    public Document parseJsonMetadataToDocument(JsonNode jsonMetadata) throws MetadataQueryException {
        Document doc = new Document();
        ObjectMapper mapper = new ObjectMapper();

        if (jsonMetadata.has("uuid")) {
            String uuid = jsonMetadata.get("uuid").asText();
            if (StringUtils.isNotBlank(uuid)) {
                doc.append("uuid", uuid);
            }
        }

        String name = parseNameToString(jsonMetadata);
        if (name != null)
            doc.append("name", name);

        JsonNode value = parseValueToJsonNode(jsonMetadata);
        if (value != null) {
            Document valueDoc = parseNestedJsonNodeToDocument(value);
            doc.append("value", valueDoc);
        }

        List<String> associationList = parseAssociationIdsToList(jsonMetadata);
        if (associationList.size() > 0)
            doc.append("associationIds", associationList);

        String schemaId = parseSchemaIdToString(jsonMetadata);
        if (schemaId != null)
            doc.append("schemaId", schemaId);

        List<Document> permissions = parsePermissionToList(jsonMetadata);
        if (permissions != null) {
            doc.append("permissions", permissions);
        }

        List<Document> notifications = parseNotificationToList(jsonMetadata);
        if (notifications != null)
            doc.append("notifications", notifications);

        return doc;
    }

    /**
     * Get associationIds field from {@link JsonNode}
     *
     * @param associationNode {@link JsonNode} to parse field from
     * @return String list of given associationIds
     */
    public List<String> parseAssociationIdsToList(JsonNode associationNode) {
        ArrayNode associationArrayNode = mapper.createArrayNode();
        List<String> associationList = new ArrayList<>();

        if (associationNode.has("associationIds")) {
            if (associationNode.get("associationIds").isArray()) {
                ArrayNode associationIdNode = (ArrayNode) associationNode.get("associationIds");
                for (int i = 0; i < associationIdNode.size(); i++) {
                    associationList.add(associationIdNode.get(i).textValue());
                }
            } else {
                if (associationNode.get("associationIds").isTextual())
                    associationList.add(associationNode.get("associationIds").asText());
            }

        }
        return associationList;
    }

    /**
     * Get notifications field from {@link JsonNode}
     *
     * @param notificationNode {@link JsonNode} to parse field from
     * @return List of Notification objects as {@link Document}
     * @throws MetadataQueryException if notification field is not an array
     */
    public List<Document> parseNotificationToList(JsonNode notificationNode) throws MetadataQueryException {
        if (notificationNode.hasNonNull("notifications")) {
            if (notificationNode.get("notifications").isArray()) {
                ArrayNode notificationArrayNode = (ArrayNode) notificationNode.get("notifications");
                return parseObjectArrayNodeToList(notificationArrayNode);
            } else {
                throw new MetadataQueryException(
                        "Invalid notifications value. notifications should be an "
                                + "JSON array of notification objects.");
            }
        }
        return null;
    }

    /**
     * Get permissions field from {@link JsonNode}
     *
     * @param permissionNode{@link JsonNode} to parse field from
     * @return List of permissions as {@link Document}
     * @throws MetadataQueryException if permissions field is not an array
     */
    public List<Document> parsePermissionToList(JsonNode permissionNode) throws MetadataQueryException {
        if (permissionNode.hasNonNull("permissions")) {
            if (permissionNode.get("permissions").isArray()) {
                ArrayNode permissionArrayNode = (ArrayNode) permissionNode.get("permissions");
                return parseObjectArrayNodeToList(permissionArrayNode);
            } else {
                throw new MetadataQueryException(
                        "Invalid permissions value. permissions should be an "
                                + "JSON array of permission objects.");
            }
        }
        return null;
    }

    /**
     * Parse {@link JsonNode} to {@link MetadataItem}
     *
     * @param jsonMetadata {@link JsonNode} parse from the query string
     * @throws MetadataValidationException if query values are missing or invalid
     */
    public MetadataItem parseJsonMetadata(JsonNode jsonMetadata) throws MetadataValidationException {
        try {
            MetadataItem metadataItem = new MetadataItem();
            setPermissions(this.mapper.createArrayNode());
            setNotifications(this.mapper.createArrayNode());

            if (jsonMetadata.has("uuid")) {
                String muuid = jsonMetadata.get("uuid").asText();
                if (StringUtils.isNotBlank(muuid)) {
                    metadataItem.setUuid(muuid);
                }
            }

            String name = parseNameToString(jsonMetadata);
            if (name == null)
                throw new MetadataQueryException(
                        "No name attribute specified. " +
                                "Please associate a value with the metadata name.");

            JsonNode value = parseValueToJsonNode(jsonMetadata);
            if (value == null)
                throw new MetadataQueryException(
                        "No value attribute specified. " +
                                "Please associate a value with the metadata value.");

            metadataItem.setName(name);
            metadataItem.setValue(value);
            metadataItem.getAssociations().addAll(parseAssociationIdsToList(jsonMetadata));
            metadataItem.setSchemaId(parseSchemaIdToString(jsonMetadata));


            //MetadataRequestPermissionProcessor will handle the notification parsing
            setPermissions(parsePermissionToArrayNode(jsonMetadata));
            //MetadataRequestNotificationProcessor will handle the notification parsing
            setNotifications(parseNotificationToArrayNode(jsonMetadata));

            return metadataItem;

        } catch (Exception e) {
            throw new MetadataValidationException(
                    "Unable to parse form. " + e.getMessage());
        }
    }

    /**
     * Get {@link JsonNode} name field
     *
     * @param nameNode {@link JsonNode} to parse field from
     * @return {@link String} value for the name field
     */
    public String parseNameToString(JsonNode nameNode){
        if (nameNode.has("name") && nameNode.get("name").isTextual())
            return nameNode.get("name").asText();
        return null;
    }

    /**
     * Get {@link JsonNode} value field
     *
     * @param valueNode {@link JsonNode} to parse field from
     * @return {@link JsonNode} of {@code value}
     */
    public JsonNode parseValueToJsonNode(JsonNode valueNode)  {
        if (valueNode.has("value") && !valueNode.get("value").isNull())
            return valueNode.get("value");
        return null;
    }

    /**
     * Get notifications field from {@link JsonNode}
     *
     * @param notificationNode{@link JsonNode} to parse field from
     * @return {@link ArrayNode} of notifications, null if notification field is not specified
     * @throws MetadataQueryException if notifications field is not an array
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
     * Get permissions field from {@link JsonNode}
     *
     * @param permissionNode{@link JsonNode} to parse field from
     * @return {@link ArrayNode} of permissions, null if permission field is not specified
     * @throws MetadataQueryException if permissions field is not an array
     * @deprecated
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
     * Get {@link JsonNode} schemaId field
     *
     * @param schemaNode {@link JsonNode} to get schemaId from
     * @return {@link String} of schemaId
     */
    public String parseSchemaIdToString(JsonNode schemaNode) {
        if (schemaNode.has("schemaId") && schemaNode.get("schemaId").isTextual()) {

//            if (metadataValidation == null)
//                metadataValidation = new MetadataValidation();
//
//            Document schemaDoc = MetadataSchemaDao.getInstance().findOne(new Document("uuid", schemaNode.get("schemaId").asText())
//                    .append("tenantId", TenancyHelper.getCurrentTenantId()));

//            Document schemaDoc = metadataValidation.checkSchemaIdExists(schemaNode.get("schemaId").asText());

            //where to check for permission?
//            if (schemaDoc != null)
            return schemaNode.get("schemaId").asText();
        }
        return null;
    }

    /**
     * Validate given JsonNode against the schemaId
     *
     * @deprecated
     */
    public String validateValueAgainstSchema(String value, String schema) throws
            MetadataSchemaValidationException {
        try {
            JsonFactory factory = this.mapper.getFactory();
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
                throw new MetadataSchemaValidationException("Metadata does not conform to schema.");
            }
            return value;
        } catch (Exception e) {
            throw new MetadataSchemaValidationException("Metadata does not conform to schema.");
        }
    }
}

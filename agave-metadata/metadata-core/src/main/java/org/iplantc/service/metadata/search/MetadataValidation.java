package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBList;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataSchemaDao;
import org.iplantc.service.metadata.exceptions.*;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.json.JSONException;
import org.json.JSONObject;

import javax.validation.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.*;

public class MetadataValidation {

    /**
     * Verify the associationIds using the agave-uuid api
     *
     * @param items {@link ArrayNode} of String associated uuids
     * @return {@link MetadataAssociationList} of valid associated uuids from {@code items}
     * @throws MetadataQueryException if no resource was found with any uuid in {@code items} was not found
     * @throws UUIDException          if unable to run query
     * @throws MetadataException      if unable to validate uuid using the agave-uuid api
     */
    public MetadataAssociationList checkAssociationIdsUuidApi(ArrayNode items) throws MetadataQueryException, UUIDException, MetadataException {
        MetadataAssociationList associationList = new MetadataAssociationList();

        if (items != null) {
            for (int i = 0; i < items.size(); i++) {
                String associationId = items.get(i).asText();
                if (StringUtils.isNotEmpty(associationId)) {
                    AgaveUUID associationUuid = new AgaveUUID(associationId);
                    String apiOutput = getValidationResponse(associationId);
                    if (!apiOutput.isEmpty()) {
                        AssociatedReference associatedReference = parseValidationResponse(apiOutput);
                        associationList.add(associatedReference);
                    } else {
                        UUIDType type = associationUuid.getResourceType();
                        if (UUIDType.METADATA == type)
                            throw new MetadataQueryException("No metadata resource found with uuid " + associationId);
                        else if (UUIDType.SCHEMA == type)
                            throw new MetadataQueryException("No metadata schema found with uuid " + associationId);
                        else
                            throw new MetadataQueryException("No associated object found with uuid " + associationId);
                    }
                }
            }
        }
        return associationList;
    }

    /**
     * Validate that fields in node conform to MetadataItem field constraints
     *
     * @param node {@link JsonNode} to validate
     * @return validated {@link MetadataItem}
     * @throws MetadataValidationException if fields do not confirm to MetadataItem constraints
     */
    public MetadataItem validateMetadataNodeFields(JsonNode node, String username) throws MetadataValidationException {
        MetadataItem item;
        JsonHandler handler = new JsonHandler();


        try {
            item = handler.parseJsonMetadata(node);
            item.setOwner(username);
            item.setInternalUsername(username);


            //validator
            Validator validator;

            ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
            validator = validatorFactory.getValidator();

            Set<ConstraintViolation<MetadataItem>> constraintViolations = validator.validate(item);

            if (constraintViolations.size() != 0)
                throw new MetadataValidationException(constraintViolations.iterator().next().getMessage());
            return item;
        } catch (Exception e) {
            throw new MetadataValidationException(e.getMessage());
        }
    }

//    /**
//     * Verify and return list of verified associated Uuids from {@link ArrayNode} items
//     *
//     * @param items {@link ArrayNode} of String uuids
//     * @return {@link MetadataAssociationList} of valid associated Uuids from {@code items}
//     * @throws MetadataQueryException       if query is missing or invalid
//     * @throws UUIDException                if unable to run query
//     * @throws PermissionException          if user does not have read permissions
//     * @throws MetadataAssociationException if the uuid is invalid
//     */
//    public MetadataAssociationList checkAssociationIds(ArrayNode items, String username) throws MetadataQueryException, UUIDException, PermissionException, MetadataAssociationException, MetadataException {
//        MetadataAssociationList associationList = new MetadataAssociationList();
//
//        if (associationList == null) {
//            associationList = new MetadataAssociationList();
//        }
//
//        BasicDBList associations = new BasicDBList();
//        String tenantId = TenancyHelper.getCurrentTenantId();
//        if (items != null) {
//            for (int i = 0; i < items.size(); i++) {
//                String associationId = items.get(i).asText();
//                Bson associationQuery = and(eq("uuid", associationId),
//                        eq("tenantId", TenancyHelper.getCurrentTenantId()));
//
//                if (StringUtils.isNotEmpty(associationId)) {
//                    AgaveUUID associationUuid = new AgaveUUID(associationId);
//
//                    if (UUIDType.METADATA == associationUuid.getResourceType()) {
//                        //retrieve item with given id
//                        MetadataSearch search = new MetadataSearch(username);
//                        MetadataItem associationItem = search.findById(associationId, tenantId);
//                        if (associationItem == null) {
//                            throw new MetadataQueryException(
//                                    "No metadata resource found with uuid " + associationId);
//                        }
//
//                    } else if (UUIDType.SCHEMA == associationUuid.getResourceType()) {
//
//                        try {
//                            MetadataSchemaDao schemaDao = new MetadataSchemaDao().getInstance();
//                            Document associationDocument = schemaDao.findOne(associationQuery);
//
//                            if (associationDocument == null) {
//                                throw new MetadataQueryException(
//                                        "No metadata schema resource found with uuid " + associationId);
//                            }
//                        } catch (MetadataStoreException e) {
//                            throw new MetadataQueryException(
//                                    "No metadata schema resource found with uuid " + associationId);
//                        }
//                    } else {
//                        try {
//                            associationUuid.getObjectReference();
//                        } catch (Exception e) {
//                            throw new MetadataQueryException(
//                                    "No associated object found with uuid " + associationId);
//                        }
//                    }
//                    associations.add(items.get(i).asText());
//                    associationList.add(items.get(i).asText());
//                }
//            }
//        }
//        return associationList;
//    }


//    /**
//     * Check if given {@code schemaId} exists
//     * @param schemaId uuid of the schema to validate
//     * @return {@link Document} of the schema if exists, null otherwise
//     * @throws MetadataStoreException if unable to connect to the mongo collection
//     */
//    public Document checkSchemaIdExists(String schemaId) throws MetadataStoreException {
//        if (schemaId != null) {
//            Bson schemaQuery = and(eq("uuid", schemaId), eq("tenantId", TenancyHelper.getCurrentTenantId()));
//            MetadataSchemaDao schemaDao = new MetadataSchemaDao().getInstance();
//
//            Document schemaDocument = schemaDao.findOne(schemaQuery);
//
//            // lookup the schema
//            if (schemaDocument != null)
//                return schemaDocument;
//        }
//        return null;
//    }

//    /**
//     * Check if {@code username} has read permissions for the given schema
//     * @param schemaDocument {@link Document} of the given schema
//     * @param username to check permissions for
//     * @return true if {@code username} has read permissions
//     * @throws MetadataQueryException
//     */
//    public boolean checkSchemaIdPermission(Document schemaDocument, String username) throws PermissionException {
//        // check user permsisions to view the schema
//        try {
//            String schemaId = (String) schemaDocument.get("uuid");
//            MetadataSchemaPermissionManager schemaPM = new MetadataSchemaPermissionManager(schemaId,
//                    (String) schemaDocument.get("owner"));
//            if (schemaPM.canRead(username)) {
//                return true;
//            }
//                else{
//                throw new MetadataException("User does not have permission to read metadata schema");
//            }
//        } catch (MetadataException e) {
//            throw new PermissionException(e.getMessage());
//        }
//    }

//    /**
//     * Verify that the {@code value} conforms to the schema in {@code schemaDoc}
//     *a
//     * @param schemaDoc {@link Document} of the schema
//     * @param value Json string to verify against the schema
//     * @return validated schema id, empty string otherwise
//     * @throws MetadataSchemaValidationException if the value does not conform to the {@code schemaId}
//     */
//    public String validateValueAgainstSchema(Document schemaDoc, String value) throws MetadataSchemaValidationException {
//        // if a schema is given, validate the metadata against that registered schema
//        if (schemaDoc != null) {
//            // now validate the json against the schema
//            String schema;
//            try {
//                schema = schemaDoc.getString("schema");
//                JsonFactory factory = new ObjectMapper().getFactory();
//                JsonNode jsonSchemaNode = factory.createParser(schema).readValueAsTree();
//                JsonNode jsonMetadataNode = factory.createParser(value).readValueAsTree();
//                AgaveJsonValidator validator = AgaveJsonSchemaFactory.byDefault().getValidator();
//
//                ProcessingReport report = validator.validate(jsonSchemaNode, jsonMetadataNode);
//                if (!report.isSuccess()) {
//                    StringBuilder sb = new StringBuilder();
//                    for (Iterator<ProcessingMessage> reportMessageIterator = report.iterator(); reportMessageIterator.hasNext(); ) {
//                        sb.append(reportMessageIterator.next().toString() + "\n");
//                    }
//                    throw new MetadataSchemaValidationException(
//                            "Metadata value does not conform to schema. \n" + sb.toString());
//                }
//
//                return (String)schemaDoc.get("uuid");
//
//            } catch (MetadataSchemaValidationException e) {
//                throw e;
//            } catch (Exception e) {
//                throw new MetadataSchemaValidationException(
//                        "Metadata does not conform to schema.");
//            }
//        }
//        return "";
//    }

    /**
     * Verify if all the uuids of the MetadataItems in the given list are valid
     *
     * @param metadataItemList list of {@link MetadataItem} to validate the uuid for
     * @return true if all uuids in {@code metadataItemList} are valid
     * @throws MetadataException if unable to validate all the uuid in {@code metadataItemList}
     */
    public boolean validateUuids(List<MetadataItem> metadataItemList) throws MetadataException {
        for (MetadataItem metadataItem : metadataItemList) {
            if (!validateUuid(metadataItem.getUuid()))
                return false;
        }
        return true;
    }

    /**
     * Verify if given uuid is a valid uuid
     *
     * @param uuid to check
     * @return true if uuid is valid
     * @throws MetadataException if unable to validate {@code uuid}
     */
    public boolean validateUuid(String uuid) throws MetadataException {
        String apiOutput = getValidationResponse(uuid);
        return StringUtils.isNotEmpty(apiOutput);
    }

    /**
     * Build the URL to query the agave-uuid api with the {@code uuid}
     *
     * @param uuid to check
     * @return String URL for the GET query to agave-uuid api
     * @throws TenantException    if unable to find the Tenant by TenantId
     * @throws URISyntaxException if unable to build a valid URL from the uuid
     */
    public String buildUuidValidationURL(String uuid) throws TenantException, URISyntaxException {
        Tenant tenant = null;
        String strUrl = "";
        URI uri = null;
        tenant = new TenantDao().findByTenantId(TenancyHelper.getCurrentTenantId());
        URI tenantBaseUrl = URI.create(tenant.getBaseUrl());
        URIBuilder builder = new URIBuilder();
        builder.setScheme(tenantBaseUrl.getHost());
        builder.setPath("uuid/v2");
        builder.setParameter("uuid", uuid);
        uri = builder.build();
        strUrl = uri.toString();
        return strUrl;
    }

    /**
     * Retrieve the GET response from agave-uuid api to validate the given uuid
     *
     * @param uuid to check
     * @return String of the response entity
     * @throws MetadataException if the uuid cannot be verified using the agave-uuid api
     */
    public String getValidationResponse(String uuid) throws MetadataException {
        //        String baseUrl = "http://localhost/uuid/v2/" + uuid;
        try {
            String strUrl = buildUuidValidationURL(uuid);
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(strUrl);
            HttpResponse httpResponse = httpClient.execute(getRequest);

            if (httpResponse.getStatusLine().getStatusCode() == 200) {
                HttpEntity httpEntity = httpResponse.getEntity();
                return EntityUtils.toString(httpEntity);
            }
        } catch (URISyntaxException e) {
            throw new MetadataException("Invalid URI syntax ", e);
        } catch (TenantException e) {
            throw new MetadataException("Unable to retrieve valid tenant information ", e);
        } catch (Exception e) {
            throw new MetadataException("Unable to validate uuid ", e);
        }
        return null;
    }

    /**
     * Parse response from uuid validation to {@link AssociatedReference}
     *
     * @param validationResponse JSON String response
     * @return {@link AssociatedReference} with valid uuid and links
     * @throws MetadataException if unable to create valid {@link AssociatedReference}
     */
    public AssociatedReference parseValidationResponse(String validationResponse) throws MetadataException {
        try {
            JSONObject jsonObject = new JSONObject(validationResponse);

            AgaveUUID uuid = new AgaveUUID(jsonObject.getString("uuid"));
            String title = jsonObject.getString("type");
            String links = jsonObject.getJSONObject("_links")
                    .getJSONObject("self")
                    .getString("href");

            return new AssociatedReference(uuid, links);
        } catch (JSONException e) {
            throw new MetadataException("Invalid Json response ", e);
        } catch (UUIDException e) {
            throw new MetadataException("Invalid uuid value ", e);
        }
    }

//    /**
//     * Add the {@code associatedReference} to the associationList of the {@link MetadataItem}
//     *
//     * @param associatedReference to add to the associated id list
//     * @return the {@code associatedReference} that was added successfully
//     */
//    public AssociatedReference setAssociatedReference(AssociatedReference associatedReference) {
//        this.associationList = this.metadataItem.getAssociations();
//        this.associationList.add(associatedReference);
//        this.metadataItem.setAssociations(this.associationList);
//        return associatedReference;
//    }


}

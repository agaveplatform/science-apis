package org.iplantc.service.metadata.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.exceptions.UUIDException;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataValidationException;
import org.iplantc.service.metadata.model.AssociatedReference;
import org.iplantc.service.metadata.model.MetadataAssociationList;
import org.iplantc.service.metadata.model.MetadataItem;
import org.json.JSONException;
import org.json.JSONObject;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

            if (constraintViolations.size() != 0) {
                String violationMessages = constraintViolations.stream().map(cv -> {
                    return cv.getMessage();
                }).collect(Collectors.joining(","));

                throw new MetadataValidationException(violationMessages);
            }
            return item;
        }
        catch (MetadataValidationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new MetadataValidationException(e);
        }
    }

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
